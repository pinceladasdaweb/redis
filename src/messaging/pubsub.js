import withDeadline from '../utils/deadline.js'

// Nothing on the shutdown path may block forever — see utils/deadline.js.
const SHUTDOWN_DEADLINE_MS = 2000

// How often the keyspace-event fan-out reconciles its subscribers with the
// cluster's current masters. The '+node' event is the fast path; this is the
// guarantee (promotions never fire '+node'). Each tick is a local map
// comparison — no network unless the topology actually drifted.
const TOPOLOGY_RESYNC_MS = 10000

// Pub/Sub manager. A connection in subscriber mode cannot execute regular
// commands, so subscriptions live on a dedicated connection created lazily
// from the main one. Re-subscribing after a reconnection is handled by the
// driver (autoResubscribe) — the same client object survives the whole cycle,
// which is exactly what makes that guarantee hold.
class SubscriptionManager {
  #subscriber = null
  #channelHandlers = new Map()
  #patternHandlers = new Map()
  // Cluster only: one subscriber per master, for channels that are published
  // node-locally (keyspace events). Keyed by the node's address.
  #nodeSubscribers = new Map()
  #nodeChannels = new Set()
  #nodeWatcher = null
  #nodeResync = null

  constructor ({ connection, logger, clock, emit }) {
    this.connection = connection
    this.logger = logger
    this.clock = clock
    this.emit = emit
  }

  #dispatch (handler, message, channel, pattern) {
    if (!handler) {
      return
    }

    // Handlers may be async: a rejection must never become an unhandled
    // rejection inside an EventEmitter callback.
    Promise.resolve()
      .then(() => handler(message, channel, pattern))
      .catch((err) => {
        this.logger.error(`Pub/sub handler for '${pattern ?? channel}' failed: ${err.message}`)
      })
  }

  // Every subscriber connection — the shared one and the per-node ones — routes
  // its traffic through the same handler maps and facade events.
  #wireSubscriber (subscriber, onEnd) {
    subscriber.on('error', (err) => {
      this.logger.error(`Redis subscriber error: ${err.message || err}`)
      this.emit('connectionError', err)
    })

    subscriber.on('message', (channel, message) => {
      this.emit('message', channel, message)
      this.#dispatch(this.#channelHandlers.get(channel), message, channel)
    })

    subscriber.on('pmessage', (pattern, channel, message) => {
      this.emit('pmessage', pattern, channel, message)
      this.#dispatch(this.#patternHandlers.get(pattern), message, channel, pattern)
    })

    subscriber.on('end', () => {
      onEnd()
      subscriber.removeAllListeners()
    })

    return subscriber
  }

  #ensureSubscriber () {
    if (this.#subscriber) {
      return this.#subscriber
    }

    const subscriber = this.connection.assertReady('subscribe').duplicate()
    this.#subscriber = subscriber

    return this.#wireSubscriber(subscriber, () => {
      // The subscriber's own retryStrategy gave up: release the dead client
      // so a later subscribe() starts a fresh connection, and never lose the
      // subscriptions silently. (A normal close() detaches this handler
      // before quitting, so it only fires for real give-ups.)
      if (this.#subscriber === subscriber) {
        this.#subscriber = null

        if (this.#channelHandlers.size > 0 || this.#patternHandlers.size > 0) {
          this.logger.warn('Redis subscriber connection ended permanently: active subscriptions were lost. Subscribe again to restore them.')
        }
      }
    })
  }

  // One handler per channel/pattern — a re-subscribe replaces it (last one
  // wins); the facade 'message'/'pmessage' events allow fan-out when needed.
  // On a failed subscribe the previous handler is restored.
  async subscribe (channel, handler) {
    const subscriber = this.#ensureSubscriber()
    const previous = this.#channelHandlers.get(channel)

    if (handler) {
      this.#channelHandlers.set(channel, handler)
    }

    try {
      return await subscriber.subscribe(channel)
    } catch (err) {
      this.#restore(this.#channelHandlers, channel, previous, handler)

      throw err
    }
  }

  // Publishes reach every node through the cluster bus, but keyspace events do
  // not: each node emits them for its own slots only, and a cluster subscriber
  // attaches to a single sampled node. Subscribing the normal way would deliver
  // one shard's events and look exactly like a subscription that works, so
  // these channels get one subscriber per master instead.
  //
  // The call is atomic to the caller: if any master refuses, every mutation is
  // rolled back — the previous handler restored, the channel deregistered, the
  // masters that DID subscribe unsubscribed — and the error rethrown. Without
  // that, a partial failure left two of three shards delivering events to a
  // handler the caller believes was never installed, and the third permanently
  // silent. A failed call can simply be retried whole.
  async subscribeEverywhere (channel, handler) {
    const client = this.connection.assertReady('subscribe')

    if (typeof client.nodes !== 'function') {
      return this.subscribe(channel, handler)
    }

    const previous = this.#channelHandlers.get(channel)
    const wasRegistered = this.#nodeChannels.has(channel)

    if (handler) {
      this.#channelHandlers.set(channel, handler)
    }

    this.#nodeChannels.add(channel)
    this.#watchTopology(client)

    const masters = client.nodes('master')
    const outcomes = await Promise.allSettled(
      masters.map((node) => this.#subscribeNode(node, [channel]))
    )

    const failed = outcomes.find((outcome) => outcome.status === 'rejected')

    if (failed) {
      this.#restore(this.#channelHandlers, channel, previous, handler)

      if (!wasRegistered) {
        this.#nodeChannels.delete(channel)
      }

      // The masters that answered are subscribed to a channel whose handler
      // is being taken back — undo them, best effort (a node that refuses
      // the undo will simply dispatch to no handler, which is inert).
      for (const [index, outcome] of outcomes.entries()) {
        if (outcome.status !== 'fulfilled') continue

        const subscriber = this.#nodeSubscribers.get(this.#nodeKey(masters[index]))
        subscriber?.unsubscribe(channel).catch(() => {})
      }

      throw failed.reason
    }

    return outcomes.at(-1)?.value ?? 0
  }

  #nodeKey (node) {
    const { host, port } = node.options

    return `${host}:${port}`
  }

  async #subscribeNode (node, channels) {
    const key = this.#nodeKey(node)
    let subscriber = this.#nodeSubscribers.get(key)

    if (!subscriber) {
      subscriber = node.duplicate()
      this.#nodeSubscribers.set(key, subscriber)

      this.#wireSubscriber(subscriber, () => {
        if (this.#nodeSubscribers.get(key) === subscriber) {
          this.#nodeSubscribers.delete(key)
          this.logger.warn(`Keyspace-event subscriber for cluster node ${key} ended permanently: that shard's events were lost.`)
        }
      })
    }

    let count = 0

    for (const channel of channels) {
      count = await subscriber.subscribe(channel)
    }

    return count
  }

  // Resharding adds masters after the fan-out: without this, a new shard's
  // events would be missing for the rest of the process's life.
  //
  // Two ways the '+node' event alone proved insufficient, both measured
  // against the driver: the listener dies with its client (a reconnect cycle
  // builds a brand-new Cluster instance, so the watcher must follow the LIVE
  // client, not the first one it saw), and a replica PROMOTED to master never
  // fires '+node' at all — ioredis only emits it for a host:port that is new
  // to the pool; promotions go through onRoleChange, silently. So the event
  // is the fast path, and a slow periodic resync is the guarantee: it
  // subscribes masters that appeared without an event and releases
  // subscribers whose node is no longer a master.
  #watchTopology (client) {
    if (this.#nodeWatcher?.client === client) {
      return
    }

    // A previous cycle's watcher points at a dead client: detach and re-arm.
    if (this.#nodeWatcher) {
      this.#nodeWatcher.client.removeListener('+node', this.#nodeWatcher.handler)
    }

    const handler = () => this.#resyncTopology(client)

    client.on('+node', handler)
    this.#nodeWatcher = { client, handler }

    if (!this.#nodeResync) {
      // Fire-and-forget by design, so unref is correct here (nothing awaits
      // it): the resync must never be what keeps the process alive.
      //
      // The tick asks the CONNECTION for the live client — never the watcher,
      // whose captured client is exactly what a reconnect cycle makes stale.
      this.#nodeResync = this.clock.setInterval(() => {
        let current

        try {
          current = this.connection.assertReady('subscribe')
        } catch {
          return // between cycles; the next tick will see the new client
        }

        if (typeof current.nodes !== 'function') {
          return
        }

        this.#watchTopology(current)
        this.#resyncTopology(current)
      }, TOPOLOGY_RESYNC_MS)

      this.#nodeResync.unref?.()
    }
  }

  #resyncTopology (client) {
    if (this.#nodeChannels.size === 0) {
      return
    }

    const masters = new Map(client.nodes('master').map((node) => [this.#nodeKey(node), node]))

    // Masters without a subscriber: new shards, or replicas promoted without
    // a '+node'. Either way their events are being lost right now.
    for (const [key, node] of masters) {
      if (this.#nodeSubscribers.has(key)) continue

      this.#subscribeNode(node, [...this.#nodeChannels]).catch((err) => {
        this.logger.error(`Could not extend keyspace-event subscriptions to cluster node ${key}: ${err.message}`)
      })
    }

    // Subscribers whose node left the master set: departed or demoted nodes
    // whose connection would otherwise retry forever, invisibly.
    for (const [key, subscriber] of this.#nodeSubscribers) {
      if (masters.has(key)) continue

      this.#nodeSubscribers.delete(key)
      this.#release(subscriber).catch(() => {})
      this.logger.info(`Keyspace-event subscriber for ${key} released: the node is no longer a master.`)
    }
  }

  async unsubscribe (channel) {
    this.#channelHandlers.delete(channel)
    this.#nodeChannels.delete(channel)

    const counts = await Promise.all(
      [...this.#nodeSubscribers.values()].map((subscriber) => subscriber.unsubscribe(channel))
    )

    if (this.#subscriber) {
      return this.#subscriber.unsubscribe(channel)
    }

    return counts.at(-1) ?? 0
  }

  async psubscribe (pattern, handler) {
    const subscriber = this.#ensureSubscriber()
    const previous = this.#patternHandlers.get(pattern)

    if (handler) {
      this.#patternHandlers.set(pattern, handler)
    }

    try {
      return await subscriber.psubscribe(pattern)
    } catch (err) {
      this.#restore(this.#patternHandlers, pattern, previous, handler)

      throw err
    }
  }

  #restore (handlers, key, previous, attempted) {
    if (!attempted) {
      return
    }

    if (previous) {
      handlers.set(key, previous)
    } else {
      handlers.delete(key)
    }
  }

  async punsubscribe (pattern) {
    this.#patternHandlers.delete(pattern)

    if (!this.#subscriber) {
      return 0
    }

    return this.#subscriber.punsubscribe(pattern)
  }

  // Called from the facade's disconnect(): every subscriber connection has its
  // own lifecycle and must be released explicitly.
  async close () {
    const closing = [this.#subscriber, ...this.#nodeSubscribers.values()].filter(Boolean)

    this.#subscriber = null
    this.#nodeSubscribers.clear()
    this.#nodeChannels.clear()
    this.#channelHandlers.clear()
    this.#patternHandlers.clear()

    if (this.#nodeWatcher) {
      this.#nodeWatcher.client.removeListener('+node', this.#nodeWatcher.handler)
      this.#nodeWatcher = null
    }

    if (this.#nodeResync) {
      this.clock.clearInterval(this.#nodeResync)
      this.#nodeResync = null
    }

    await Promise.all(closing.map((subscriber) => this.#release(subscriber)))
  }

  async #release (subscriber) {
    subscriber.removeAllListeners()

    try {
      if (subscriber.status !== 'end') {
        // Same trap as the main connection, and this one runs first on the
        // facade's disconnect(): a quit() parked behind the offline queue may
        // never answer, which would hang the entire shutdown right here.
        await withDeadline(subscriber.quit(), {
          clock: this.clock,
          ms: SHUTDOWN_DEADLINE_MS,
          operation: 'quit'
        })
      }
    } catch {
      subscriber.disconnect()
    }
  }
}

export { SubscriptionManager }
export default SubscriptionManager

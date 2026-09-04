import withDeadline from '../utils/deadline.js'
import RedisClientError from '../utils/errors.js'

// How long a shutdown may wait on the driver, per step. Nothing here may block
// forever: a graceful shutdown that never finishes is worse than an abrupt one.
const SHUTDOWN_DEADLINE_MS = 2000

// Owns the client lifecycle. Reconnection itself belongs entirely to the
// ioredis driver (retryStrategy / reconnectOnError in RedisConfig): a single
// client instance survives the whole connect()..disconnect() cycle and the
// listeners here only track state and surface events on the facade.
//
// Two facts about the driver shape everything below, both probed against
// ioredis 6 rather than read off its docs:
//
//   1. `setStatus` assigns `client.status` SYNCHRONOUSLY and emits the event
//      on process.nextTick, and a give-up runs close→end (a flap close→
//      reconnecting) in one synchronous stack. So by the time ANY listener
//      runs, the status already says which one it was — and the driver's
//      'end' emit may still be queued behind the 'close' our listener is
//      handling. Stripping listeners in that window swallows it.
//
//   2. While the client is 'reconnecting' the socket is already destroyed.
//      quit() is then answered locally (disconnect() + 'OK'), and that
//      disconnect() can produce no 'close' — so 'end' NEVER comes, and
//      nothing the driver offers publicly will flush what is parked. The
//      cycle has to be ended from here.
class ConnectionManager {
  #connectPromise = null
  #disconnectPromise = null
  #client = null
  #isConnected = false
  // The caller's LAST word: 'connect' or 'disconnect'. A connect() queued
  // behind a teardown proceeds only if nobody asked for a disconnect() after
  // it — otherwise it would resume once the teardown finished and build a
  // live client the caller believes it just tore down.
  #intent = 'disconnect'
  // Raised for the whole of a shutdown, from the facade's first step to the
  // driver's last: quit() does not flip the driver's status synchronously,
  // so without this every gate would keep admitting work into the quit
  // window — commands rejected later with a bare driver Error, connections
  // created after their reapers ran.
  #closing = false
  // Clients whose cycle this manager has already ended, so the facade 'end'
  // fires exactly once per cycle whether the driver's own 'end' arrives
  // first, late, or never.
  #released = new WeakSet()

  constructor ({ redisConfig, logger, clock, emit }) {
    this.redisConfig = redisConfig
    this.logger = logger
    this.clock = clock
    this.emit = emit
  }

  get client () {
    return this.#client
  }

  get isConnected () {
    return this.#isConnected
  }

  get closing () {
    return this.#closing
  }

  async connect () {
    this.#intent = 'connect'
    this.#closing = false

    // An attempt in flight must be joined, never skipped: #establishConnection
    // assigns the client synchronously, so a concurrent caller that checked
    // for the client first would resolve before the connection was ready.
    if (this.#connectPromise) {
      return this.#connectPromise
    }

    // The slot is claimed SYNCHRONOUSLY, before #connectCycle can reach its
    // first await. The cycle waits — for a teardown, for the cluster 'close'
    // ambiguity — and any caller that arrived during one of those waits used
    // to sail past the check above and out through the "reusing" branch
    // below, resolving while the connection was still being built. The guard
    // is only a mutex if it is set before the very first suspension point.
    //
    // Only the attempt that is still current may clear the slot: disconnect()
    // drops it on purpose, and a settling older attempt must not wipe the
    // brand-new one that replaced it.
    const attempt = this.#connectCycle().finally(() => {
      if (this.#connectPromise === attempt) {
        this.#connectPromise = null
      }
    })

    this.#connectPromise = attempt

    return attempt
  }

  async #connectCycle () {
    // A teardown in flight must be waited out, never raced: for the whole
    // quit window #client still points at the dying client, and "reusing"
    // it would resolve this call successfully moments before 'end' nulls
    // the client out from under the caller — connected, with nothing behind
    // it. Waiting lets the fresh cycle below start from a clean slate.
    if (this.#disconnectPromise) {
      await this.#disconnectPromise
    }

    // Cluster only. A standalone client never shows 'close' to any listener
    // (fact 1 above), but a Cluster's pool-'drain' path sets 'close' and
    // only decides between 'reconnecting' and 'end' in a once('close')
    // handler the driver registered AFTER ours — so a supervisor reconnecting
    // from the facade's 'close' event really does act one turn too early
    // there. One deferral later the status has already moved on.
    if (this.#client?.status === 'close') {
      await new Promise((resolve) => setImmediate(resolve))

      // A disconnect() that started during that deferral owns the client now,
      // and the promise above is the same one this method already honours.
      if (this.#disconnectPromise) {
        await this.#disconnectPromise
      }
    }

    // Abandoned: a disconnect() was asked for after this connect(). Resuming
    // would hand the caller a live client they no longer want.
    if (this.#intent !== 'connect') {
      return
    }

    if (this.#client) {
      // A give-up leaves #client set with status 'end' for the tick between
      // the status flip and the 'end' handler running — and (fact 1) that is
      // exactly the tick a supervisor reconnecting from 'close' lands in. The
      // corpse can never carry a command again; end its cycle here, which
      // also emits the facade 'end' the driver's queued emit would otherwise
      // have delivered to nobody once the listeners were gone.
      if (this.#client.status === 'end') {
        this.#releaseClient(this.#client)
      } else {
        this.logger.debug?.('Redis client already exists. Reusing existing connection.')
        return
      }
    }

    return this.#establishConnection()
  }

  async #establishConnection () {
    const client = this.redisConfig.createRedisClient()
    this.#client = client

    client.on('ready', () => {
      this.#isConnected = true
      this.logger.info('Redis connection is ready')
      this.emit('ready')
    })

    client.on('error', (err) => {
      this.logger.error(`Redis client error: ${err.message || err}`)
      // Never re-emitted as 'error': an EventEmitter 'error' without a
      // listener crashes the process. Consumers subscribe on their terms.
      this.emit('connectionError', err)
    })

    client.on('close', () => {
      this.#isConnected = false
      this.logger.warn('Redis connection closed')
      this.emit('close')
    })

    client.on('reconnecting', (delay) => {
      this.logger.info(`Redis client is reconnecting${typeof delay === 'number' ? ` in ${delay}ms` : ''}...`)
      this.emit('reconnecting', delay)
    })

    // Final state: emitted after quit() or when retryStrategy gives up.
    client.on('end', () => this.#releaseClient(client))

    try {
      await this.#settled(client)
    } catch (err) {
      if (client.status === 'end') {
        this.logger.error(`Failed to connect to Redis: ${err.message}`)

        // The driver is done with this client. Release it here too: if it
        // never emits 'end', a stale reference would make every later
        // connect() short-circuit and leave the caller unable to reconnect.
        this.#releaseClient(client)

        return
      }

      // The driver keeps retrying in the background per retryStrategy;
      // commands stay gated by assertReady until it succeeds.
      this.logger.error(`Failed to connect to Redis: ${err.message}. Reconnection attempts continue in the background.`)
    }
  }

  // connect() settles on the client's OWN lifecycle, not on the driver's
  // promise alone. Probed against ioredis 6: Cluster.connect() removes its
  // close listener at 'refresh' and, when the first ready-check reports
  // cluster_state:fail, calls disconnect(true) without ever resolving or
  // rejecting — the internal reconnect makes a NEW promise and the caller's
  // is orphaned. The cluster then becomes 'ready' on its own while
  // `await client.connect()` hangs forever, pinning the connect slot for the
  // life of the process. 'ready' and 'end' are the two ways a cycle actually
  // resolves; the promise is honoured when it does settle, and ignored when
  // it cannot.
  #settled (client) {
    return new Promise((resolve, reject) => {
      const detach = () => {
        client.removeListener('ready', onReady)
        client.removeListener('end', onEnd)
      }
      const onReady = () => {
        detach()
        resolve()
      }
      const onEnd = () => {
        detach()
        reject(new Error('Connection ended before it was ready'))
      }

      client.once('ready', onReady)
      client.once('end', onEnd)

      client.connect().then(
        () => { detach(); resolve() },
        (err) => { detach(); reject(err) }
      )
    })
  }

  // The ONE way a client's cycle ends, whoever notices first: the driver's
  // 'end' handler, the connect-failure path, a supervisor finding a corpse,
  // or a teardown whose driver will never say 'end'. Idempotent per client,
  // so the facade 'end' fires exactly once per cycle.
  #releaseClient (client) {
    if (this.#released.has(client)) {
      return
    }

    this.#released.add(client)
    client.removeAllListeners()

    if (this.#client === client) {
      this.#client = null
      this.#isConnected = false
    }

    this.emit('end')
  }

  // Marks the start of a shutdown BEFORE the driver is touched. The facade
  // has its own steps to run first (subscribers, dedicated connections), and
  // work arriving during those must already be refused — quit() has not
  // happened yet, so the driver's status alone would still admit it.
  beginShutdown () {
    this.#intent = 'disconnect'
    this.#closing = true
  }

  // Final and idempotent: quit() makes the driver emit 'end', which releases
  // the client (see #releaseClient) without ever scheduling a reconnection.
  // A later connect() starts a brand-new cycle — and one that arrives DURING
  // this teardown waits for it (see connect()), which is what makes that
  // promise true rather than aspirational.
  async disconnect () {
    // Recorded on the join path too: a connect() queued behind this teardown
    // must not survive a disconnect() that was asked for after it.
    this.beginShutdown()

    if (this.#disconnectPromise) {
      return this.#disconnectPromise
    }

    const teardown = this.#teardown().finally(() => {
      this.#disconnectPromise = null
    })

    this.#disconnectPromise = teardown

    return teardown
  }

  async #teardown () {
    const client = this.#client

    // An attempt still in flight is abandoned here: its client is the one
    // being closed, so a later connect() must start a fresh cycle instead of
    // joining a promise that will resolve with nothing behind it.
    this.#connectPromise = null

    if (!client) {
      return
    }

    if (client.status === 'end') {
      this.#releaseClient(client)

      return
    }

    // Fact 2: between retries there is nothing to say goodbye to. quit()
    // would resolve 'OK' without touching the wire and 'end' would never
    // follow — every shutdown during an outage used to sit out the whole
    // escape timer and then release in silence. Cancel the driver's retry
    // (disconnect() does that much), reject whatever it still holds — the
    // flush is a driver internal, so it is best effort — and end the cycle
    // from here.
    if (client.status === 'reconnecting') {
      client.disconnect()
      client.flushQueue?.(new RedisClientError(
        'disconnect() closed the connection while it was reconnecting.',
        'disconnect',
        'REDIS_UNAVAILABLE'
      ))
      this.logger.info('Redis client disconnected while reconnecting')
      this.#releaseClient(client)

      return
    }

    // 'end' fires asynchronously after quit() resolves — wait for it so the
    // handler releases the client and emits the facade event, with a timed
    // escape route in case the driver never gets there.
    // The escape timer is awaited, so it must be able to fire (an unref'd
    // timer never does once the loop is otherwise idle) — and it is cleared
    // the moment 'end' arrives, so a clean shutdown never waits on it.
    const ended = new Promise((resolve) => {
      const timer = this.clock.setTimeout(resolve, SHUTDOWN_DEADLINE_MS)

      client.once('end', () => {
        this.clock.clearTimeout(timer)
        resolve()
      })
    })

    try {
      // quit() only answers immediately while the offline queue is empty:
      // with anything queued the driver parks the QUIT behind it and only
      // replies once it reconnects — which, with the default infinite
      // retries, may be never. Without a deadline here the escape route
      // above is unreachable and shutdown hangs forever.
      await withDeadline(client.quit(), {
        clock: this.clock,
        ms: SHUTDOWN_DEADLINE_MS,
        operation: 'quit'
      })

      this.logger.info('Redis client disconnected successfully')
    } catch (err) {
      this.logger.warn(`Error during Redis quit: ${err.message}. Forcing the connection closed.`)
      client.disconnect()
    }

    await ended

    // Idempotent with the 'end' handler, and the only release path — facade
    // 'end' included — when the driver never fired it.
    this.#releaseClient(client)
  }

  // Fail-fast gate: a cheap local probe of the driver's own status, never a
  // network round-trip. Commands issued while disconnected throw a structured
  // REDIS_UNAVAILABLE error instead of silently resolving to null — writes
  // must never look successful when nothing happened. Reconnection is not
  // this gate's job: the driver already owns it.
  //
  // This is the ONE gate. "Shutting down" and "not connected" are refused
  // here with the same code, under the operation the CALLER named — so a
  // consumer never sees the same call surface reported under two different
  // names depending on which check happened to fire first.
  assertReady (operation) {
    if (this.#closing) {
      throw new RedisClientError(
        `disconnect() is in progress. Cannot execute '${operation}'.`,
        operation,
        'REDIS_UNAVAILABLE'
      )
    }

    const client = this.#client

    if (!client || client.status !== 'ready') {
      this.logger.debug?.(`Redis is not connected. Rejecting '${operation}'.`)

      throw new RedisClientError(
        `Redis is not connected. Cannot execute '${operation}'.`,
        operation,
        'REDIS_UNAVAILABLE'
      )
    }

    return client
  }
}

export { ConnectionManager }
export default ConnectionManager

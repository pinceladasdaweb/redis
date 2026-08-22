import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { describe, test } from 'node:test'
import SubscriptionManager from '../src/messaging/pubsub.js'
import createManualClock from './helpers/manual-clock.js'

const tick = () => new Promise((resolve) => setImmediate(resolve))

const createSubscriber = () => {
  const subscriber = new EventEmitter()

  subscriber.status = 'ready'
  subscriber.calls = []
  subscriber.subscribe = async (channel) => { subscriber.calls.push(['subscribe', channel]); return 1 }
  subscriber.unsubscribe = async (channel) => { subscriber.calls.push(['unsubscribe', channel]); return 0 }
  subscriber.psubscribe = async (pattern) => { subscriber.calls.push(['psubscribe', pattern]); return 1 }
  subscriber.punsubscribe = async (pattern) => { subscriber.calls.push(['punsubscribe', pattern]); return 0 }
  subscriber.quit = async () => { subscriber.calls.push(['quit']); return 'OK' }
  subscriber.disconnect = () => { subscriber.calls.push(['disconnect']) }

  return subscriber
}

const createManager = () => {
  const subscriber = createSubscriber()
  const clock = createManualClock()
  const events = []
  const logs = []
  let duplicates = 0

  const manager = new SubscriptionManager({
    connection: {
      assertReady: () => ({ duplicate: () => { duplicates++; return subscriber } })
    },
    clock,
    logger: {
      info () {},
      debug () {},
      warn: (message) => logs.push(['warn', message]),
      error: (message) => logs.push(['error', message])
    },
    emit: (...args) => events.push(args)
  })

  return { manager, subscriber, clock, events, logs, duplicates: () => duplicates }
}

describe('subscription manager', () => {
  test('creates the dedicated subscriber lazily and reuses it', async () => {
    const { manager, subscriber, duplicates } = createManager()

    assert.equal(duplicates(), 0, 'no connection before the first subscribe')

    await manager.subscribe('a')
    await manager.subscribe('b')
    await manager.psubscribe('c.*')

    assert.equal(duplicates(), 1, 'all subscriptions share one connection')
    assert.deepEqual(subscriber.calls, [['subscribe', 'a'], ['subscribe', 'b'], ['psubscribe', 'c.*']])
  })

  test('routes messages to handlers and facade events', async () => {
    const { manager, subscriber, events } = createManager()
    const received = []

    await manager.subscribe('news', (message, channel) => received.push(['channel', channel, message]))
    await manager.psubscribe('logs.*', (message, channel, pattern) => received.push(['pattern', pattern, channel, message]))

    subscriber.emit('message', 'news', 'hello')
    subscriber.emit('pmessage', 'logs.*', 'logs.app', 'entry')
    await tick()

    assert.deepEqual(received, [
      ['channel', 'news', 'hello'],
      ['pattern', 'logs.*', 'logs.app', 'entry']
    ])
    assert.deepEqual(events, [
      ['message', 'news', 'hello'],
      ['pmessage', 'logs.*', 'logs.app', 'entry']
    ])
  })

  test('messages without a handler still reach the facade events', async () => {
    const { manager, subscriber, events } = createManager()

    await manager.subscribe('news')
    subscriber.emit('message', 'news', 'hello')
    await tick()

    assert.deepEqual(events, [['message', 'news', 'hello']])
  })

  test('a message with no handler is delivered silently', async () => {
    const { manager, subscriber, logs } = createManager()

    await manager.subscribe('news')
    subscriber.emit('message', 'news', 'hello')
    await tick()

    assert.deepEqual(logs, [], 'a handler-less channel must not produce errors')
  })

  test('a pattern-only subscription is also reported as lost on a permanent end', async () => {
    const { manager, subscriber, logs } = createManager()

    await manager.psubscribe('logs.*', () => {})
    // Probed: a give-up emits close before end, in one stack.
    subscriber.emit('close')
    subscriber.emit('end')

    assert.equal(logs.filter(([level]) => level === 'warn').length, 1)
    assert.match(logs.at(-1)[1], /subscriptions were lost/)
  })

  test('a rejecting handler is logged instead of crashing the process', async () => {
    const { manager, subscriber, logs } = createManager()

    await manager.subscribe('boom', async () => { throw new Error('handler exploded') })

    subscriber.emit('message', 'boom', 'x')
    await tick()

    assert.deepEqual(logs.filter(([level]) => level === 'error').length, 1)
    assert.match(logs[0][1], /handler for 'boom' failed.*handler exploded/)
  })

  test('re-subscribing replaces the handler (last one wins)', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    await manager.subscribe('news', () => received.push('first'))
    await manager.subscribe('news', () => received.push('second'))

    subscriber.emit('message', 'news', 'x')
    await tick()

    assert.deepEqual(received, ['second'])
  })

  test('a failed subscribe restores the previous handler', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    await manager.subscribe('news', () => received.push('original'))

    subscriber.subscribe = async () => { throw new Error('subscribe failed') }
    await assert.rejects(manager.subscribe('news', () => received.push('replacement')), /subscribe failed/)

    subscriber.emit('message', 'news', 'x')
    await tick()

    assert.deepEqual(received, ['original'], 'the failed handler must not stay registered')
  })

  test('a failed first subscribe leaves no handler behind', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    subscriber.subscribe = async () => { throw new Error('nope') }
    await assert.rejects(manager.subscribe('news', () => received.push('ghost')), /nope/)

    subscriber.subscribe = async () => 1
    await manager.subscribe('news')

    subscriber.emit('message', 'news', 'x')
    await tick()

    assert.deepEqual(received, [])
  })

  test('the same restore rules apply to patterns', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    await manager.psubscribe('a.*', () => received.push('original'))

    subscriber.psubscribe = async () => { throw new Error('psubscribe failed') }
    await assert.rejects(manager.psubscribe('a.*', () => received.push('replacement')), /psubscribe failed/)

    subscriber.emit('pmessage', 'a.*', 'a.b', 'x')
    await tick()

    assert.deepEqual(received, ['original'])
  })

  test('a failed subscribe without a handler leaves the registry untouched', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    await manager.subscribe('news', () => received.push('original'))

    subscriber.subscribe = async () => { throw new Error('nope') }
    await assert.rejects(manager.subscribe('news'), /nope/)

    subscriber.emit('message', 'news', 'x')
    await tick()

    assert.deepEqual(received, ['original'], 'a handler-less retry must not drop the existing handler')
  })

  test('unsubscribing before any subscription is a no-op', async () => {
    const { manager, duplicates } = createManager()

    assert.equal(await manager.unsubscribe('news'), 0)
    assert.equal(await manager.punsubscribe('logs.*'), 0)
    assert.equal(duplicates(), 0, 'unsubscribing must not open a connection')
  })

  test('unsubscribing stops delivery to the handler', async () => {
    const { manager, subscriber } = createManager()
    const received = []

    await manager.subscribe('news', () => received.push('x'))
    await manager.unsubscribe('news')

    subscriber.emit('message', 'news', 'ignored')
    await tick()

    assert.deepEqual(received, [])
  })

  test('close quits the subscriber, clears handlers and is idempotent', async () => {
    const { manager, subscriber } = createManager()

    await manager.subscribe('news', () => {})
    await manager.close()

    assert.deepEqual(subscriber.calls.at(-1), ['quit'])
    assert.equal(subscriber.listenerCount('message'), 0, 'listeners must be detached')

    subscriber.calls.length = 0
    await manager.close()
    assert.deepEqual(subscriber.calls, [], 'closing twice must not touch the connection again')
  })

  test('close forces the socket closed when quit fails', async () => {
    const { manager, subscriber } = createManager()

    await manager.subscribe('news')
    subscriber.quit = async () => { throw new Error('quit failed') }

    await manager.close()

    assert.deepEqual(subscriber.calls.at(-1), ['disconnect'])
  })

  // Regression: this close() runs FIRST on the facade's disconnect(), and a
  // quit() parked behind the driver's offline queue never answers — an
  // unbounded wait here hung the entire shutdown before it reached the main
  // connection, which does have a deadline.
  test('close gives up on a subscriber whose quit never answers', async () => {
    const { manager, subscriber, clock } = createManager()

    await manager.subscribe('news', () => {})

    subscriber.quit = () => {
      subscriber.calls.push(['quit'])

      return new Promise(() => {})
    }

    let done = false
    const closing = manager.close().then(() => { done = true })

    await clock.advance(1999)
    assert.equal(done, false, 'one millisecond before the deadline it is still waiting')

    await clock.advance(1)
    await closing

    assert.equal(done, true, 'and on the deadline the shutdown continues')
    assert.deepEqual(subscriber.calls.at(-1), ['disconnect'], 'the socket is forced closed instead')
  })

  test('close skips quit for an already ended connection', async () => {
    const { manager, subscriber } = createManager()

    await manager.subscribe('news')
    subscriber.status = 'end'
    subscriber.calls.length = 0

    await manager.close()

    assert.deepEqual(subscriber.calls, [])
  })

  test('a permanent end releases the subscriber and warns about lost subscriptions', async () => {
    const { manager, subscriber, logs, duplicates } = createManager()

    await manager.subscribe('news', () => {})
    subscriber.emit('close')
    subscriber.emit('end')

    assert.equal(logs.filter(([level]) => level === 'warn').length, 1)
    assert.match(logs.at(-1)[1], /subscriptions were lost/)

    // A later subscribe starts a fresh connection instead of using the dead one.
    await manager.subscribe('news')
    assert.equal(duplicates(), 2)
  })

  test('an end with no active subscriptions does not warn', async () => {
    const { manager, subscriber, logs } = createManager()

    await manager.subscribe('news')
    await manager.unsubscribe('news')
    subscriber.emit('end')

    assert.deepEqual(logs.filter(([level]) => level === 'warn'), [])
  })

  test('subscriber errors surface as connectionError, never as error', async () => {
    const { manager, subscriber, events, logs } = createManager()

    await manager.subscribe('news')
    subscriber.emit('error', new Error('socket died'))

    assert.deepEqual(events.at(-1)[0], 'connectionError')
    assert.match(logs.at(-1)[1], /subscriber error.*socket died/)
  })
})

// Keyspace events are the one channel family that is NOT broadcast across the
// cluster bus: every node emits them for its own slots only, while a cluster
// subscriber attaches to a single sampled node. A plain subscribe() therefore
// delivers one shard's events and looks exactly like one that works.
describe('cluster keyspace-event fan-out', () => {
  const KEY_EVENT = '__keyevent@0__:expired'

  const createClusterManager = (ports = [7001, 7002, 7003]) => {
    const clock = createManualClock()
    const events = []
    const logs = []

    const makeMaster = (port) => {
      const node = { options: { host: '127.0.0.1', port } }

      node.duplicate = () => {
        node.subscriber = createSubscriber()

        return node.subscriber
      }

      return node
    }

    const masters = ports.map(makeMaster)
    const replica = makeMaster(7101)
    const cluster = new EventEmitter()

    // Faithful to ioredis: nodes(role) filters. A fake that ignores the role
    // would approve `nodes('')` — which returns masters AND replicas, so
    // keyspace events would be delivered twice per shard.
    cluster.nodes = (role) => {
      if (role === 'master') return masters
      if (role === 'slave') return [replica]
      if (role === 'all' || role === undefined) return [...masters, replica]

      throw new Error(`ioredis rejects an unknown role: ${JSON.stringify(role)}`)
    }
    cluster.duplicate = () => {
      throw new Error('a cluster must never be duplicated for node-local events')
    }

    // `current` is swappable so a test can model the reconnect cycle that
    // replaces the Cluster instance out from under the collaborators.
    const connection = { current: cluster, assertReady: () => connection.current }

    const manager = new SubscriptionManager({
      connection,
      logger: {
        info () {},
        debug () {},
        warn: (message) => logs.push(['warn', message]),
        error: (message) => logs.push(['error', message])
      },
      clock,
      emit: (...args) => events.push(args)
    })

    return { manager, masters, cluster, connection, clock, events, logs, addMaster: (port) => masters.push(makeMaster(port)) }
  }

  test('every master gets its own subscriber', async () => {
    const { manager, masters } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    for (const node of masters) {
      assert.deepEqual(
        node.subscriber.calls,
        [['subscribe', KEY_EVENT]],
        `master ${node.options.port} must be subscribed`
      )
    }
  })

  test('events from every shard reach the handler', async () => {
    const { manager, masters } = createClusterManager()
    const seen = []

    await manager.subscribeEverywhere(KEY_EVENT, (message) => seen.push(message))

    masters.forEach((node, index) => node.subscriber.emit('message', KEY_EVENT, `key-${index}`))
    await tick()

    assert.deepEqual(seen, ['key-0', 'key-1', 'key-2'], 'no shard may be silently missing')
  })

  test('a master added by resharding is subscribed too', async () => {
    const { manager, masters, cluster, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    addMaster(7004)
    cluster.emit('+node')
    await tick()

    assert.deepEqual(masters.at(-1).subscriber.calls, [['subscribe', KEY_EVENT]])
    assert.equal(masters[0].subscriber.calls.length, 1, 'the existing ones are not re-subscribed')
  })

  test('unsubscribe reaches every node', async () => {
    const { manager, masters } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.unsubscribe(KEY_EVENT)

    for (const node of masters) {
      assert.ok(
        node.subscriber.calls.some((call) => call[0] === 'unsubscribe' && call[1] === KEY_EVENT),
        `master ${node.options.port} must be told to unsubscribe`
      )
    }
  })

  // Review finding: these connections, the '+node' watcher and the resync tick
  // exist only to serve node-local channels, and nothing released them when
  // the last one went away — #resyncTopology returns early on an empty set. An
  // app that used keyspace events for a maintenance window kept one idle
  // connection per master for the rest of the process.
  test('unsubscribing the last node channel releases the whole fan-out', async () => {
    const { manager, masters, cluster, clock, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.subscribeEverywhere('__keyevent@0__:evicted', () => {})

    await manager.unsubscribe(KEY_EVENT)

    assert.equal(cluster.listenerCount('+node'), 1, 'a channel is still registered — the watcher stays')
    for (const node of masters) {
      assert.equal(node.subscriber.calls.some((call) => call[0] === 'quit'), false)
    }

    await manager.unsubscribe('__keyevent@0__:evicted')

    for (const node of masters) {
      assert.ok(node.subscriber.calls.some((call) => call[0] === 'quit'), 'the per-node connection must be closed')
    }

    assert.equal(cluster.listenerCount('+node'), 0, 'and the topology watch detached')

    // The resync tick is gone too: a new master must not resurrect a fan-out
    // that has no channels left to carry.
    addMaster(7004)
    cluster.emit('+node')
    await clock.advance(10000)
    await tick()

    assert.equal(masters.at(-1).subscriber, undefined)
  })

  test('a node subscriber that dies for good is released and reported', async () => {
    const { manager, masters, logs } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    masters[1].subscriber.emit('close')
    masters[1].subscriber.emit('end')

    assert.match(logs.at(-1)[1], /node 127\.0\.0\.1:7002 ended permanently/)
    assert.equal(masters[1].subscriber.listenerCount('message'), 0, 'the dead client is detached')

    // The slot is free again, so the next fan-out rebuilds that shard.
    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    assert.notEqual(masters[1].subscriber.listenerCount('message'), 0, 'a fresh subscriber replaces it')
  })

  // Review finding: the fan-out subscribed a node to the channel being added
  // and nothing else. When the call had to REBUILD a node's subscriber, it
  // came back carrying only the newest channel — and the resync, which asked
  // "does this node have a subscriber?" rather than "is it subscribed to
  // everything?", skipped it forever after. One shard, silently short one
  // channel, for the life of the process.
  test('a rebuilt node subscriber catches up on every registered channel', async () => {
    const { manager, masters } = createClusterManager()
    const OTHER = '__keyevent@0__:evicted'
    const expired = []
    const evicted = []

    await manager.subscribeEverywhere(KEY_EVENT, (message) => expired.push(message))

    // 7002's subscriber gives up for good, so the slot is empty again...
    masters[1].subscriber.emit('end')

    // ...and a DIFFERENT channel is what rebuilds it.
    await manager.subscribeEverywhere(OTHER, (message) => evicted.push(message))

    assert.deepEqual(
      masters[1].subscriber.calls.map(([, channel]) => channel).sort(),
      [OTHER, KEY_EVENT].sort(),
      'the rebuilt subscriber must carry what was registered before it, not only the newest channel'
    )
    assert.equal(masters[0].subscriber.calls.length, 2, 'a healthy master just takes the new channel')

    // The point of all of it: that shard still delivers both.
    masters[1].subscriber.emit('message', KEY_EVENT, 'from 7002')
    masters[1].subscriber.emit('message', OTHER, 'also from 7002')
    await tick()

    assert.deepEqual(expired, ['from 7002'])
    assert.deepEqual(evicted, ['also from 7002'])
  })

  // The other half of the same defect: a node that answered SUBSCRIBE for one
  // channel and refused the next was left registered as if it were complete.
  test('the resync finishes a node subscribe that failed partway', async () => {
    const { manager, masters, cluster, clock, logs, addMaster } = createClusterManager()
    const OTHER = '__keyevent@0__:evicted'

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.subscribeEverywhere(OTHER, () => {})

    // A master joins while it is still LOADING, and refuses the second channel.
    addMaster(7004)
    const joining = masters.at(-1)
    const build = joining.duplicate
    let loading = true

    joining.duplicate = () => {
      const subscriber = build.call(joining)
      const real = subscriber.subscribe

      subscriber.subscribe = async (channel) => {
        if (loading && channel === OTHER) throw new Error('LOADING Redis is loading the dataset in memory')

        return real.call(subscriber, channel)
      }

      return subscriber
    }

    cluster.emit('+node')
    await tick()

    assert.deepEqual(
      joining.subscriber.calls,
      [['subscribe', KEY_EVENT]],
      'the first channel lands, the second is refused — and the node looks healthy from the map'
    )
    assert.match(logs.at(-1)[1], /cluster node 127\.0\.0\.1:7004.*LOADING/)

    loading = false
    await clock.advance(10000)
    await tick()

    assert.deepEqual(
      joining.subscriber.calls,
      [['subscribe', KEY_EVENT], ['subscribe', OTHER]],
      'the resync must retry the channel the node never confirmed, and only that one'
    )
  })

  test('a node that refuses the subscription is reported, not swallowed silently', async () => {
    const { manager, masters, cluster, logs, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    addMaster(7004)
    const joining = masters.at(-1)
    const failing = joining.duplicate
    joining.duplicate = () => {
      const subscriber = failing()
      subscriber.subscribe = async () => { throw new Error('LOADING Redis is loading the dataset') }

      return subscriber
    }

    cluster.emit('+node')
    await tick()

    assert.match(logs.at(-1)[1], /extend keyspace-event subscriptions to cluster node 127\.0\.0\.1:7004.*LOADING/)
  })

  test('resharding before any subscription does no work', async () => {
    const { cluster, masters, addMaster } = createClusterManager()

    cluster.emit('+node')
    addMaster(7004)
    cluster.emit('+node')
    await tick()

    assert.deepEqual(
      masters.map((node) => node.subscriber),
      [undefined, undefined, undefined, undefined],
      'no channels means no connections to open'
    )
  })

  test('a handler is optional: the facade events still carry every shard', async () => {
    const { manager, masters, events } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT)

    masters[2].subscriber.emit('message', KEY_EVENT, 'ct:orphan')
    await tick()

    assert.deepEqual(events.at(-1), ['message', KEY_EVENT, 'ct:orphan'])
  })

  test('the topology watcher is attached once, not once per channel', async () => {
    const { manager, cluster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.subscribeEverywhere('__keyevent@0__:evicted', () => {})

    assert.equal(cluster.listenerCount('+node'), 1)

    await manager.close()

    assert.equal(cluster.listenerCount('+node'), 0, 'and detached on shutdown')
  })

  test('a cluster still refreshing its slot map reports no subscriptions yet', async () => {
    const { manager } = createClusterManager([])

    assert.equal(await manager.subscribeEverywhere(KEY_EVENT, () => {}), 0)
  })

  test('a non-Error from a node subscriber is still readable', async () => {
    const { manager, masters, logs, events } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    masters[0].subscriber.emit('error', 'socket reset with no Error wrapper')

    assert.match(logs.at(-1)[1], /socket reset with no Error wrapper/)
    assert.deepEqual(events.at(-1), ['connectionError', 'socket reset with no Error wrapper'])
  })

  // Review finding: a partial fan-out failure used to leave two of three
  // shards delivering to a handler the caller believes was never installed,
  // and the failed shard permanently silent. The call is atomic now.
  test('a partial fan-out failure rolls everything back and can be retried', async () => {
    const { manager, masters } = createClusterManager()
    const before = []
    const after = []

    await manager.subscribeEverywhere(KEY_EVENT, (m) => before.push(m))

    // Node 7002 refuses the next subscribe.
    let refuse = true
    const failing = masters[1].duplicate
    masters[1].duplicate = () => {
      const subscriber = failing.call(masters[1])
      const real = subscriber.subscribe
      subscriber.subscribe = async (channel) => {
        if (refuse) throw new Error('CLUSTERDOWN The cluster is down')
        return real.call(subscriber, channel)
      }
      return subscriber
    }

    // Force a fresh subscriber on 7002 so the failing subscribe is reached.
    await manager.close()

    // The undo on a succeeded shard is best effort: even if IT fails too,
    // the original error is what surfaces.
    const undoRefused = masters[0]
    const originalDuplicate = undoRefused.duplicate
    undoRefused.duplicate = () => {
      const subscriber = originalDuplicate.call(undoRefused)
      const realUnsubscribe = subscriber.unsubscribe
      subscriber.unsubscribe = async (ch) => {
        await realUnsubscribe.call(subscriber, ch)
        throw new Error('connection already gone')
      }
      return subscriber
    }

    await assert.rejects(
      manager.subscribeEverywhere(KEY_EVENT, (m) => after.push(m)),
      /CLUSTERDOWN/,
      'one refusing master must fail the whole call'
    )

    // The masters that DID subscribe were told to undo it...
    assert.deepEqual(
      masters[0].subscriber.calls.at(-1),
      ['unsubscribe', KEY_EVENT],
      'a succeeded shard must not keep delivering to a rolled-back handler'
    )

    // ...and the one that REFUSED gets no undo — there is nothing to undo,
    // and an unsubscribe against it would mask which shard actually failed.
    assert.equal(
      masters[1].subscriber.calls.some((c) => c[0] === 'unsubscribe'),
      false,
      'the failed shard must not receive an undo'
    )

    // ...and the rolled-back handler receives nothing.
    masters[0].subscriber.emit('message', KEY_EVENT, 'ghost')
    await tick()
    assert.deepEqual(after, [], 'the failed call must not leave its handler behind')

    // A retry of the whole call succeeds once the shard recovers.
    refuse = false
    await manager.subscribeEverywhere(KEY_EVENT, (m) => after.push(m))
    masters[1].subscriber.emit('message', KEY_EVENT, 'recovered')
    await tick()
    assert.deepEqual(after, ['recovered'])
  })

  // Review finding: the '+node' watcher was keyed on presence, not on client
  // identity — after a reconnect cycle it stayed bound to the dead cluster
  // and resharding lost shards silently forever.
  test('the topology watcher follows the live client across a reconnect cycle', async () => {
    const { manager, cluster, connection } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    assert.equal(cluster.listenerCount('+node'), 1)

    // The driver gave up; a reconnect builds a brand-new Cluster instance.
    const clusterB = new EventEmitter()
    const newMaster = { options: { host: '127.0.0.1', port: 8001 } }
    newMaster.duplicate = () => {
      newMaster.subscriber = createSubscriber()
      return newMaster.subscriber
    }
    clusterB.nodes = (role) => (role === 'master' ? [newMaster] : [])
    connection.current = clusterB

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    assert.equal(cluster.listenerCount('+node'), 0, 'the dead client must be released')
    assert.equal(clusterB.listenerCount('+node'), 1, 'the live client must be watched')
    assert.deepEqual(newMaster.subscriber.calls, [['subscribe', KEY_EVENT]])

    // Old masters keep whatever they had; the point is the NEW topology works.
    clusterB.emit('+node')
    await tick()
    assert.equal(clusterB.listenerCount('+node'), 1, 'no duplicate watchers')
  })

  // Review finding: a replica promoted to master never fires '+node' (the
  // driver only emits it for a host:port new to the pool), so the event alone
  // cannot keep the fan-out complete. The periodic resync is the guarantee.
  test('the periodic resync subscribes promoted masters and releases departed ones', async () => {
    const { manager, masters, clock, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    // A promotion: new master appears with NO '+node' event...
    addMaster(7004)
    // ...and a departure: 7003 is no longer a master. Its release is best
    // effort — even a socket that refuses its own teardown must not explode.
    const departed = masters.splice(2, 1)[0]
    const departedQuit = departed.subscriber.quit
    departed.subscriber.quit = async () => {
      await departedQuit()
      throw new Error('teardown refused')
    }
    departed.subscriber.disconnect = () => { throw new Error('socket gone') }

    await clock.advance(10000)
    await tick()

    assert.deepEqual(
      masters.at(-1).subscriber.calls,
      [['subscribe', KEY_EVENT]],
      'the promoted master must be subscribed without any event'
    )
    assert.deepEqual(
      departed.subscriber.calls.at(-1),
      ['quit'],
      'the departed master\'s subscriber must be released, not left retrying forever'
    )
  })

  // The resync tick asks the CONNECTION for the live client — never the
  // watcher, whose captured client is exactly what a reconnect makes stale.
  // Between cycles there is no client at all, and the tick must simply wait.
  test('the resync tick follows the connection through a full reconnect cycle', async () => {
    const { manager, cluster, connection, clock } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    // Mid-cycle: no client. The tick must be a silent no-op.
    connection.current = null
    connection.assertReady = () => { throw Object.assign(new Error('down'), { code: 'REDIS_UNAVAILABLE' }) }
    await clock.advance(10000)
    await tick()

    // Reconnected as a brand-new cluster: the NEXT tick alone must re-arm the
    // watcher and subscribe the new topology, with no subscribe call needed.
    const clusterB = new EventEmitter()
    const master = { options: { host: '127.0.0.1', port: 9001 } }
    master.duplicate = () => {
      master.subscriber = createSubscriber()
      return master.subscriber
    }
    clusterB.nodes = (role) => (role === 'master' ? [master] : [])
    connection.assertReady = () => clusterB

    await clock.advance(10000)
    await tick()

    assert.equal(cluster.listenerCount('+node'), 0, 'the dead client is released by the tick itself')
    assert.equal(clusterB.listenerCount('+node'), 1, 'the live client is watched')
    assert.deepEqual(master.subscriber.calls, [['subscribe', KEY_EVENT]], 'the new topology is subscribed')
  })

  // A standalone client between the fan-out and the tick (e.g. the facade was
  // reconfigured): the tick must not treat it as a cluster.
  test('the resync tick ignores a client with no nodes()', async () => {
    const { manager, connection, clock, logs } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    connection.assertReady = () => ({ duplicate: () => createSubscriber() })
    await clock.advance(10000)
    await tick()

    assert.deepEqual(logs.filter(([level]) => level === 'error'), [], 'no resync against a standalone client')
  })

  test('a resync with no registered channels does no work', async () => {
    const { manager, masters, cluster, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.unsubscribe(KEY_EVENT)

    for (const node of masters) {
      node.subscriber.calls.length = 0
    }

    // A master joining while nothing is registered must not even get a
    // connection opened toward it.
    addMaster(7004)
    cluster.emit('+node')
    await tick()

    for (const node of masters.slice(0, 3)) {
      assert.deepEqual(node.subscriber.calls, [], 'no channels means nothing to reconcile')
    }

    assert.equal(masters.at(-1).subscriber, undefined, 'and no subscriber is created for the newcomer')
  })

  // Found by the mutation gate, in the catch-up fix itself: the rollback used
  // to skip every master whose promise rejected. Once a node can be asked for
  // more than one channel, it can take the NEW one and fail on an older one —
  // rejected, while holding exactly what the rollback exists to withdraw. What
  // the node took is the question; whether its promise settled is not.
  test('a shard that took the channel and then failed is still undone', async () => {
    const { manager, masters } = createClusterManager()
    const OTHER = '__keyevent@0__:evicted'

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    // 7002 loses its subscriber, so the next call has to rebuild it and catch
    // it up on KEY_EVENT — and that catch-up is what fails.
    masters[1].subscriber.emit('end')

    const build = masters[1].duplicate
    masters[1].duplicate = () => {
      const subscriber = build.call(masters[1])
      const real = subscriber.subscribe

      subscriber.subscribe = async (channel) => {
        if (channel === KEY_EVENT) throw new Error('CLUSTERDOWN The cluster is down')

        return real.call(subscriber, channel)
      }

      return subscriber
    }

    await assert.rejects(manager.subscribeEverywhere(OTHER, () => {}), /CLUSTERDOWN/)

    assert.deepEqual(
      masters[1].subscriber.calls,
      [['subscribe', OTHER], ['unsubscribe', OTHER]],
      'the shard took the new channel, so the rollback must take it back'
    )
  })

  // The undo runs after an await, and a shard's connection can die inside it:
  // the node answered SUBSCRIBE, then ended for good and dropped out of the
  // registry before the rollback came looking for it. There is nothing left to
  // undo there, and reaching into the missing entry would turn a partial
  // failure into a crash.
  test('the rollback tolerates a shard whose connection died in the meantime', async () => {
    const { manager, masters } = createClusterManager()

    masters[2].duplicate = () => {
      const subscriber = createSubscriber()

      subscriber.subscribe = async () => {
        // 7001 answered, then lost its connection for good before the undo.
        masters[0].subscriber.emit('end')

        throw new Error('CLUSTERDOWN The cluster is down')
      }

      masters[2].subscriber = subscriber

      return subscriber
    }

    await assert.rejects(manager.subscribeEverywhere(KEY_EVENT, () => {}), /CLUSTERDOWN/)

    assert.equal(
      masters[0].subscriber.calls.some((call) => call[0] === 'unsubscribe'),
      false,
      'a shard that is already gone has nothing to undo'
    )
    assert.deepEqual(
      masters[1].subscriber.calls.at(-1),
      ['unsubscribe', KEY_EVENT],
      'the shard that is still alive is undone as usual'
    )
  })

  // The rollback must deregister ONLY what the failed call added: a channel
  // that was already registered before it stays registered, or a later
  // resync would silently stop covering it.
  test('a failed RE-subscribe keeps the channel registered for the resync', async () => {
    const { manager, masters, cluster, addMaster } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    // The re-subscribe fails on one master...
    masters[1].subscriber.subscribe = async () => { throw new Error('CLUSTERDOWN') }
    await assert.rejects(manager.subscribeEverywhere(KEY_EVENT, () => {}), /CLUSTERDOWN/)

    // ...but the channel was registered BEFORE the failed call, so a new
    // master must still be subscribed to it by the watcher.
    addMaster(7004)
    cluster.emit('+node')
    await tick()

    assert.deepEqual(masters.at(-1).subscriber.calls, [['subscribe', KEY_EVENT]],
      'the channel must survive the failed re-subscribe')
  })

  // The mirror of keeps-registered: a channel whose FIRST fan-out failed was
  // never established, so the rollback must deregister it — or the watcher
  // would subscribe new masters to a channel the caller believes failed.
  test('a failed first fan-out deregisters the channel from the watcher', async () => {
    const { manager, masters, cluster, addMaster } = createClusterManager()

    masters[1].duplicate = () => {
      const subscriber = createSubscriber()
      subscriber.subscribe = async () => { throw new Error('CLUSTERDOWN') }
      masters[1].subscriber = subscriber
      return subscriber
    }

    await assert.rejects(manager.subscribeEverywhere(KEY_EVENT, () => {}), /CLUSTERDOWN/)

    addMaster(7004)
    cluster.emit('+node')
    await tick()

    assert.equal(
      masters.at(-1).subscriber,
      undefined,
      'a channel that never established must not spread to new masters'
    )
  })

  test('the fan-out reports the last master count, and unsubscribe likewise', async () => {
    const { manager, masters } = createClusterManager()

    // Distinct per-node replies: only the LAST one may be reported, the same
    // convention the driver uses for variadic subscribes.
    masters.forEach((node, index) => {
      const original = node.duplicate
      node.duplicate = () => {
        const subscriber = original.call(node)
        subscriber.subscribe = async (ch) => { subscriber.calls.push(['subscribe', ch]); return index + 1 }
        subscriber.unsubscribe = async (ch) => { subscriber.calls.push(['unsubscribe', ch]); return (index + 1) * 10 }
        return subscriber
      }
    })

    assert.equal(await manager.subscribeEverywhere(KEY_EVENT, () => {}), 3)
    assert.equal(await manager.unsubscribe(KEY_EVENT), 30)
  })

  test('only one resync clock exists, however many cycles the watcher survives', async () => {
    const { manager, clock, connection } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    const after = clock.pending()

    // A reconnect cycle re-arms the watcher on a new client...
    const clusterB = new EventEmitter()
    clusterB.nodes = () => []
    connection.current = clusterB
    await manager.subscribeEverywhere(KEY_EVENT, () => {}).catch(() => {})

    assert.equal(clock.pending(), after, 're-arming must not stack a second interval')
  })

  test('close stops the resync clock', async () => {
    const { manager, clock } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.close()

    assert.equal(clock.pending(), 0, 'no timer may survive close()')
  })

  test('close releases every per-node subscriber', async () => {
    const { manager, masters } = createClusterManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})
    await manager.close()

    for (const node of masters) {
      assert.deepEqual(node.subscriber.calls.at(-1), ['quit'], `master ${node.options.port} must be released`)
      assert.equal(node.subscriber.listenerCount('message'), 0)
    }
  })

  test('the fan-out names its operation at the readiness gate', async () => {
    const { manager, connection } = createClusterManager()
    const asked = []

    const real = connection.assertReady.bind(connection)
    connection.assertReady = (operation) => {
      asked.push(operation)
      return real(operation)
    }

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    assert.deepEqual([...new Set(asked)], ['subscribe'],
      'a REDIS_UNAVAILABLE from this path must name the operation a caller recognizes')
  })

  test('outside a cluster it stays a single subscription', async () => {
    const { manager, subscriber, duplicates } = createManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    assert.equal(duplicates(), 1, 'standalone must not fan out')
    assert.deepEqual(subscriber.calls, [['subscribe', KEY_EVENT]])
  })
})

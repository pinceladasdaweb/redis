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
    const cluster = new EventEmitter()

    cluster.nodes = () => masters
    cluster.duplicate = () => {
      throw new Error('a cluster must never be duplicated for node-local events')
    }

    const manager = new SubscriptionManager({
      connection: { assertReady: () => cluster },
      logger: {
        info () {},
        debug () {},
        warn: (message) => logs.push(['warn', message]),
        error: (message) => logs.push(['error', message])
      },
      clock,
      emit: (...args) => events.push(args)
    })

    return { manager, masters, cluster, clock, events, logs, addMaster: (port) => masters.push(makeMaster(port)) }
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
      assert.deepEqual(node.subscriber.calls.at(-1), ['unsubscribe', KEY_EVENT])
    }
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

  test('outside a cluster it stays a single subscription', async () => {
    const { manager, subscriber, duplicates } = createManager()

    await manager.subscribeEverywhere(KEY_EVENT, () => {})

    assert.equal(duplicates(), 1, 'standalone must not fan out')
    assert.deepEqual(subscriber.calls, [['subscribe', KEY_EVENT]])
  })
})

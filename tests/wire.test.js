// Wire contract: every public method must reach ioredis with the exact
// command and arguments it promises. The facade runs for real — only the
// driver is faked — so a typo in a command name, a swapped argument or a
// method that calls a collaborator that does not exist fails here.

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { RedisClient } from '../src/index.js'
import createManualClock from './helpers/manual-clock.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

// Property names that must never be turned into recorded commands: awaiting
// or inspecting the fake would otherwise register bogus calls.
const NON_COMMANDS = new Set(['then', 'catch', 'finally', 'constructor', 'inspect'])

const createRecorder = () => {
  const calls = []
  // Commands still waiting for a reply. The driver flushes them with an error
  // when the connection closes; a fake that forgets to would let broken
  // shutdown code look correct.
  const waiting = new Set()

  const target = {
    status: 'ready',
    // Stands in for a command that only answers when told to, like XREAD BLOCK 0.
    blockForever: () => new Promise((resolve, reject) => waiting.add(reject)),
    flushWaiting: () => {
      for (const reject of waiting) {
        reject(new Error('Connection is closed.'))
      }

      waiting.clear()
    },
    // Custom commands do not exist until defineCommand creates them — the
    // fake must reproduce that, or the lock manager's registration is skipped.
    releaseLock: undefined,
    extendLock: undefined,
    // A standalone client has no nodes(): leaving it to the proxy would make
    // every fake look like a cluster.
    nodes: undefined,
    on () { return target },
    once () { return target },
    removeAllListeners () { return target },
    disconnect () {
      calls.push(['<disconnect>'])
      target.flushWaiting()
    },
    async quit () { calls.push(['<quit>']); return 'OK' },
    multi () { calls.push(['multi']); return { exec: async () => [] } },
    defineCommand (name) {
      target[name] = async (...args) => {
        calls.push([name, ...args])
        return 1
      }
    }
  }

  const client = new Proxy(target, {
    get (t, prop) {
      if (prop in t) return t[prop]
      if (typeof prop !== 'string' || NON_COMMANDS.has(prop)) return undefined

      return async (...args) => {
        calls.push([prop, ...args])
        return 'OK'
      }
    }
  })

  target.duplicate = () => client

  return { client, calls }
}

const createClient = (options = {}) => {
  const { client: fake, calls } = createRecorder()
  const redis = new RedisClient({ logger: quietLogger, ...options })

  const connection = {
    client: fake,
    isConnected: true,
    assertReady: () => fake,
    connect: async () => {},
    disconnect: async () => {}
  }

  // Every collaborator captured the real connection at construction, so each
  // one has to be pointed at the fake. A new collaborator that is missed here
  // fails with REDIS_UNAVAILABLE, which reads like a library bug and is not.
  redis.connection = connection
  redis.locks.connection = connection
  redis.subscriptions.connection = connection
  redis.scripts.connection = connection
  redis.health.getClient = () => fake

  return { redis, calls, fake }
}

// Each entry: [method arguments] -> expected wire call.
const CONTRACTS = [
  ['get', ['k'], ['get', 'k']],
  ['set', ['k', 'v'], ['set', 'k', 'v']],
  ['setex', ['k', 60, 'v'], ['setex', 'k', 60, 'v']],
  ['del', ['a', 'b'], ['del', 'a', 'b']],
  ['incr', ['k'], ['incr', 'k']],
  ['decr', ['k'], ['decr', 'k']],
  ['exists', ['k'], ['exists', 'k']],
  ['type', ['k'], ['type', 'k']],
  ['rename', ['a', 'b'], ['rename', 'a', 'b']],
  ['renamenx', ['a', 'b'], ['renamenx', 'a', 'b']],
  ['persist', ['k'], ['persist', 'k']],
  ['expire', ['k', 30], ['expire', 'k', 30]],
  ['ttl', ['k'], ['ttl', 'k']],
  ['mget', ['a', 'b'], ['mget', 'a', 'b']],
  ['hget', ['h', 'f'], ['hget', 'h', 'f']],
  ['hgetall', ['h'], ['hgetall', 'h']],
  ['hmget', ['h', 'f1', 'f2'], ['hmget', 'h', 'f1', 'f2']],
  ['hincrby', ['h', 'f', 2], ['hincrby', 'h', 'f', 2]],
  ['hexists', ['h', 'f'], ['hexists', 'h', 'f']],
  ['hdel', ['h', 'f1', 'f2'], ['hdel', 'h', 'f1', 'f2']],
  ['lpush', ['l', 'a', 'b'], ['lpush', 'l', 'a', 'b']],
  ['rpop', ['l'], ['rpop', 'l']],
  ['lrange', ['l', 0, -1], ['lrange', 'l', 0, -1]],
  ['llen', ['l'], ['llen', 'l']],
  ['lrem', ['l', 2, 'v'], ['lrem', 'l', 2, 'v']],
  ['lpushx', ['l', 'v'], ['lpushx', 'l', 'v']],
  ['rpushx', ['l', 'v'], ['rpushx', 'l', 'v']],
  ['sadd', ['s', 'a', 'b'], ['sadd', 's', 'a', 'b']],
  ['smembers', ['s'], ['smembers', 's']],
  ['sismember', ['s', 'm'], ['sismember', 's', 'm']],
  ['scard', ['s'], ['scard', 's']],
  ['srem', ['s', 'a', 'b'], ['srem', 's', 'a', 'b']],
  ['zcard', ['z'], ['zcard', 'z']],
  ['zcount', ['z', 0, 100], ['zcount', 'z', 0, 100]],
  ['zrank', ['z', 'm'], ['zrank', 'z', 'm']],
  ['zrevrank', ['z', 'm'], ['zrevrank', 'z', 'm']],
  ['zrem', ['z', 'a', 'b'], ['zrem', 'z', 'a', 'b']],
  ['zremrangebyrank', ['z', 0, 9], ['zremrangebyrank', 'z', 0, 9]],
  ['zremrangebyscore', ['z', '-inf', 10], ['zremrangebyscore', 'z', '-inf', 10]],
  ['xadd', ['st', '*', 'f', 'v'], ['xadd', 'st', '*', 'f', 'v']],
  ['xlen', ['st'], ['xlen', 'st']],
  ['xinfo', ['STREAM', 'st'], ['xinfo', 'STREAM', 'st']],
  ['xdel', ['st', '1-1', '1-2'], ['xdel', 'st', '1-1', '1-2']],
  ['xack', ['st', 'g', '1-1', '1-2'], ['xack', 'st', 'g', '1-1', '1-2']],
  ['xclaim', ['st', 'g', 'c', 1000, '1-1'], ['xclaim', 'st', 'g', 'c', 1000, '1-1']],
  ['xrange', ['st', '-', '+'], ['xrange', 'st', '-', '+']],
  ['xrevrange', ['st', '+', '-'], ['xrevrange', 'st', '+', '-']],
  ['xpending', ['st', 'g'], ['xpending', 'st', 'g']],
  ['publish', ['ch', 'msg'], ['publish', 'ch', 'msg']]
]

describe('wire contract', () => {
  for (const [method, args, expected] of CONTRACTS) {
    test(`${method}() sends ${expected[0].toUpperCase()} with the right arguments`, async () => {
      const { redis, calls } = createClient()

      await redis[method](...args)

      assert.equal(calls.length, 1, `${method}() must issue exactly one command`)
      assert.deepEqual(calls[0], expected)
    })
  }

  test('every public method is covered by a contract or an explicit exception', () => {
    const EXCEPTIONS = new Set([
      // lifecycle / non-command surface
      'constructor', 'connect', 'disconnect', 'checkHealth', 'withDedicatedConnection',
      'executeCommand', 'executeBlockingCommand', 'logError', 'omitPrefix', '_getAllStream',
      // asserted in dedicated tests below
      'getJson', 'setJson', 'setexJson', 'getOrSet', 'getOrSetJson', 'deleteByPattern',
      'getAllStream', 'hset', 'hmset', 'mset', 'spop', 'sort', 'multi', 'watch', 'unwatch',
      'zadd', 'zscore', 'zincrby', 'zrange', 'zrevrange', 'zrangebyscore', 'zpopmin', 'zpopmax',
      'xautoclaim', 'keyspaceNotifications', 'subscribeToKeyEvents',
      'xread', 'xreadgroup', 'xgroup', 'xtrim', 'publishJson',
      'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe', 'acquireLock', 'withLock',
      'defineScript', 'runScript'
    ])
    const covered = new Set(CONTRACTS.map(([method]) => method))

    // Read descriptors, never values: accessing a getter off the prototype
    // would evaluate it without an instance.
    const surface = Object.getOwnPropertyNames(RedisClient.prototype)
      .filter((name) => typeof Object.getOwnPropertyDescriptor(RedisClient.prototype, name).value === 'function')

    const uncovered = surface.filter((name) => !covered.has(name) && !EXCEPTIONS.has(name))

    assert.deepEqual(uncovered, [], `public methods with no wire test: ${uncovered.join(', ')}`)
  })

  test('hset accepts pairs and objects', async () => {
    const { redis, calls } = createClient()

    await redis.hset('h', 'f', 'v')
    await redis.hset('h', { a: '1', b: '2' })
    await redis.hset('h', 'f1', 'v1', 'f2', 'v2')

    assert.deepEqual(calls[0], ['hset', 'h', 'f', 'v'])
    assert.deepEqual(calls[1], ['hset', 'h', { a: '1', b: '2' }])
    assert.deepEqual(calls[2], ['hset', 'h', 'f1', 'v1', 'f2', 'v2'])
  })

  test('json helpers serialize and deserialize explicitly', async () => {
    const { redis, calls, fake } = createClient()

    await redis.setJson('k', { a: 1 })
    await redis.setexJson('k', 60, { a: 1 })
    await redis.publishJson('ch', { a: 1 })

    assert.deepEqual(calls[0], ['set', 'k', '{"a":1}'])
    assert.deepEqual(calls[1], ['setex', 'k', 60, '{"a":1}'])
    assert.deepEqual(calls[2], ['publish', 'ch', '{"a":1}'])

    fake.get = async () => '{"a":1}'
    assert.deepEqual(await redis.getJson('k'), { a: 1 })

    fake.get = async () => null
    assert.equal(await redis.getJson('k'), null)
  })

  test('sort builds its clause in protocol order', async () => {
    const { redis, calls } = createClient()

    await redis.sort('l', {
      by: 'w_*',
      limit: { offset: 0, count: 10 },
      get: 'o_*',
      direction: 'DESC',
      alpha: true
    })

    assert.deepEqual(calls[0], ['sort', 'l', 'BY', 'w_*', 'LIMIT', 0, 10, 'GET', 'o_*', 'DESC', 'ALPHA'])
  })

  test('zadd accepts an object of members and raw arguments alike', async () => {
    const { redis, calls } = createClient()

    await redis.zadd('z', { ada: 100, alan: 90 })
    await redis.zadd('z', 100, 'ada')
    await redis.zadd('z', 'NX', 'CH', 50, 'grace')

    assert.deepEqual(calls[0], ['zadd', 'z', 100, 'ada', 90, 'alan'], 'score comes before member on the wire')
    assert.deepEqual(calls[1], ['zadd', 'z', 100, 'ada'])
    assert.deepEqual(calls[2], ['zadd', 'z', 'NX', 'CH', 50, 'grace'], 'flags must stay available')
  })

  // Regression: an empty member map used to reach the server as a bare ZADD
  // ("wrong number of arguments"), and an array was mistaken for a member map.
  test('zadd rejects an empty batch and leaves arrays to the driver', async () => {
    const { redis, calls } = createClient()

    await assert.rejects(redis.zadd('z', {}), { code: 'INVALID_ARGUMENT', operation: 'zadd' })
    await assert.rejects(redis.zadd('z'), { code: 'INVALID_ARGUMENT', operation: 'zadd' })
    assert.equal(calls.length, 0, 'nothing may be sent for an empty batch')

    await redis.zadd('z', [100, 'ada'])
    assert.deepEqual(calls[0], ['zadd', 'z', [100, 'ada']], 'arrays go through untouched')
  })

  test('sorted-set ranges build their clauses in protocol order', async () => {
    const { redis, calls } = createClient()

    await redis.zrange('z', 0, -1)
    await redis.zrange('z', 0, -1, { withScores: true })
    await redis.zrange('z', 10, 20, { byScore: true, rev: true, limit: { offset: 0, count: 5 }, withScores: true })
    await redis.zrevrange('z', 0, 9, { withScores: true })
    await redis.zrangebyscore('z', '-inf', '+inf', { withScores: true, limit: { offset: 5, count: 10 } })

    assert.deepEqual(calls[0], ['zrange', 'z', 0, -1])
    assert.deepEqual(calls[1], ['zrange', 'z', 0, -1, 'WITHSCORES'])
    assert.deepEqual(calls[2], ['zrange', 'z', 10, 20, 'BYSCORE', 'REV', 'LIMIT', 0, 5, 'WITHSCORES'])
    assert.deepEqual(calls[3], ['zrevrange', 'z', 0, 9, 'WITHSCORES'])
    assert.deepEqual(calls[4], ['zrangebyscore', 'z', '-inf', '+inf', 'WITHSCORES', 'LIMIT', 5, 10])
  })

  // Without WITHSCORES the reply is a bare member list and must be handed back
  // untouched — pairing it up would invent scores that were never asked for.
  test('ranges without WITHSCORES return the members as they came', async () => {
    const { redis, calls, fake } = createClient()

    fake.zrevrange = async (...args) => { calls.push(['zrevrange', ...args]); return ['grace', 'ada'] }
    fake.zrangebyscore = async (...args) => { calls.push(['zrangebyscore', ...args]); return ['ada', 'grace'] }

    assert.deepEqual(await redis.zrevrange('z', 0, 9), ['grace', 'ada'])
    assert.deepEqual(await redis.zrangebyscore('z', 0, 100), ['ada', 'grace'])

    assert.deepEqual(calls[0], ['zrevrange', 'z', 0, 9])
    assert.deepEqual(calls[1], ['zrangebyscore', 'z', 0, 100])
  })

  test('zrange speaks BYLEX for lexicographic ranges', async () => {
    const { redis, calls } = createClient()

    await redis.zrange('z', '[a', '[z', { byLex: true, limit: { offset: 0, count: 5 } })

    assert.deepEqual(calls[0], ['zrange', 'z', '[a', '[z', 'BYLEX', 'LIMIT', 0, 5])
  })

  test('scores are returned as numbers, and WITHSCORES as member/score pairs', async () => {
    const { redis, fake } = createClient()

    fake.zscore = async () => '42.5'
    fake.zincrby = async () => 'inf'
    fake.zrange = async () => ['ada', '100', 'alan', '90']
    fake.zpopmin = async () => ['alan', '90']

    assert.equal(await redis.zscore('z', 'ada'), 42.5)
    assert.equal(await redis.zincrby('z', 1, 'ada'), Number.POSITIVE_INFINITY)
    assert.deepEqual(await redis.zrange('z', 0, -1, { withScores: true }), [
      { member: 'ada', score: 100 },
      { member: 'alan', score: 90 }
    ])
    assert.deepEqual(await redis.zpopmin('z'), { member: 'alan', score: 90 })

    fake.zscore = async () => null
    assert.equal(await redis.zscore('z', 'ghost'), null, 'a missing member is null, never NaN')
  })

  test('zpopmin and zpopmax switch shape on the count argument', async () => {
    const { redis, calls, fake } = createClient()

    // Overrides must keep recording, or the wire assertions below see nothing.
    fake.zpopmin = async (...args) => { calls.push(['zpopmin', ...args]); return [] }
    fake.zpopmax = async (...args) => { calls.push(['zpopmax', ...args]); return ['ada', '100', 'alan', '90'] }

    assert.equal(await redis.zpopmin('z'), null, 'an empty set pops nothing')
    assert.deepEqual(await redis.zpopmax('z', 2), [
      { member: 'ada', score: 100 },
      { member: 'alan', score: 90 }
    ])

    assert.deepEqual(calls[0], ['zpopmin', 'z'], 'no count means no count on the wire')
    assert.deepEqual(calls[1], ['zpopmax', 'z', 2])
  })

  test('sort without options sends no clauses at all', async () => {
    const { redis, calls } = createClient()

    await redis.sort('l')
    await redis.sort('l', { alpha: true })
    await redis.sort('l', { by: 'w_*', direction: 'ASC' })

    assert.deepEqual(calls[0], ['sort', 'l'])
    assert.deepEqual(calls[1], ['sort', 'l', 'ALPHA'])
    assert.deepEqual(calls[2], ['sort', 'l', 'BY', 'w_*', 'ASC'])
  })

  test('stream reads without block stay on the shared connection and omit clauses', async () => {
    const { redis, calls } = createClient()

    await redis.xread({}, ['s', '0-0'])
    await redis.xreadgroup('g', 'c', {}, ['s', '>'])
    await redis.xreadgroup('g', 'c', { count: 10 }, ['s', '>'])

    assert.deepEqual(calls[0], ['xread', 'STREAMS', 's', '0-0'])
    assert.deepEqual(calls[1], ['xreadgroup', 'GROUP', 'g', 'c', 'STREAMS', 's', '>'])
    assert.deepEqual(calls[2], ['xreadgroup', 'GROUP', 'g', 'c', 'COUNT', 10, 'STREAMS', 's', '>'])
    assert.equal(calls.some(([command]) => command === '<disconnect>'), false, 'no dedicated connection is needed')
  })

  test('xread sends COUNT on the shared connection when no block is given', async () => {
    const { redis, calls } = createClient()

    await redis.xread({ count: 7 }, ['s', '0-0'])

    assert.deepEqual(calls[0], ['xread', 'COUNT', 7, 'STREAMS', 's', '0-0'])
  })

  test('xgroup covers every subcommand arity', async () => {
    const { redis, calls } = createClient()

    await redis.xgroup('CREATECONSUMER', 'st', 'g', 'consumer-1')
    await redis.xgroup('SETID', 'st', 'g')
    await redis.xgroup('destroy', 'st', 'g')

    assert.deepEqual(calls[0], ['xgroup', 'CREATECONSUMER', 'st', 'g', 'consumer-1'])
    assert.deepEqual(calls[1], ['xgroup', 'SETID', 'st', 'g', '$'], 'SETID defaults to the last id')
    assert.deepEqual(calls[2], ['xgroup', 'DESTROY', 'st', 'g'], 'the subcommand is normalized to upper case')
  })

  test('xtrim only marks the threshold as approximate when asked', async () => {
    const { redis, calls } = createClient()

    await redis.xtrim('st', 'MAXLEN', false, 1000)
    await redis.xtrim('st', 'MAXLEN', undefined, 1000)
    await redis.xtrim('st', 'MAXLEN', true, 1000)

    assert.deepEqual(calls[0], ['xtrim', 'st', 'MAXLEN', 1000])
    assert.deepEqual(calls[1], ['xtrim', 'st', 'MAXLEN', 1000], 'exact trimming is the default')
    assert.deepEqual(calls[2], ['xtrim', 'st', 'MAXLEN', '~', 1000])
  })

  // XPENDING answers two different questions in two different shapes: the
  // group summary without a range, the pending entries with one. Dropping a
  // partial range used to answer the wrong question silently — the caller
  // asked for entries and got [total, minId, maxId, consumers].
  test('xpending rejects a partial range instead of silently summarizing', async () => {
    const { redis, calls } = createClient()

    const partials = [
      { start: '-' },
      { start: '-', count: 5 },
      { end: '+', count: 5 },
      { start: '-', end: '+' },
      { consumer: 'c1' }
    ]

    for (const options of partials) {
      await assert.rejects(redis.xpending('st', 'g', options), {
        code: 'INVALID_ARGUMENT',
        operation: 'xpending'
      }, `${JSON.stringify(options)} must not reach the wire`)
    }

    assert.deepEqual(calls, [], 'no partial range may reach the server')

    // The summary form is still the one you get by asking for nothing.
    await redis.xpending('st', 'g')

    assert.deepEqual(calls, [['xpending', 'st', 'g']])
  })

  test('getAllStream scans everything by default', async () => {
    const { redis, fake } = createClient()
    const patterns = []

    fake.scanStream = (options) => {
      patterns.push(options.match)
      return {
        on (event, handler) { if (event === 'end') queueMicrotask(handler); return this },
        pause () {},
        resume () {},
        destroy () {}
      }
    }

    await redis.getAllStream()

    assert.deepEqual(patterns, ['*'])
  })

  test('blocking stream reads run on a dedicated connection', async () => {
    const { redis, calls } = createClient()

    await redis.xread({ block: 0 }, ['s', '$'])

    assert.deepEqual(calls[0], ['xread', 'BLOCK', 0, 'STREAMS', 's', '$'])
  })

  // A consumer loop calls this on every iteration: a handshake per iteration
  // (a whole cluster pool per iteration, under cluster) is a cost nobody asked
  // for. The connection is pooled between reads and only closed on shutdown.
  test('consecutive blocking reads reuse the pooled connection', async () => {
    const { redis, calls } = createClient()

    await redis.xread({ block: 0 }, ['s', '$'])
    await redis.xread({ block: 0 }, ['s', '$'])
    await redis.xread({ block: 0 }, ['s', '$'])

    assert.equal(
      calls.filter((call) => call[0] === '<disconnect>').length,
      0,
      'no connection may be torn down between reads'
    )

    await redis.disconnect()

    assert.equal(
      calls.filter((call) => call[0] === '<disconnect>').length,
      1,
      'and the pooled connection must be released on shutdown'
    )
  })

  // A pooled connection can die between two reads — the server restarted, the
  // socket dropped. Handing it to the next read would fail a command that had
  // nothing wrong with it.
  test('a pooled connection that died is discarded, not handed out again', async () => {
    const { redis, fake, calls } = createClient()

    await redis.xread({ block: 0 }, ['s', '$'])

    // Whatever was pooled is no longer usable.
    fake.status = 'end'
    calls.length = 0

    await redis.xread({ block: 0 }, ['s', '$'])

    assert.deepEqual(calls[0], ['<disconnect>'], 'the dead connection is dropped before anything else')
    assert.deepEqual(calls[1], ['xread', 'BLOCK', 0, 'STREAMS', 's', '$'], 'and the read still runs')
  })

  // Review finding: disconnect() is an async sequence and quit() does not
  // flip the driver's status synchronously, so work arriving DURING the
  // teardown still passed assertReady and created connections AFTER their
  // reapers ran — sockets nobody cancels, a process that never exits.
  test('work arriving during disconnect() is refused, not leaked', async () => {
    const { redis, calls } = createClient()

    // Pin the teardown open so the window is observable.
    let releaseTeardown
    redis.connection.disconnect = () => new Promise((resolve) => { releaseTeardown = resolve })

    const closing = redis.disconnect()
    await new Promise((resolve) => setImmediate(resolve))

    calls.length = 0

    await assert.rejects(redis.xread({ block: 0 }, ['s', '$']), {
      code: 'REDIS_UNAVAILABLE',
      operation: 'xread'
    }, 'a blocking read in the window must be refused')

    await assert.rejects(redis.withDedicatedConnection(async () => {}), {
      code: 'REDIS_UNAVAILABLE'
    })

    await assert.rejects(redis.subscribe('news', () => {}), {
      code: 'REDIS_UNAVAILABLE',
      operation: 'subscribe'
    })

    assert.deepEqual(calls, [], 'nothing may reach the wire — and no connection may be created')

    releaseTeardown()
    await closing

    // The refusal is about the shutdown, not the client's future: a new
    // connect() lifts the gate.
    await redis.connect()
    await redis.xread({ block: 0 }, ['s', '$'])
  })

  // Review finding: pooled connections are duplicates of the CURRENT client;
  // when that cycle ends (quit or the driver giving up) they kept their own
  // infinite retry loops alive, invisible to the facade.
  test('the blocking pool is drained when the connection cycle ends', async () => {
    const { redis, calls } = createClient()

    await redis.xread({ block: 0 }, ['s', '$'])
    assert.equal(calls.filter((c) => c[0] === '<disconnect>').length, 0, 'one connection parked in the pool')

    redis.emit('end')

    assert.equal(
      calls.filter((c) => c[0] === '<disconnect>').length,
      1,
      "the cycle's end is the pool's end"
    )
  })

  // Regression: a blocking read parked on a dedicated connection used to
  // survive disconnect() — its promise never settled and its socket kept the
  // process alive, so graceful shutdown never finished.
  test('disconnect cancels commands still waiting on a dedicated connection', async () => {
    const { redis, calls } = createClient()
    let settled = 'pending'

    // A command that never answers on its own, like XREAD BLOCK 0.
    redis.client.xread = redis.client.blockForever

    const blocked = redis.xread({ block: 0 }, ['s', '$'])
      .then(() => { settled = 'resolved' })
      .catch((err) => { settled = `${err.code}:${err.operation}` })

    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(settled, 'pending', 'the read is waiting, as intended')

    await redis.disconnect()
    await blocked

    assert.equal(settled, 'REDIS_UNAVAILABLE:xread', 'shutdown must cancel it with a code the caller can branch on')
    assert.deepEqual(calls.at(-1), ['<disconnect>'], 'and the dedicated connection must be released')
  })

  test('a dedicated connection released by shutdown reports cancellation, not its own failure', async () => {
    const { redis } = createClient()

    const held = redis.withDedicatedConnection((client) => client.blockForever())
    await new Promise((resolve) => setImmediate(resolve))

    await redis.disconnect()

    await assert.rejects(held, {
      code: 'REDIS_UNAVAILABLE',
      operation: 'withDedicatedConnection'
    })
  })

  test('withDedicatedConnection releases the connection even when fn throws', async () => {
    const { redis, calls } = createClient()

    await assert.rejects(redis.withDedicatedConnection(async () => {
      throw new Error('boom')
    }), /boom/)

    assert.deepEqual(calls.at(-1), ['<disconnect>'])
  })

  test('unsupported and malformed calls name their operation', async () => {
    const { redis } = createClient()

    await assert.rejects(redis.watch('k'), { code: 'UNSUPPORTED_OPERATION', operation: 'watch' })
    await assert.rejects(redis.unwatch(), { code: 'UNSUPPORTED_OPERATION', operation: 'unwatch' })
    await assert.rejects(redis.xtrim('st', 'MAXLEN'), { code: 'INVALID_ARGUMENT', operation: 'xtrim' })
  })

  test('multi goes through the readiness gate', async () => {
    const { redis, calls } = createClient()

    await redis.multi()

    assert.deepEqual(calls[0], ['multi'])
  })

  test('locking sends SET NX PX and releases through the token-checked script', async () => {
    const { redis, calls } = createClient()

    const lock = await redis.acquireLock('job', { ttl: 1000 })

    assert.equal(calls[0][0], 'set')
    assert.equal(calls[0][1], 'lock:job')
    assert.equal(calls[0][3], 'PX')
    assert.equal(calls[0][4], 1000)
    assert.equal(calls[0][5], 'NX')
    assert.match(calls[0][2], /^[0-9a-f-]{36}$/, 'the lock token must be a random uuid')

    await lock.release()
    assert.deepEqual(calls.at(-1), ['releaseLock', 'lock:job', lock.token])

    await lock.extend(5000)
    assert.deepEqual(calls.at(-1), ['extendLock', 'lock:job', lock.token, 5000])
  })

  test('xautoclaim builds its clause and returns named fields', async () => {
    const { redis, calls, fake } = createClient()

    fake.xautoclaim = async (...args) => {
      calls.push(['xautoclaim', ...args])

      return ['5-0', [['1-1', ['f', 'v']]], ['2-2']]
    }

    const swept = await redis.xautoclaim('st', 'g', 'worker-2', 60000, '0-0', { count: 10 })

    assert.deepEqual(calls[0], ['xautoclaim', 'st', 'g', 'worker-2', 60000, '0-0', 'COUNT', 10])
    assert.deepEqual(swept, {
      cursor: '5-0',
      entries: [['1-1', ['f', 'v']]],
      deleted: ['2-2']
    }, 'the positional reply must not leak to the caller')

    await redis.xautoclaim('st', 'g', 'worker-2', 60000)
    assert.deepEqual(calls[1], ['xautoclaim', 'st', 'g', 'worker-2', 60000, '0-0'], 'the sweep starts at the beginning by default')

    await redis.xautoclaim('st', 'g', 'worker-2', 60000, '0-0', { justId: true })
    assert.deepEqual(calls[2].at(-1), 'JUSTID')
  })

  // Redis 6.2 answers XAUTOCLAIM with two elements; the deleted-ids list only
  // arrived in 7.0. The named fields must not turn into undefined there.
  test('xautoclaim tolerates the two-element reply of Redis 6.2', async () => {
    const { redis, fake } = createClient()

    fake.xautoclaim = async () => ['0-0', [['1-1', ['f', 'v']]]]

    assert.deepEqual(await redis.xautoclaim('st', 'g', 'worker-2', 60000), {
      cursor: '0-0',
      entries: [['1-1', ['f', 'v']]],
      deleted: []
    })

    fake.xautoclaim = async () => ['0-0']

    assert.deepEqual(await redis.xautoclaim('st', 'g', 'worker-2', 60000), {
      cursor: '0-0',
      entries: [],
      deleted: []
    })
  })

  // Mid-failover a cluster can report no masters at all while it refreshes
  // its slot map. Reading the flags off nothing must not crash the probe.
  test('keyspaceNotifications survives a cluster with no masters yet', async () => {
    const { redis, fake } = createClient()

    fake.nodes = () => []

    assert.equal(await redis.keyspaceNotifications(), '')
  })

  test('keyspace event subscriptions probe the server config first', async () => {
    const { redis, calls, fake } = createClient()

    fake.config = async (...args) => {
      calls.push(['config', ...args])

      return ['notify-keyspace-events', 'Ex']
    }

    await redis.subscribeToKeyEvents('expired', () => {})

    assert.deepEqual(calls[0], ['config', 'GET', 'notify-keyspace-events'])
    assert.deepEqual(calls[1], ['subscribe', '__keyevent@0__:expired'])
  })

  test('a silent keyspace channel is refused with the command that fixes it', async () => {
    const { redis, calls, fake } = createClient()

    fake.config = async () => ['notify-keyspace-events', '']

    await assert.rejects(redis.subscribeToKeyEvents('expired', () => {}), {
      code: 'KEYSPACE_NOTIFICATIONS_DISABLED',
      operation: 'subscribeToKeyEvents',
      message: /missing "Ex".*CONFIG SET notify-keyspace-events "Ex"/s
    })

    assert.equal(calls.some(([command]) => command === 'subscribe'), false, 'nothing may be subscribed to a channel that cannot speak')
  })

  // The library calls `logger.debug?.()` in eight places. The `?.` is not
  // decoration: pino, winston and bunyan all ship `debug`, but a hand-rolled
  // three-level logger is a perfectly ordinary thing to inject, and without
  // the guard every one of those call sites is a TypeError on the hot path.
  test('a logger without debug() is not a crash', async () => {
    const seen = []
    const threeLevels = {
      error: (message) => seen.push(['error', message]),
      warn: (message) => seen.push(['warn', message]),
      info: (message) => seen.push(['info', message])
      // no debug, on purpose
    }

    const { redis, fake } = createClient({ logger: threeLevels })

    fake.scanStream = () => ({
      on (event, handler) {
        if (event === 'data') queueMicrotask(() => handler(['a']))
        if (event === 'end') queueMicrotask(handler)
        return this
      },
      pause () {},
      resume () {},
      destroy () {}
    })

    // Paths that reach for debug: the scanner (twice), the lock, and the
    // readiness gate rejecting a command.
    await redis.getAllStream('*')
    await redis.deleteByPattern('*')
    await redis.acquireLock('l', { ttl: 1000 })

    // And the readiness gate, on a client that was never connected.
    const offline = new RedisClient({ logger: threeLevels })
    await assert.rejects(offline.get('k'), { code: 'REDIS_UNAVAILABLE' })

    assert.deepEqual(seen.filter(([level]) => level === 'error'), [], 'nothing crashed on the way')
  })

  // The class letters are load-bearing: each keyspace event only fires if the
  // server has its class enabled, so a wrong letter here either refuses a
  // subscription that would have worked or — worse — waves through one that
  // will never speak. Only 'expired' had a test; the map has thirteen entries.
  test('every keyspace event demands its own notify-keyspace-events class', async () => {
    const cases = [
      ['expired', 'x'], ['evicted', 'e'], ['set', '$'], ['del', 'g'],
      ['rename_from', 'g'], ['rename_to', 'g'], ['expire', 'g'], ['lpush', 'l'],
      ['rpush', 'l'], ['sadd', 's'], ['hset', 'h'], ['zadd', 'z'],
      ['xadd', 't'], ['new', 'n']
    ]

    for (const [event, required] of cases) {
      const { redis, fake } = createClient()

      // 'E' alone is never enough: the class has to be there too.
      fake.config = async () => ['notify-keyspace-events', 'E']

      await assert.rejects(redis.subscribeToKeyEvents(event, () => {}), {
        code: 'KEYSPACE_NOTIFICATIONS_DISABLED',
        message: new RegExp(`missing "${required.replace('$', '\\$')}"`)
      }, `'${event}' must demand class '${required}'`)

      // With its class present it goes through...
      fake.config = async () => ['notify-keyspace-events', `E${required}`]
      await redis.subscribeToKeyEvents(event, () => {})

      // ...and the 'A' alias satisfies it — UNLESS the class is one of the
      // two redis.conf deliberately leaves out of the alias ('n' new-key,
      // 'm' key-miss). Accepting "EA" for those hands the caller a
      // subscription that never speaks: the canonical "AKE" config would
      // pass the probe and deliver nothing.
      fake.config = async () => ['notify-keyspace-events', 'EA']

      if ('nm'.includes(required)) {
        await assert.rejects(redis.subscribeToKeyEvents(event, () => {}), {
          code: 'KEYSPACE_NOTIFICATIONS_DISABLED'
        }, `'${event}' must NOT be satisfied by the 'A' alias`)
      } else {
        await redis.subscribeToKeyEvents(event, () => {})
      }
    }
  })

  test('an unnamed event only needs the E flag', async () => {
    const { redis, fake, calls } = createClient()

    fake.config = async () => ['notify-keyspace-events', 'E']
    await redis.subscribeToKeyEvents('json.set', () => {})

    assert.deepEqual(calls.at(-1), ['subscribe', '__keyevent@0__:json.set'])
  })

  test('keyspaceNotifications reports the flags, and an empty string when there are none', async () => {
    const { redis, fake } = createClient()

    fake.config = async () => ['notify-keyspace-events', 'gxE']
    assert.equal(await redis.keyspaceNotifications(), 'gxE')

    // A server that answers the key with no value must not become undefined.
    fake.config = async () => ['notify-keyspace-events']
    assert.equal(await redis.keyspaceNotifications(), '')
  })

  // Review finding: the CONFIG probe had no deadline, and its fallback — the
  // warn-and-subscribe-anyway written for managed providers that restrict
  // CONFIG — is only reachable through a REJECTION. A provider that HANGS on
  // CONFIG parked subscribeToKeyEvents forever, defeating the fallback built
  // for exactly that class of provider.
  test('a hanging CONFIG probe times out into the fallback instead of hanging the caller', async () => {
    const clock = createManualClock()
    const warnings = []
    const { redis, calls, fake } = createClient({
      clock,
      logger: { ...quietLogger, warn: (message) => warnings.push(message) }
    })

    fake.config = () => new Promise(() => {})

    const subscribing = redis.subscribeToKeyEvents('expired', () => {})

    await clock.advance(2000)
    await subscribing

    assert.match(warnings.at(-1), /Could not read notify-keyspace-events.*did not answer within 2000ms/s)
    assert.deepEqual(calls.at(-1), ['subscribe', '__keyevent@0__:expired'], 'the subscription proceeds unverified')
  })

  // Each cluster node is configured on its own and emits only its own slots'
  // events, so asking one master and trusting the answer would let a single
  // misconfigured shard go silent behind a probe that passed.
  test('the keyspace probe asks every master, not just the first', async () => {
    const { redis, fake } = createClient()
    const asked = []

    const master = (port, flags) => ({
      options: { host: '127.0.0.1', port },
      config: async () => {
        asked.push(port)

        return ['notify-keyspace-events', flags]
      },
      duplicate: () => ({
        on () {},
        subscribe: async () => 1
      })
    })

    // The first two are fine; the last one would silently drop its shard.
    fake.nodes = () => [master(7001, 'Ex'), master(7002, 'Ex'), master(7003, '')]

    await assert.rejects(redis.subscribeToKeyEvents('expired', () => {}), {
      code: 'KEYSPACE_NOTIFICATIONS_DISABLED',
      message: /cluster node 127\.0\.0\.1:7003/
    }, 'the weakest node decides the verdict')

    assert.deepEqual(asked, [7001, 7002, 7003], 'every master must be asked')
  })

  test('an unreadable config downgrades to a warning instead of blocking', async () => {
    const { redis, calls, fake } = createClient()
    const warnings = []

    redis.logger = { ...quietLogger, warn: (message) => warnings.push(message) }
    fake.config = async () => { throw new Error('ERR unknown command CONFIG') }

    await redis.subscribeToKeyEvents('expired', () => {})

    assert.match(warnings.at(-1), /Could not read notify-keyspace-events/)
    assert.deepEqual(calls.at(-1), ['subscribe', '__keyevent@0__:expired'], 'managed providers block CONFIG; refusing would be worse')
  })

  test('subscribe and unsubscribe reach the subscriber connection', async () => {
    const { redis, calls } = createClient()

    await redis.subscribe('news')
    await redis.psubscribe('logs.*')
    await redis.unsubscribe('news')
    await redis.punsubscribe('logs.*')

    assert.deepEqual(calls, [
      ['subscribe', 'news'],
      ['psubscribe', 'logs.*'],
      ['unsubscribe', 'news'],
      ['punsubscribe', 'logs.*']
    ])
  })

  test('optional stream clauses are appended when present', async () => {
    const { redis, calls } = createClient()

    await redis.xrange('st', '-', '+', { count: 5 })
    await redis.xrevrange('st', '+', '-', { count: 5 })
    await redis.xpending('st', 'g', { start: '-', end: '+', count: 5, consumer: 'c1' })

    assert.deepEqual(calls[0], ['xrange', 'st', '-', '+', 'COUNT', 5])
    assert.deepEqual(calls[1], ['xrevrange', 'st', '+', '-', 'COUNT', 5])
    assert.deepEqual(calls[2], ['xpending', 'st', 'g', '-', '+', 5, 'c1'])
  })

  // The registry has its own suite; this proves the facade is actually wired
  // to it — including that the key count declared at registration reaches the
  // call, which is the whole reason keys and args are separate arrays.
  test('defineScript and runScript reach the registry', async () => {
    const { redis, fake } = createClient()
    const defined = []

    fake.defineCommand = (name, definition) => {
      defined.push({ name, ...definition })
      fake[name] = async (...argv) => ['ran', ...argv]
    }

    redis.defineScript('cas', { numberOfKeys: 1, lua: 'return 1' })
    assert.deepEqual(redis.scripts.names, ['cas'])

    assert.deepEqual(await redis.runScript('cas', ['k'], ['a']), ['ran', 'k', 'a'])
    assert.deepEqual(defined, [{ name: 'userScript_cas', numberOfKeys: 1, lua: 'return 1', readOnly: false }])

    await assert.rejects(redis.runScript('cas', ['k', 'extra']), {
      code: 'INVALID_ARGUMENT',
      operation: 'runScript'
    }, 'the declared key count is enforced through the facade too')

    assert.throws(() => redis.defineScript('bad', { numberOfKeys: 1 }), {
      code: 'INVALID_ARGUMENT',
      operation: 'defineScript'
    })

    // A keyless script called with neither array: both defaults have to hold
    // here, not only inside the registry.
    redis.defineScript('tick', { numberOfKeys: 0, lua: 'return 1' })

    assert.deepEqual(await redis.runScript('tick'), ['ran'])
  })

  test('withLock delegates to the lock manager', async () => {
    const { redis, calls } = createClient()

    const result = await redis.withLock('job', async () => 'critical section ran')

    assert.equal(result, 'critical section ran')
    assert.equal(calls[0][0], 'set', 'the lock must actually be acquired')
    assert.equal(calls.at(-1)[0], 'releaseLock')
  })

  test('driver failures are logged and rethrown, never swallowed', async () => {
    const { redis, fake } = createClient()
    const logged = []

    redis.logger = { ...quietLogger, error: (message) => logged.push(message) }
    fake.get = async () => { throw new Error('WRONGTYPE Operation against a key') }

    await assert.rejects(redis.get('k'), /WRONGTYPE/)
    assert.match(logged.at(-1), /Unexpected error in Redis 'get' operation/)
  })

  test('blocking command failures are logged and rethrown too', async () => {
    const { redis, fake } = createClient()
    const logged = []

    redis.logger = { ...quietLogger, error: (message) => logged.push(message) }
    fake.xread = async () => { throw new Error('stream gone') }

    await assert.rejects(redis.xread({ block: 100 }, ['s', '$']), /stream gone/)
    assert.match(logged.at(-1), /Unexpected error in Redis 'xread' operation/)
  })

  test('errors on a dedicated connection are logged at debug level', async () => {
    const { redis, fake } = createClient()
    const debugged = []
    const handlers = []

    redis.logger = { ...quietLogger, debug: (message) => debugged.push(message) }
    fake.on = (event, handler) => { if (event === 'error') handlers.push(handler); return fake }

    await redis.withDedicatedConnection(async () => 'ok')
    handlers[0](new Error('dedicated socket died'))

    assert.match(debugged.at(-1), /Dedicated connection error: dedicated socket died/)
  })

  // Regression: ioredis does not prefix the key of XGROUP/XINFO (it sits
  // after the subcommand), so a prefixed client used to create consumer
  // groups on a different key than the one XADD wrote to.
  // Through ioredis 5 this facade prefixed XGROUP/XINFO by hand, because the
  // driver did not know their key sits after a subcommand. @ioredis/commands
  // 2.0.0 declares that position, so the driver prefixes them like any other
  // key — and doing it here too produced `app:app:events`, creating the group
  // on one key and reading from another. The keys now leave unprefixed, on
  // purpose, and the integration suite proves the round trip end to end.
  test('xgroup and xinfo leave prefixing to the driver, like every other key', async () => {
    const { redis, calls } = createClient({ keyPrefix: 'app:' })

    await redis.xgroup('CREATE', 'events', 'workers', '$', true)
    await redis.xgroup('DESTROY', 'events', 'workers')
    await redis.xinfo('STREAM', 'events')
    await redis.xinfo('CONSUMERS', 'events', 'workers')

    assert.deepEqual(calls[0], ['xgroup', 'CREATE', 'events', 'workers', '$', 'MKSTREAM'])
    assert.deepEqual(calls[1], ['xgroup', 'DESTROY', 'events', 'workers'])
    assert.deepEqual(calls[2], ['xinfo', 'STREAM', 'events'])
    assert.deepEqual(calls[3], ['xinfo', 'CONSUMERS', 'events', 'workers'])
  })

  test('getAllStream and deleteByPattern prefix the scan pattern', async () => {
    const { redis, fake } = createClient({ keyPrefix: 'app:' })
    const patterns = []

    fake.scanStream = (options) => {
      patterns.push(options.match)

      return {
        on (event, handler) {
          if (event === 'end') queueMicrotask(handler)
          return this
        },
        pause () {},
        resume () {},
        destroy () {}
      }
    }
    fake.unlink = async () => 0

    await redis.getAllStream('user:*')
    await redis.deleteByPattern('user:*')

    assert.deepEqual(patterns, ['app:user:*', 'app:user:*'])
  })
})

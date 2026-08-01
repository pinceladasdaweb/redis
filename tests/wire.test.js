// Wire contract: every public method must reach ioredis with the exact
// command and arguments it promises. The facade runs for real — only the
// driver is faked — so a typo in a command name, a swapped argument or a
// method that calls a collaborator that does not exist fails here.

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { RedisClient } from '../src/index.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

// Property names that must never be turned into recorded commands: awaiting
// or inspecting the fake would otherwise register bogus calls.
const NON_COMMANDS = new Set(['then', 'catch', 'finally', 'constructor', 'inspect'])

const createRecorder = () => {
  const calls = []

  const target = {
    status: 'ready',
    // Custom commands do not exist until defineCommand creates them — the
    // fake must reproduce that, or the lock manager's registration is skipped.
    releaseLock: undefined,
    extendLock: undefined,
    on () { return target },
    once () { return target },
    removeAllListeners () { return target },
    disconnect () { calls.push(['<disconnect>']) },
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

  redis.connection = connection
  redis.locks.connection = connection
  redis.subscriptions.connection = connection
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
  ['xadd', ['st', '*', 'f', 'v'], ['xadd', 'st', '*', 'f', 'v']],
  ['xlen', ['st'], ['xlen', 'st']],
  ['xinfo', ['STREAM', 'st'], ['xinfo', 'STREAM', 'st']],
  ['xdel', ['st', '1-1', '1-2'], ['xdel', 'st', '1-1', '1-2']],
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
      'xread', 'xreadgroup', 'xgroup', 'xtrim', 'publishJson',
      'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe', 'acquireLock', 'withLock'
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

  test('xpending ignores incomplete ranges instead of sending holes', async () => {
    const { redis, calls } = createClient()

    await redis.xpending('st', 'g', { start: '-' })
    await redis.xpending('st', 'g', { start: '-', count: 5 })
    await redis.xpending('st', 'g', { end: '+', count: 5 })
    await redis.xpending('st', 'g', { start: '-', end: '+' })
    await redis.xpending('st', 'g', { consumer: 'c1' })

    for (const call of calls) {
      assert.deepEqual(call, ['xpending', 'st', 'g'], 'a partial range must be dropped entirely')
    }
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

  test('blocking stream reads run on a dedicated connection and release it', async () => {
    const { redis, calls } = createClient()

    await redis.xread({ block: 0 }, ['s', '$'])

    assert.deepEqual(calls[0], ['xread', 'BLOCK', 0, 'STREAMS', 's', '$'])
    assert.deepEqual(calls[1], ['<disconnect>'], 'the dedicated connection must be released')
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

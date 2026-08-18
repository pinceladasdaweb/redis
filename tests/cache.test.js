// Cache-aside logic with a fake keyspace: hit/miss, validation, and the
// stampede lock including its fallback path.

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { RedisClient } from '../src/index.js'
import RedisClientError from '../src/utils/errors.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

const createClient = ({ store = new Map(), lockBehavior = 'grant' } = {}) => {
  const writes = []
  const lockCalls = []

  const fake = {
    status: 'ready',
    async get (key) { return store.has(key) ? store.get(key) : null },
    async setex (key, ttl, value) {
      writes.push([key, ttl, value])
      store.set(key, value)
      return 'OK'
    }
  }

  const redis = new RedisClient({ logger: quietLogger })
  const connection = { client: fake, isConnected: true, assertReady: () => fake }

  redis.connection = connection
  redis.subscriptions.connection = connection
  redis.health.getClient = () => fake

  redis.locks = {
    connection,
    async withLock (name, options, fn) {
      lockCalls.push([name, options])

      if (lockBehavior === 'denied') {
        // Faithful to LockManager: the failure names its lock, and the
        // facade's fallback only fires for the cache's own.
        const failure = new RedisClientError(`Could not acquire lock '${name}'.`, 'acquireLock', 'LOCK_NOT_ACQUIRED')
        failure.lockName = name

        throw failure
      }

      if (lockBehavior === 'broken') {
        throw new RedisClientError('Redis is not connected.', 'acquireLock', 'REDIS_UNAVAILABLE')
      }

      return fn({ name, token: 'fake-token' })
    }
  }

  return { redis, store, writes, lockCalls, fake }
}

describe('cache-aside', () => {
  test('serves a hit without running the producer', async () => {
    const { redis, writes } = createClient({ store: new Map([['k', '{"cached":true}']]) })
    let calls = 0

    const value = await redis.getOrSetJson('k', 60, () => { calls++; return { fresh: true } })

    assert.deepEqual(value, { cached: true })
    assert.equal(calls, 0)
    assert.deepEqual(writes, [], 'a hit must never write')
  })

  test('produces, stores with the ttl and returns the value on a miss', async () => {
    const { redis, writes } = createClient()

    const value = await redis.getOrSetJson('k', 90, () => ({ produced: true }))

    assert.deepEqual(value, { produced: true })
    assert.deepEqual(writes, [['k', 90, '{"produced":true}']])
  })

  test('raw getOrSet stores strings and numbers as strings', async () => {
    const { redis, writes } = createClient()

    assert.equal(await redis.getOrSet('s', 30, () => 'text'), 'text')
    assert.equal(await redis.getOrSet('n', 30, () => 42), '42')
    assert.deepEqual(writes, [['s', 30, 'text'], ['n', 30, '42']])
  })

  test('rejects an invalid ttl before touching the connection', async () => {
    const { redis, writes } = createClient()

    for (const ttl of [0, -1, 1.5, '60', null, undefined, NaN]) {
      await assert.rejects(redis.getOrSetJson('k', ttl, () => ({})), { code: 'INVALID_ARGUMENT' })
    }

    assert.deepEqual(writes, [])
  })

  test('rejects a producer that is not a function', async () => {
    const { redis } = createClient()

    await assert.rejects(redis.getOrSet('k', 60, 'nope'), { code: 'INVALID_ARGUMENT', operation: 'getOrSet' })
    await assert.rejects(redis.getOrSetJson('k', 60, null), { code: 'INVALID_ARGUMENT', operation: 'getOrSet' })
  })

  test('rejections name the operation that failed', async () => {
    const { redis } = createClient()

    await assert.rejects(redis.getOrSet('k', 0, () => 'x'), { operation: 'getOrSet' })
    await assert.rejects(redis.getOrSet('k', 60, () => ({})), { operation: 'getOrSet' })
    await assert.rejects(redis.getOrSetJson('k', 60, () => undefined), { operation: 'getOrSetJson' })
    await assert.rejects(redis.deleteByPattern(''), { operation: 'deleteByPattern' })
  })

  test('never caches a value it cannot represent', async () => {
    const { redis, writes, store } = createClient()

    await assert.rejects(redis.getOrSetJson('k', 60, () => undefined), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(redis.getOrSetJson('k', 60, () => () => {}), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(redis.getOrSetJson('k', 60, () => Symbol('x')), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(redis.getOrSet('k', 60, () => ({ obj: true })), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(redis.getOrSet('k', 60, () => null), { code: 'INVALID_ARGUMENT' })

    assert.deepEqual(writes, [])
    assert.equal(store.size, 0, 'a poisoned key must never reach the server')
  })

  test('a rejecting producer propagates and caches nothing', async () => {
    const { redis, writes } = createClient()

    await assert.rejects(redis.getOrSetJson('k', 60, async () => { throw new Error('upstream down') }), /upstream down/)
    assert.deepEqual(writes, [])
  })

  test('the lock path namespaces the key and auto-extends by default', async () => {
    const { redis, lockCalls } = createClient()

    await redis.getOrSetJson('report', 60, () => ({ ok: true }), { lock: true })

    const [name, options] = lockCalls[0]
    assert.equal(name, 'cache:report')
    assert.equal(options.autoExtend, true, 'a slow producer must not reopen the stampede')
    assert.ok(options.retries > 0 && options.retryDelay > 0)
  })

  test('lock options from the caller override the defaults', async () => {
    const { redis, lockCalls } = createClient()

    await redis.getOrSetJson('report', 60, () => ({ ok: true }), { lock: { ttl: 999, retries: 3 } })

    assert.equal(lockCalls[0][1].ttl, 999)
    assert.equal(lockCalls[0][1].retries, 3)
  })

  test('the lock winner re-checks the cache before producing', async () => {
    const store = new Map()
    const { redis } = createClient({ store })
    let calls = 0

    // Someone else fills the cache while we are waiting for the lock.
    const original = redis.locks.withLock
    redis.locks.withLock = async (name, options, fn) => {
      store.set('k', '{"filledByWinner":true}')
      return original.call(redis.locks, name, options, fn)
    }

    const value = await redis.getOrSetJson('k', 60, () => { calls++; return { mine: true } }, { lock: true })

    assert.deepEqual(value, { filledByWinner: true })
    assert.equal(calls, 0, 'the double-check must prevent a redundant producer run')
  })

  test('the lock winner produces when the cache is still empty', async () => {
    const { redis, writes, lockCalls } = createClient()
    let calls = 0

    const value = await redis.getOrSetJson('k', 60, () => { calls++; return { produced: true } }, { lock: true })

    assert.equal(lockCalls.length, 1, 'the lock path must have been taken')
    assert.equal(calls, 1, 'an empty cache inside the lock must still produce')
    assert.deepEqual(value, { produced: true })
    assert.deepEqual(writes, [['k', 60, '{"produced":true}']])
  })

  // Review finding: the fallback used to match on code alone, and a producer
  // that uses locks internally surfaces the same LOCK_NOT_ACQUIRED — the
  // facade misread it as its own cache lock failing, released the real cache
  // lock, and reran the producer UNPROTECTED under the very contention that
  // made the inner lock fail.
  test("a producer's own lock failure propagates instead of rerunning the producer", async () => {
    const { redis } = createClient()
    let calls = 0

    const innerFailure = new RedisClientError("Could not acquire lock 'render:report'.", 'acquireLock', 'LOCK_NOT_ACQUIRED')
    innerFailure.lockName = 'render:report'

    await assert.rejects(
      redis.getOrSetJson('report', 60, async () => {
        calls++
        throw innerFailure
      }, { lock: true }),
      (err) => err.lockName === 'render:report',
      'the inner failure must reach the caller untranslated'
    )

    assert.equal(calls, 1, 'the producer must never be rerun on a foreign lock failure')
  })

  test('an exhausted lock budget re-reads what the winner cached', async () => {
    const store = new Map()
    const { redis } = createClient({ store, lockBehavior: 'denied' })
    let calls = 0

    // The winner fills the cache while this caller burns its retry budget:
    // the first read missed, so only the fallback re-read can find it.
    const denied = redis.locks.withLock
    redis.locks.withLock = async (...args) => {
      store.set('k', '{"fromWinner":true}')
      return denied.apply(redis.locks, args)
    }

    const value = await redis.getOrSetJson('k', 60, () => { calls++; return { mine: true } }, { lock: true })

    assert.deepEqual(value, { fromWinner: true }, 'the waiter must reuse the winner value')
    assert.equal(calls, 0, 'producing again would defeat the whole point of the lock')
  })

  test('an exhausted lock budget still produces when nothing was cached', async () => {
    const { redis } = createClient({ lockBehavior: 'denied' })
    let calls = 0

    const value = await redis.getOrSetJson('k', 60, () => { calls++; return { mine: true } }, { lock: true })

    assert.deepEqual(value, { mine: true }, 'availability beats perfect protection')
    assert.equal(calls, 1)
  })

  test('lock failures other than a lost race are not swallowed', async () => {
    const { redis } = createClient({ lockBehavior: 'broken' })

    await assert.rejects(redis.getOrSetJson('k', 60, () => ({}), { lock: true }), { code: 'REDIS_UNAVAILABLE' })
  })

  test('callers always receive the value in its cached form', async () => {
    const { redis } = createClient()

    // Dates serialize to strings: the producer's return value and the cached
    // read must not disagree between the winner and later readers.
    const produced = await redis.getOrSetJson('k', 60, () => ({ at: new Date('2026-01-01T00:00:00.000Z') }))
    const reread = await redis.getOrSetJson('k', 60, () => ({ at: 'unused' }))

    assert.deepEqual(produced, reread)
    assert.equal(produced.at, '2026-01-01T00:00:00.000Z')
  })
})

describe('facade delegation', () => {
  test('exposes the connection state through getters', async () => {
    const { redis, fake } = createClient()

    assert.equal(redis.client, fake)
    assert.equal(redis.isConnected, true)
  })

  test('connect, disconnect and checkHealth delegate to the collaborators', async () => {
    const { redis } = createClient()
    const seen = []

    redis.connection.connect = async () => { seen.push('connect'); return 'connected' }
    redis.connection.disconnect = async () => { seen.push('disconnect') }
    redis.subscriptions.close = async () => { seen.push('close-subscriptions') }
    redis.health.check = async () => { seen.push('health'); return true }

    await redis.connect()
    assert.equal(await redis.checkHealth(), true)
    await redis.disconnect()

    assert.deepEqual(seen, ['connect', 'health', 'close-subscriptions', 'disconnect'])
  })

  test('deleteByPattern demands an explicit pattern', async () => {
    const { redis } = createClient()

    for (const pattern of [undefined, '', null, 42]) {
      await assert.rejects(redis.deleteByPattern(pattern), { code: 'INVALID_ARGUMENT' })
    }
  })

  test('operations report which call was gated when the connection is down', async () => {
    const redis = new RedisClient({ logger: quietLogger })

    await assert.rejects(redis.deleteByPattern('*'), { operation: 'deleteByPattern', code: 'REDIS_UNAVAILABLE' })
    await assert.rejects(redis.multi(), { operation: 'multi', code: 'REDIS_UNAVAILABLE' })
    await assert.rejects(redis.withDedicatedConnection(async () => {}), {
      operation: 'withDedicatedConnection',
      code: 'REDIS_UNAVAILABLE'
    })
    await assert.rejects(redis.acquireLock('job'), { operation: 'acquireLock', code: 'REDIS_UNAVAILABLE' })
    await assert.rejects(redis.subscribe('news'), { operation: 'subscribe', code: 'REDIS_UNAVAILABLE' })
  })

  test('omitPrefix strips only the configured prefix', () => {
    const prefixed = new RedisClient({ keyPrefix: 'app:', logger: quietLogger })
    const plain = new RedisClient({ logger: quietLogger })

    assert.equal(prefixed.omitPrefix('app:user:1'), 'user:1')
    assert.equal(prefixed.omitPrefix('other:user:1'), 'other:user:1')
    assert.equal(plain.omitPrefix('app:user:1'), 'app:user:1')
  })

  test('logError distinguishes library errors from unexpected ones', () => {
    const messages = []
    const redis = new RedisClient({ logger: { ...quietLogger, error: (message) => messages.push(message) } })

    redis.logError(new RedisClientError('gate closed', 'set', 'REDIS_UNAVAILABLE'), 'set')
    redis.logError(new Error('boom'), 'get')

    assert.match(messages[0], /operation 'set' failed: gate closed/)
    assert.match(messages[1], /Unexpected error in Redis 'get' operation: boom/)
  })
})

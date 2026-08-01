import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import LockManager from '../src/resilience/lock.js'
import RedisClientError from '../src/utils/errors.js'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const createManager = ({ acquireReplies = ['OK'], releaseReply = 1, extendReply = 1 } = {}) => {
  const calls = []
  const logs = []
  let defineCommandCalls = 0

  const client = {
    status: 'ready',
    async set (...args) {
      calls.push(['set', ...args])

      return acquireReplies.length > 1 ? acquireReplies.shift() : acquireReplies[0]
    },
    defineCommand (name, definition) {
      defineCommandCalls++
      assert.equal(definition.numberOfKeys, 1, `${name} must declare exactly one key`)
      assert.match(definition.lua, /redis\.call\("get", KEYS\[1\]\) == ARGV\[1\]/, `${name} must compare the holder token`)

      client[name] = async (...args) => {
        calls.push([name, ...args])

        return name === 'releaseLock' ? releaseReply : extendReply
      }
    }
  }

  const manager = new LockManager({
    connection: { assertReady: () => client },
    logger: {
      info () {},
      debug () {},
      warn: (message) => logs.push(message),
      error: (message) => logs.push(message)
    }
  })

  return { manager, calls, logs, client, defineCommandCalls: () => defineCommandCalls }
}

describe('lock manager', () => {
  test('acquires with SET NX PX under a lock: namespace', async () => {
    const { manager, calls } = createManager()

    const lock = await manager.acquire('job', { ttl: 1500 })

    assert.equal(lock.name, 'job')
    assert.match(lock.token, /^[0-9a-f-]{36}$/)
    assert.deepEqual(calls[0], ['set', 'lock:job', lock.token, 'PX', 1500, 'NX'])
  })

  test('defaults to a 30s ttl', async () => {
    const { manager, calls } = createManager()

    await manager.acquire('job')

    assert.equal(calls[0][4], 30000)
  })

  test('registers the Lua scripts once per client', async () => {
    const { manager, defineCommandCalls } = createManager()

    const lock = await manager.acquire('job')
    await lock.release()
    await manager.acquire('other')

    assert.equal(defineCommandCalls(), 2, 'exactly one release and one extend definition')
  })

  test('rejects with LOCK_NOT_ACQUIRED when the lock is held', async () => {
    const { manager, calls } = createManager({ acquireReplies: [null] })

    await assert.rejects(manager.acquire('job'), {
      name: 'RedisClientError',
      code: 'LOCK_NOT_ACQUIRED',
      operation: 'acquireLock'
    })

    assert.equal(calls.length, 1, 'no retries by default')
  })

  test('retries the configured number of times before giving up', async () => {
    const { manager, calls } = createManager({ acquireReplies: [null] })

    await assert.rejects(manager.acquire('job', { retries: 3, retryDelay: 1 }), { code: 'LOCK_NOT_ACQUIRED' })

    assert.equal(calls.length, 4, 'the first attempt plus three retries')
  })

  test('stops retrying as soon as it wins the lock', async () => {
    const { manager, calls } = createManager({ acquireReplies: [null, null, 'OK', 'OK'] })

    const lock = await manager.acquire('job', { retries: 5, retryDelay: 1, retryJitter: 2 })

    assert.equal(lock.name, 'job')
    assert.equal(calls.length, 3)
  })

  test('release deletes the key only while the token still holds', async () => {
    const held = createManager({ releaseReply: 1 })
    const lock = await held.manager.acquire('job')

    assert.equal(await lock.release(), true)
    assert.deepEqual(held.calls.at(-1), ['releaseLock', 'lock:job', lock.token])

    const stale = createManager({ releaseReply: 0 })
    const staleLock = await stale.manager.acquire('job')

    assert.equal(await staleLock.release(), false)
    assert.match(stale.logs.at(-1), /was not held on release/)
  })

  test('a successful release is silent', async () => {
    const { manager, logs } = createManager({ releaseReply: 1 })

    const lock = await manager.acquire('job')
    await lock.release()

    assert.deepEqual(logs, [], 'only a lost lock deserves a warning')
  })

  test('waits exactly the configured delay between attempts', async () => {
    const { manager } = createManager({ acquireReplies: [null] })

    const started = Date.now()
    await assert.rejects(manager.acquire('job', { retries: 3, retryDelay: 30 }), { code: 'LOCK_NOT_ACQUIRED' })
    const elapsed = Date.now() - started

    assert.ok(elapsed >= 60, `three 30ms delays should take at least 60ms, took ${elapsed}ms`)
    assert.ok(elapsed < 250, `it must not sleep longer than configured, took ${elapsed}ms`)
  })

  test('failing to acquire names the operation', async () => {
    const { manager } = createManager({ acquireReplies: [null] })

    await assert.rejects(manager.acquire('job'), { operation: 'acquireLock', name: 'RedisClientError' })
  })

  test('release and extend gate on the connection too', async () => {
    let ready = true
    const manager = new LockManager({
      connection: {
        assertReady: (operation) => {
          if (!ready) throw new RedisClientError('down', operation, 'REDIS_UNAVAILABLE')

          return {
            status: 'ready',
            async set () { return 'OK' },
            defineCommand (name) { this[name] = async () => 1 }
          }
        }
      },
      logger: { info () {}, debug () {}, warn () {}, error () {} }
    })

    const lock = await manager.acquire('job')
    ready = false

    await assert.rejects(lock.release(), { operation: 'releaseLock', code: 'REDIS_UNAVAILABLE' })
    await assert.rejects(lock.extend(), { operation: 'extendLock', code: 'REDIS_UNAVAILABLE' })
  })

  test('extend resets the ttl and reports whether the lock survived', async () => {
    const held = createManager({ extendReply: 1 })
    const lock = await held.manager.acquire('job', { ttl: 2000 })

    assert.equal(await lock.extend(), true)
    assert.deepEqual(held.calls.at(-1), ['extendLock', 'lock:job', lock.token, 2000], 'extend() reuses the acquire ttl')

    assert.equal(await lock.extend(9000), true)
    assert.deepEqual(held.calls.at(-1), ['extendLock', 'lock:job', lock.token, 9000])

    const lost = createManager({ extendReply: 0 })
    const lostLock = await lost.manager.acquire('job')
    assert.equal(await lostLock.extend(), false)
  })

  test('withLock runs the critical section and always releases', async () => {
    const { manager, calls } = createManager()

    const result = await manager.withLock('job', async (lock) => {
      assert.match(lock.token, /^[0-9a-f-]{36}$/)
      return 'done'
    })

    assert.equal(result, 'done')
    assert.equal(calls.at(-1)[0], 'releaseLock')
  })

  test('withLock accepts the two-argument form', async () => {
    const { manager, calls } = createManager()

    assert.equal(await manager.withLock('job', async () => 'ok'), 'ok')
    assert.equal(calls.at(-1)[0], 'releaseLock')
  })

  test('withLock releases and rethrows when the critical section fails', async () => {
    const { manager, calls } = createManager()

    await assert.rejects(manager.withLock('job', async () => { throw new Error('section failed') }), /section failed/)

    assert.equal(calls.at(-1)[0], 'releaseLock', 'the lock must be released even on failure')
  })

  test('a failing release never masks the critical section result', async () => {
    const { manager, logs, client } = createManager()

    const lock = manager.withLock('job', async () => {
      client.releaseLock = async () => { throw new Error('release exploded') }
      return 'value survives'
    })

    assert.equal(await lock, 'value survives')
    assert.match(logs.at(-1), /Failed to release lock 'job'.*release exploded/)
  })

  test('autoExtend keeps extending while the section runs, then stops', async () => {
    const { manager, calls } = createManager()

    await manager.withLock('job', { ttl: 100, autoExtend: true }, () => sleep(260))

    const extendsDuringSection = calls.filter(([name]) => name === 'extendLock').length
    assert.ok(extendsDuringSection >= 2, `expected the watchdog to extend at least twice, got ${extendsDuringSection}`)

    await sleep(150)
    assert.equal(
      calls.filter(([name]) => name === 'extendLock').length,
      extendsDuringSection,
      'the watchdog must stop once the section ends'
    )
  })

  test('autoExtend gives up and warns when the lock is definitively lost', async () => {
    const { manager, calls, logs } = createManager({ extendReply: 0 })

    await manager.withLock('job', { ttl: 100, autoExtend: true }, () => sleep(260))

    assert.equal(calls.filter(([name]) => name === 'extendLock').length, 1, 'a lost lock stops the watchdog')
    assert.match(logs.find((message) => /could not be extended/.test(message)), /it was lost/)
  })

  test('the watchdog extends at half the ttl, not at the floor', async () => {
    const { manager, calls } = createManager()

    await manager.withLock('job', { ttl: 400, autoExtend: true }, () => sleep(500))

    const extensions = calls.filter(([name]) => name === 'extendLock').length
    assert.ok(
      extensions >= 1 && extensions <= 4,
      `a 400ms ttl means a ~200ms cadence (about 2 extensions), got ${extensions}`
    )
  })

  test('a transient extend failure is logged and the watchdog keeps trying', async () => {
    const { manager, logs, client } = createManager()
    let attempts = 0

    await manager.withLock('job', { ttl: 100, autoExtend: true }, async () => {
      client.extendLock = async () => {
        attempts++
        throw new Error('extend exploded')
      }

      await sleep(260)
    })

    assert.ok(attempts >= 2, `a throwing extend must not kill the watchdog (attempts: ${attempts})`)
    assert.match(logs.find((message) => /Failed to extend lock/.test(message)), /extend exploded/)
  })

  test('no watchdog runs unless autoExtend is requested', async () => {
    const { manager, calls } = createManager()

    await manager.withLock('job', { ttl: 100 }, () => sleep(260))

    assert.equal(calls.filter(([name]) => name === 'extendLock').length, 0)
  })
})

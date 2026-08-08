import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import LockManager from '../src/resilience/lock.js'
import RedisClientError from '../src/utils/errors.js'
import { createManualClock, flush } from './helpers/manual-clock.js'

const createManager = ({ acquireReplies = ['OK'], releaseReply = 1, extendReply = 1, clock = createManualClock() } = {}) => {
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
    clock,
    logger: {
      info () {},
      debug () {},
      warn: (message) => logs.push(message),
      error: (message) => logs.push(message)
    }
  })

  return { manager, calls, logs, client, clock, defineCommandCalls: () => defineCommandCalls }
}

// Drives the manual clock until a retrying acquire settles: no real time
// passes, and the recorded delays are what the assertions read.
const drive = async (promise, clock, step = 1000) => {
  const state = { settled: false }
  const outcome = promise.then((value) => ({ value }), (error) => ({ error }))

  outcome.then(() => { state.settled = true })

  for (let round = 0; round < 100 && !state.settled; round++) {
    await clock.advance(step)
  }

  return outcome
}

// A critical section the test opens and closes on demand.
const deferred = () => {
  const handle = {}
  handle.promise = new Promise((resolve) => { handle.resolve = resolve })

  return handle
}

const extensions = (calls) => calls.filter(([name]) => name === 'extendLock').length

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

  test('retries the configured number of times, waiting the configured delay', async () => {
    const { manager, calls, clock } = createManager({ acquireReplies: [null] })

    const outcome = await drive(manager.acquire('job', { retries: 3, retryDelay: 30 }), clock)

    assert.equal(outcome.error.code, 'LOCK_NOT_ACQUIRED')
    assert.equal(calls.length, 4, 'the first attempt plus three retries')
    assert.deepEqual(clock.delays(), [30, 30, 30], 'one configured wait between attempts, never after the last')
  })

  test('stops retrying — and stops waiting — as soon as it wins', async () => {
    const { manager, calls, clock } = createManager({ acquireReplies: [null, null, 'OK', 'OK'] })

    const outcome = await drive(manager.acquire('job', { retries: 5, retryDelay: 10 }), clock)

    assert.equal(outcome.value.name, 'job')
    assert.equal(calls.length, 3)
    assert.deepEqual(clock.delays(), [10, 10], 'the winning attempt must not be followed by a wait')
  })

  test('jitter spreads the waits so contenders stop retrying in lockstep', async () => {
    const { manager, clock } = createManager({ acquireReplies: [null] })

    await drive(manager.acquire('job', { retries: 20, retryDelay: 40, retryJitter: 20 }), clock)

    const delays = clock.delays()

    assert.equal(delays.length, 20)
    for (const delay of delays) {
      assert.ok(delay >= 40 && delay < 60, `each wait is the base plus 0..jitter, got ${delay}`)
    }
    assert.ok(new Set(delays).size > 1, 'jitter that never varies is not jitter')
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
      clock: createManualClock(),
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

  test('the watchdog first extends at exactly half the ttl', async () => {
    const { manager, calls, clock } = createManager()
    const job = deferred()

    const running = manager.withLock('job', { ttl: 400, autoExtend: true }, () => job.promise)
    await flush()

    await clock.advance(199)
    assert.equal(extensions(calls), 0, 'nothing is renewed before half the ttl')

    await clock.advance(1)
    assert.equal(extensions(calls), 1, 'and the first renewal lands exactly on it')

    job.resolve('done')
    assert.equal(await running, 'done')
  })

  test('the watchdog renews until the section ends, then leaves no timer behind', async () => {
    const { manager, calls, clock } = createManager()
    const job = deferred()

    const running = manager.withLock('job', { ttl: 1000, autoExtend: true }, () => job.promise)
    await flush()

    await clock.advance(500)
    await clock.advance(500)
    await clock.advance(500)
    assert.equal(extensions(calls), 3, 'one renewal per half-ttl while the section runs')

    job.resolve()
    await running

    await clock.advance(10000)
    assert.equal(extensions(calls), 3, 'the watchdog stops with the section')
    assert.equal(clock.pending(), 0, 'and no timer outlives withLock')
  })

  test('a lock lost mid-section stops the watchdog and warns', async () => {
    const { manager, calls, logs, clock } = createManager({ extendReply: 0 })
    const job = deferred()

    const running = manager.withLock('job', { ttl: 1000, autoExtend: true }, () => job.promise)
    await flush()

    await clock.advance(500)
    await clock.advance(500)

    assert.equal(extensions(calls), 1, 'a definitively lost lock is not renewed again')
    assert.match(logs.find((message) => /could not be extended/.test(message)), /it was lost/)

    job.resolve()
    await running
  })

  test('a transient extend failure is logged and the watchdog keeps trying', async () => {
    const { manager, client, logs, clock } = createManager()
    const job = deferred()
    let attempts = 0

    // Replaced inside the section: acquiring is what registers the scripts,
    // so an earlier override would just be overwritten.
    const running = manager.withLock('job', { ttl: 1000, autoExtend: true }, () => {
      client.extendLock = async () => {
        attempts++
        throw new Error('extend exploded')
      }

      return job.promise
    })
    await flush()

    await clock.advance(500)
    await clock.advance(500)

    assert.equal(attempts, 2, 'a throwing extend must not kill the watchdog')
    assert.match(logs.find((message) => /Failed to extend lock/.test(message)), /extend exploded/)

    job.resolve()
    await running
  })

  test('no watchdog runs unless autoExtend is requested', async () => {
    const { manager, calls, clock } = createManager()
    const job = deferred()

    const running = manager.withLock('job', { ttl: 1000 }, () => job.promise)
    await flush()

    await clock.advance(10000)
    assert.equal(extensions(calls), 0)
    assert.equal(clock.pending(), 0, 'nothing is scheduled at all')

    job.resolve()
    await running
  })
})

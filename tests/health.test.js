import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import HealthChecker from '../src/connection/health.js'
import { createManualClock } from './helpers/manual-clock.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

const createChecker = ({ client, clock = createManualClock(), ...options } = {}) => {
  const checker = new HealthChecker({ getClient: () => client, logger: quietLogger, clock, ...options })

  return Object.assign(checker, { clock })
}

// Faithful to ioredis: ping() returns a promise. `silent` models a wedged
// server — a promise that never settles, which is exactly the case the
// probe's timeout (and stop()) exist for.
const pingingClient = (reply, { fail = false, silent = false } = {}) => {
  const client = {
    status: 'ready',
    pings: 0,
    ping () {
      client.pings++

      if (silent) return new Promise(() => {})
      if (fail) return Promise.reject(new Error(reply))

      return Promise.resolve(reply)
    }
  }

  return client
}

describe('health checker', () => {
  test('reports unhealthy when there is no client', async () => {
    assert.equal(await createChecker().check(), false)
  })

  test('reports unhealthy when the client is not ready, without pinging', async () => {
    const client = pingingClient('PONG')
    client.status = 'connecting'

    assert.equal(await createChecker({ client }).check(), false)
    assert.equal(client.pings, 0, 'an unready client must never be pinged')
  })

  test('reports healthy only for a PONG reply', async () => {
    assert.equal(await createChecker({ client: pingingClient('PONG') }).check(), true)
    assert.equal(await createChecker({ client: pingingClient('LOADING') }).check(), false)
  })

  test('reports unhealthy and logs when the ping fails', async () => {
    const logged = []
    const checker = new HealthChecker({
      getClient: () => pingingClient('connection lost', { fail: true }),
      logger: { ...quietLogger, error: (message) => logged.push(message) },
      clock: createManualClock()
    })

    assert.equal(await checker.check(), false)
    assert.match(logged[0], /health check failed.*connection lost/)
  })

  test('waits exactly the configured timeout before giving up on a silent ping', async () => {
    const clock = createManualClock()
    const checker = createChecker({ client: pingingClient('PONG', { silent: true }), clock, timeout: 1000 })

    let settled = null
    const probe = checker.check().then((healthy) => { settled = healthy })

    await clock.advance(999)
    assert.equal(settled, null, 'one millisecond before the deadline it must still be waiting')

    await clock.advance(1)
    await probe
    assert.equal(settled, false, 'and exactly on the deadline it gives up')
  })

  test('a reply cancels the timeout instead of leaving it armed', async () => {
    const clock = createManualClock()
    const checker = createChecker({ client: pingingClient('PONG'), clock, timeout: 1000 })

    assert.equal(await checker.check(), true)
    assert.equal(clock.pending(), 0, 'no timer may outlive the reply')
  })

  test('shares one in-flight ping between concurrent callers', async () => {
    const client = pingingClient('PONG')
    const checker = createChecker({ client, interval: 5000 })

    const results = await Promise.all([checker.check(), checker.check(), checker.check()])

    assert.deepEqual(results, [true, true, true])
    assert.equal(client.pings, 1, 'concurrent checks must reuse a single PING')
  })

  // Regression: an unhealthy result used to be cached for a whole interval,
  // so the probe kept reporting "down" long after the connection recovered.
  test('never caches an unhealthy result', async () => {
    const client = pingingClient('PONG')
    client.status = 'connecting'

    const checker = createChecker({ client, interval: 60000 })

    assert.equal(await checker.check(), false)

    client.status = 'ready'
    assert.equal(await checker.check(), true, 'recovery must be visible immediately')
    assert.equal(client.pings, 1, 'the recovery check must be a real probe, not a cached answer')
  })

  test('never caches a failed ping either', async () => {
    let failing = true
    const client = {
      status: 'ready',
      pings: 0,
      ping () {
        client.pings++

        return failing ? Promise.reject(new Error('connection lost')) : Promise.resolve('PONG')
      }
    }
    const checker = createChecker({ client, interval: 60000 })

    assert.equal(await checker.check(), false)

    failing = false
    assert.equal(await checker.check(), true)
    assert.equal(client.pings, 2, 'a failed probe must be retried, not remembered')
  })

  // Regression: a cached "healthy" used to be served without looking at the
  // connection, so a readiness endpoint kept sending traffic to a client that
  // had already dropped.
  test('never serves a cached healthy result for a dropped connection', async () => {
    const client = pingingClient('PONG')
    const checker = createChecker({ client, interval: 60000 })

    assert.equal(await checker.check(), true)

    client.status = 'end'
    assert.equal(await checker.check(), false, 'the cache must not outlive the connection')
    assert.equal(client.pings, 1, 'and noticing costs no extra round-trip')
  })

  test('caches a healthy result for the configured interval', async () => {
    const client = pingingClient('PONG')
    const checker = createChecker({ client, interval: 60000 })

    assert.equal(await checker.check(), true)
    assert.equal(await checker.check(), true)
    assert.equal(await checker.check(), true)

    assert.equal(client.pings, 1, 'a healthy connection must not be re-pinged within the interval')
  })

  test('re-pings exactly when the interval expires, not a millisecond earlier', async () => {
    const clock = createManualClock()
    const client = pingingClient('PONG')
    const checker = createChecker({ client, clock, interval: 5000 })

    await checker.check()
    assert.equal(client.pings, 1)

    // The cache is still valid right up to the boundary...
    clock.jump(4999)
    await checker.check()
    assert.equal(client.pings, 1, 'inside the interval the cached result is reused')

    // ...and expires on it.
    clock.jump(1)
    await checker.check()
    assert.equal(client.pings, 2, 'the interval must expire at exactly interval ms')
  })

  test('never mutates connection state — it only observes', async () => {
    const client = pingingClient('PONG')
    const checker = createChecker({ client })
    const before = { ...client }

    await checker.check()

    assert.equal(client.status, before.status)
    assert.equal('isConnected' in client, false)
  })
})

// Fourth full-source review (22/08/2026).
describe('health checker — review findings', () => {
  // A PING that answered late — after its own timeout had already settled the
  // probe — nulled #cancelInFlight unconditionally, wiping the canceller a
  // NEWER probe had just installed. stop() then did nothing and that probe's
  // deliberately ref'd timer outlived disconnect(). The fake never modeled a
  // reply that arrives after the timeout; now it does.
  test('a late reply from a timed-out probe cannot disarm the next probe\'s stop()', async () => {
    const clock = createManualClock()
    const replies = []
    const client = {
      status: 'ready',
      ping: () => new Promise((resolve) => { replies.push(resolve) })
    }
    const checker = createChecker({ client, clock, timeout: 1000, interval: 0 })

    // Probe 1 times out; its PING is still pending.
    const first = checker.check()
    await clock.advance(1000)
    assert.equal(await first, false)

    // Probe 2 is in flight with its own timer armed.
    const second = checker.check()
    await flushMicrotasks()
    assert.equal(clock.pending(), 1, 'probe 2 holds a ref\'d timer')

    // Probe 1's PING finally answers.
    replies[0]('PONG')
    await flushMicrotasks()

    // stop() must still cancel probe 2.
    checker.stop()
    assert.equal(await second, false, 'probe 2 was cancelled, not left to its timer')
    assert.equal(clock.pending(), 0, 'and its timer is gone — nothing outlives disconnect()')
  })
})

const flushMicrotasks = () => new Promise((resolve) => setImmediate(resolve))

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import HealthChecker from '../src/connection/health.js'
import { createManualClock } from './helpers/manual-clock.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

const createChecker = ({ client, clock = createManualClock(), ...options } = {}) => {
  const checker = new HealthChecker({ getClient: () => client, logger: quietLogger, clock, ...options })

  return Object.assign(checker, { clock })
}

const pingingClient = (reply, { fail = false, silent = false } = {}) => {
  const client = {
    status: 'ready',
    pings: 0,
    ping (callback) {
      client.pings++

      if (silent) return
      if (fail) return callback(new Error(reply))

      callback(null, reply)
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
      ping (callback) {
        client.pings++

        return failing ? callback(new Error('connection lost')) : callback(null, 'PONG')
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

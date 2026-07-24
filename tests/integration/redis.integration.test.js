// Integration suite against a REAL Redis (docker compose up -d).
// Gated by REDIS_INTEGRATION=1 so plain `npm test` never requires docker.
//
// The reconnection tests kill connections server-side via CLIENT KILL —
// the Redis equivalent of RabbitMQ's DELETE /api/connections — so they
// exercise the exact failure a production outage produces. They are
// destructive by design: never run them in parallel with anything else
// against the same Redis instance.

import { after, before, describe, test } from 'node:test'
import assert from 'node:assert/strict'
import Redis from 'ioredis'
import { RedisClient } from '../../src/index.js'

const RUN = process.env.REDIS_INTEGRATION === '1'
const HOST = process.env.REDIS_HOST || '127.0.0.1'
const PORT = Number(process.env.REDIS_PORT || '6379')
const ADMIN_NAME = 'integration-admin'

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const waitFor = async (predicate, { timeout = 15000, interval = 250, message = 'condition' } = {}) => {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(interval)
  }
  throw new Error(`Timed out after ${timeout}ms waiting for: ${message}`)
}

// Logger stub: integration output stays readable and no lib logging
// side effects leak into the assertions.
const quietLogger = { info () {}, warn () {}, error () {}, debug () {} }

describe('redis client integration', { skip: !RUN && 'set REDIS_INTEGRATION=1 (requires a real Redis, e.g. docker compose up -d)' }, () => {
  let admin

  const listOtherClientIds = async () => {
    const adminId = Number(await admin.client('ID'))
    const list = await admin.client('LIST')

    return list
      .split('\n')
      .filter(Boolean)
      .map(line => Number(/(?:^|\s)id=(\d+)/.exec(line)[1]))
      .filter(id => id !== adminId)
  }

  const killAllOtherClients = async () => {
    const ids = await listOtherClientIds()

    for (const id of ids) {
      try {
        await admin.client('KILL', 'ID', String(id))
      } catch {
        // the client may already be gone; the sweep is best-effort
      }
    }

    return ids.length
  }

  before(async () => {
    admin = new Redis({ host: HOST, port: PORT, connectionName: ADMIN_NAME })
    await admin.ping()
    await killAllOtherClients()
  })

  after(async () => {
    await admin.quit()
  })

  // Regression (AUDIT C5): commands while disconnected used to resolve null —
  // a silent no-op for writes. They must fail fast with a structured error.
  test('rejects commands with REDIS_UNAVAILABLE before connect()', async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })

    await assert.rejects(client.set('it:unavailable', 'x'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE',
      operation: 'set'
    })
  })

  test('rejects commands with REDIS_UNAVAILABLE while the server is unreachable', { timeout: 15000 }, async () => {
    // Nothing listens on this port: connect() resolves and retries run in
    // the background, but commands must fail fast instead of queueing.
    const client = new RedisClient({
      host: HOST,
      port: 1,
      baseRetryDelay: 50,
      maxRetryAttempts: 1,
      logger: quietLogger
    })
    await client.connect()

    await assert.rejects(client.set('it:unreachable', 'x'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE'
    })

    await client.disconnect()
  })

  test('performs a basic write/read/delete roundtrip', async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:roundtrip', 'value'), 'OK')
    assert.equal(await client.get('it:roundtrip'), 'value')

    await client.setJson('it:json', { nested: { ok: true } })
    assert.deepEqual(await client.getJson('it:json'), { nested: { ok: true } })

    assert.equal(await client.del('it:roundtrip', 'it:json'), 2)

    await client.disconnect()
  })

  // Regression (AUDIT C7): scanStream does not apply keyPrefix to MATCH, so
  // getAllStream used to scan the whole database (missing every prefixed key
  // and leaking foreign ones) and rejected everything on the first
  // non-string key.
  test('getAllStream scans only the prefixed keyspace and skips non-strings', { timeout: 30000 }, async () => {
    await admin.flushdb()

    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'app:', logger: quietLogger })
    await client.connect()

    await client.set('user:1', 'alice')
    await client.set('user:2', 'bob')
    await client.set('config:x', 'y')
    await client.hset('user:hash', 'field', 'value') // non-string: skipped
    await admin.set('foreign:user:1', 'intruder') // outside the prefix

    const users = Object.assign({}, ...await client.getAllStream('user:*'))
    assert.deepEqual(users, { 'user:1': 'alice', 'user:2': 'bob' })

    const everything = Object.assign({}, ...await client.getAllStream())
    assert.deepEqual(everything, { 'user:1': 'alice', 'user:2': 'bob', 'config:x': 'y' })

    await client.disconnect()
  })

  // The sacred test (playbook §4): drop the connection FOR REAL, from the
  // server side, and prove the client fully recovers.
  test('recovers after all connections are killed server-side (CLIENT KILL)', { timeout: 60000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:sacred:before', 'ok'), 'OK')

    // Admin APIs list with lag: make sure the library connection is visible
    // before sweeping, or the kill silently misses it (real flake we had).
    await waitFor(async () => (await listOtherClientIds()).length >= 1, {
      message: 'library connection to appear in CLIENT LIST'
    })

    const killed = await killAllOtherClients()
    assert.ok(killed >= 1, 'expected to kill at least the library connection')

    await waitFor(async () => {
      try {
        return (await client.set('it:sacred:after', 'ok')) === 'OK' &&
          (await client.get('it:sacred:after')) === 'ok'
      } catch {
        return false
      }
    }, { timeout: 20000, message: 'write+read to succeed again after the server-side kill' })

    await client.disconnect()
  })

  // Regression probe for AUDIT C1/C2: the manual reconnection layer spawns a
  // brand-new ioredis client per attempt while the driver's own retryStrategy
  // also reconnects the old one — every recovery must end with the SAME
  // number of connections it started with.
  test('does not leak extra connections while recovering', { timeout: 60000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:leak:probe', '1'), 'OK')
    await waitFor(async () => (await listOtherClientIds()).length >= 1, {
      message: 'library connection to appear in CLIENT LIST'
    })
    const baseline = (await listOtherClientIds()).length

    await killAllOtherClients()

    await waitFor(async () => {
      try {
        return (await client.set('it:leak:probe', '2')) === 'OK'
      } catch {
        return false
      }
    }, { timeout: 20000, message: 'a write to succeed after the kill' })

    // Give competing reconnection loops time to surface extra connections.
    await sleep(3000)

    const settled = (await listOtherClientIds()).length
    assert.equal(
      settled,
      baseline,
      `expected ${baseline} connection(s) after recovery, server sees ${settled} (leaked reconnection loop)`
    )

    await client.disconnect()
  })

  // Regression probe for AUDIT C4: quit() emits 'close', and the close
  // handler used to schedule a reconnection — disconnect() must be final.
  test('disconnect() stays disconnected (no self-resurrection)', { timeout: 30000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:cycle:probe', '1'), 'OK')
    await client.disconnect()

    // Long enough for any wrongly-scheduled reconnection to have fired.
    await sleep(2000)

    assert.equal(client.client, null, 'internal client must remain null after disconnect()')
    assert.equal(client.isConnected, false, 'isConnected must remain false after disconnect()')

    const remaining = (await listOtherClientIds()).length
    assert.equal(remaining, 0, `server still sees ${remaining} connection(s) after disconnect()`)

    await assert.rejects(client.set('it:cycle:probe', '2'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE'
    })
  })
})

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
const RUN_CLUSTER = process.env.REDIS_CLUSTER_INTEGRATION === '1'
const CLUSTER_NODES = (process.env.REDIS_CLUSTER_NODES || '127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003')
  .split(',')
  .map((node) => {
    const [host, port] = node.split(':')

    return { host, port: Number(port) }
  })
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

  // Counting every connection on the server couples each test to the cleanup
  // of every other one: a single leak elsewhere cascades into unrelated
  // failures. Tests that care about their own footprint name their client and
  // count only that name — the same lesson as filtering the RabbitMQ
  // management API by connection name.
  const listClientIdsNamed = async (name) => {
    const list = await admin.client('LIST')

    return list
      .split('\n')
      .filter(Boolean)
      .filter(line => new RegExp(`(?:^|\\s)name=${name}(?:\\s|$)`).test(line))
      .map(line => Number(/(?:^|\s)id=(\d+)/.exec(line)[1]))
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

  // Regression (AUDIT B2): xgroup DESTROY used to send a stray '$'.
  test('xgroup CREATE and DESTROY roundtrip against the server', async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    assert.equal(await client.xgroup('CREATE', 'it:stream', 'it-group', '$', true), 'OK')
    assert.equal(await client.xgroup('DESTROY', 'it:stream', 'it-group'), 1)

    await client.del('it:stream')
    await client.disconnect()
  })

  test('sorted sets keep scores numeric through a real round-trip', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'zset:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    await client.zadd('board', { ada: 120, alan: 95, grace: 180 })
    assert.equal(await client.zcard('board'), 3)

    assert.equal(await client.zincrby('board', 45, 'ada'), 165, 'increments return a number')
    assert.equal(await client.zscore('board', 'alan'), 95)
    assert.equal(await client.zscore('board', 'nobody'), null)
    assert.equal(await client.zrevrank('board', 'grace'), 0)

    assert.deepEqual(await client.zrevrange('board', 0, 1, { withScores: true }), [
      { member: 'grace', score: 180 },
      { member: 'ada', score: 165 }
    ])

    assert.deepEqual(await client.zrangebyscore('board', 100, '+inf'), ['ada', 'grace'])
    assert.equal(await client.zcount('board', 90, 130), 1)

    // The infinity round-trip: Number('inf') would be NaN.
    await client.zadd('board', { pinned: '+inf' })
    assert.equal(await client.zscore('board', 'pinned'), Number.POSITIVE_INFINITY)
    assert.equal(await client.zrem('board', 'pinned'), 1)

    assert.deepEqual(await client.zpopmin('board'), { member: 'alan', score: 95 })
    assert.deepEqual(await client.zpopmax('board', 2), [
      { member: 'grace', score: 180 },
      { member: 'ada', score: 165 }
    ])
    assert.equal(await client.zpopmin('board'), null, 'an empty set pops null')

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  // Regression: ioredis does not prefix the key of XGROUP/XINFO, so a
  // prefixed client used to create the consumer group on a different key than
  // the one XADD wrote to — every consumer-group flow was broken under a
  // keyPrefix.
  test('consumer groups work under a keyPrefix end to end', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'grp:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    await client.xadd('events', '*', 'type', 'signup')
    await client.xgroup('CREATE', 'events', 'workers', '0', true)

    // The group must live on the same (prefixed) key the entries went to.
    assert.equal(await admin.exists('grp:events'), 1)
    assert.equal(await admin.exists('events'), 0, 'nothing may be created outside the prefix')

    const groups = await client.xinfo('GROUPS', 'events')
    assert.equal(groups.length, 1, 'xinfo must look at the prefixed key')

    const delivered = await client.xreadgroup('workers', 'worker-1', { count: 10 }, ['events', '>'])
    assert.equal(delivered[0][1].length, 1, 'the group must see the entries')

    assert.equal(await client.xack('events', 'workers', delivered[0][1][0][0]), 1)
    assert.equal((await client.xpending('events', 'workers'))[0], 0)

    await client.xgroup('DESTROY', 'events', 'workers')
    await client.deleteByPattern('*')
    await client.disconnect()
  })

  // A consumer that dies holding deliveries leaves them pending forever;
  // XAUTOCLAIM is how another worker sweeps them up.
  test('xautoclaim recovers entries abandoned by a dead consumer', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'claim:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    await client.xadd('jobs', '*', 'task', 'one')
    await client.xadd('jobs', '*', 'task', 'two')
    await client.xgroup('CREATE', 'jobs', 'workers', '0', true)

    // worker-1 takes both and then "dies" without acknowledging.
    const taken = await client.xreadgroup('workers', 'worker-1', { count: 10 }, ['jobs', '>'])
    assert.equal(taken[0][1].length, 2)
    assert.equal((await client.xpending('jobs', 'workers'))[0], 2, 'both are stuck pending')

    // worker-2 sweeps anything idle, however briefly.
    const swept = await client.xautoclaim('jobs', 'workers', 'worker-2', 0)

    assert.equal(swept.entries.length, 2, 'the abandoned work is handed over')
    assert.equal(swept.cursor, '0-0', 'a full sweep ends back at the start')
    assert.deepEqual(swept.deleted, [])

    await client.xack('jobs', 'workers', ...swept.entries.map(([id]) => id))
    assert.equal((await client.xpending('jobs', 'workers'))[0], 0, 'and the group is settled again')

    await client.xgroup('DESTROY', 'jobs', 'workers')
    await client.deleteByPattern('*')
    await client.disconnect()
  })

  // Probed against a real Redis 7.4 before this test existed, because a fake
  // written by us would only have confirmed what we already believed. Raw
  // output, `delivery_count` per entry:
  //
  //   after XREADGROUP '>' (first delivery)          -> 1     (NOT 0)
  //   after XREADGROUP '0' (re-reading own PEL)      -> 2     (counts as a delivery!)
  //   after XCLAIM                                   -> 3
  //   after XCLAIM ... JUSTID                        -> unchanged
  //   after XAUTOCLAIM                               -> +1
  //   after XAUTOCLAIM ... JUSTID                    -> unchanged
  //   after a real server restart (SAVE + restart)   -> preserved
  //   idle after that same restart                   -> reset to ~0
  //
  // Two of those are traps. The recovery pattern every streams consumer runs
  // on startup — re-read my own pending with id '0' — BURNS retry budget, so
  // a crash loop exhausts it without a single new delivery. And after a
  // restart the two inputs to a retry policy disagree: the count survives,
  // the idle clock does not.
  test('delivery_count counts deliveries, and JUSTID is how you look without spending', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'dc:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    await client.xadd('jobs', '*', 'task', 'one')
    await client.xgroup('CREATE', 'jobs', 'workers', '0', true)

    const countOf = async () => {
      const [entry] = await client.xpending('jobs', 'workers', { start: '-', end: '+', count: 10 })

      return entry[3]
    }

    await client.xreadgroup('workers', 'worker-1', { count: 10 }, ['jobs', '>'])
    assert.equal(await countOf(), 1, 'the first delivery already counts as one')

    // The standard restart-recovery read, and it is not free.
    await client.xreadgroup('workers', 'worker-1', { count: 10 }, ['jobs', '0'])
    assert.equal(await countOf(), 2, 're-reading your own pending list spends budget')

    await client.xautoclaim('jobs', 'workers', 'worker-2', 0, '0-0', { justId: true })
    assert.equal(await countOf(), 2, 'JUSTID hands the entry over without spending')

    await client.xautoclaim('jobs', 'workers', 'worker-3', 0)
    assert.equal(await countOf(), 3, 'a normal claim does spend')

    await client.xgroup('DESTROY', 'jobs', 'workers')
    await client.deleteByPattern('*')
    await client.disconnect()
  })

  // Probed first as well. XDEL removes the entry from the stream but leaves
  // it in the group's pending list — a ghost no consumer can ever process:
  //
  //   pending before XDEL        -> [id-0, id-1, id-2]
  //   after XDEL id-1            -> [id-0, id-1, id-2]   (still pending!)
  //   XAUTOCLAIM entries         -> [id-0, id-2]
  //   XAUTOCLAIM deleted (3rd)   -> [id-1]
  //   pending after the sweep    -> [id-0, id-2]
  //
  // So `deleted` is not bookkeeping: it is the list of work that is GONE, and
  // the sweep is what clears the ghosts out of the PEL. A caller that ignores
  // the field loses entries silently — the same class as PUBLISH with zero
  // receivers.
  test('xautoclaim reports entries whose data was deleted and clears them from the PEL', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'ghost:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    const ids = []
    for (const task of ['one', 'two', 'three']) {
      ids.push(await client.xadd('jobs', '*', 'task', task))
    }

    await client.xgroup('CREATE', 'jobs', 'workers', '0', true)
    await client.xreadgroup('workers', 'dead-worker', { count: 10 }, ['jobs', '>'])

    await client.xdel('jobs', ids[1])
    assert.equal(
      (await client.xpending('jobs', 'workers'))[0], 3,
      'deleting the entry does NOT take it out of the pending list'
    )

    const swept = await client.xautoclaim('jobs', 'workers', 'live-worker', 0)

    assert.deepEqual(swept.entries.map(([id]) => id), [ids[0], ids[2]], 'only the surviving entries are handed over')
    assert.deepEqual(swept.deleted, [ids[1]], 'and the lost one is reported, not swallowed')
    assert.equal((await client.xpending('jobs', 'workers'))[0], 2, 'the ghost is gone from the PEL')

    await client.xgroup('DESTROY', 'jobs', 'workers')
    await client.deleteByPattern('*')
    await client.disconnect()
  })

  // The lock's Lua scripts are cached by SHA on the server, and that cache
  // dies with a restart or a failover. Proving the reload works is the
  // difference between "ioredis probably handles it" and knowing.
  // The shape a distributed circuit breaker needs: a fenced compare-and-set,
  // where a stale generation must not be able to close a window a newer one
  // opened. Registered Lua is what makes it atomic, and EVALSHA is what keeps
  // it cheap enough to run on every request.
  test('registered Lua runs a fenced compare-and-set, prefix and all', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'bw:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    client.defineScript('fencedSet', {
      numberOfKeys: 1,
      lua: `
        local current = tonumber(redis.call("hget", KEYS[1], "generation")) or 0
        if tonumber(ARGV[1]) < current then return {0, current} end
        redis.call("hset", KEYS[1], "generation", ARGV[1], "state", ARGV[2])
        return {1, tonumber(ARGV[1])}
      `
    })

    assert.deepEqual(await client.runScript('fencedSet', ['breaker'], [1, 'open']), [1, 1])
    assert.deepEqual(await client.runScript('fencedSet', ['breaker'], [2, 'half']), [1, 2])

    // The stale generation loses, which is the entire point of the fence.
    assert.deepEqual(await client.runScript('fencedSet', ['breaker'], [1, 'closed']), [0, 2])
    assert.equal((await client.hgetall('breaker')).state, 'half', 'the newer generation stands')

    // keyPrefix reaches the script's KEYS, so the state lives where the rest
    // of the application's keys live — not next to them. Asked through the
    // admin connection, which carries no prefix: `client.client` would prefix
    // the question too and answer about `bw:bw:breaker`.
    assert.equal(await admin.exists('bw:breaker'), 1, 'the script wrote to the prefixed key')
    assert.equal(await admin.exists('breaker'), 0, 'and nothing landed unprefixed')

    // And the SHA cache dying does not take the breaker with it.
    await client.client.script('FLUSH')
    assert.deepEqual(await client.runScript('fencedSet', ['breaker'], [3, 'closed']), [1, 3])

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('locking survives the script cache being wiped (NOSCRIPT)', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const lock = await client.acquireLock('script-cache', { ttl: 10000 })
    assert.equal(await lock.extend(10000), true)

    // Exactly what a restarted server looks like to a client that already
    // registered its scripts.
    await admin.script('FLUSH')

    assert.equal(await lock.extend(10000), true, 'extend must reload the script instead of failing')
    assert.equal(await lock.release(), true, 'and so must release')

    const again = await client.acquireLock('script-cache')
    assert.equal(await again.release(), true)

    await client.disconnect()
  })

  // Pipelines answer positionally: one failing command must not shift the
  // replies of its neighbours.
  test('a failing command in a pipeline does not desynchronize the replies', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'pipe:', logger: quietLogger })
    await client.connect()
    await client.deleteByPattern('*')

    await client.lpush('list', 'not-a-counter')

    const transaction = await client.multi()
    const results = await transaction
      .set('first', 'ok')
      .incr('list') // WRONGTYPE: fails in the middle
      .set('third', 'ok')
      .exec()

    assert.equal(results.length, 3, 'every command still reports back')
    assert.deepEqual(results[0], [null, 'OK'])
    assert.equal(results[1][0] instanceof Error, true, 'the failure stays in its own slot')
    assert.match(results[1][0].message, /WRONGTYPE/)
    assert.deepEqual(results[2], [null, 'OK'], 'the command after the failure keeps its position')

    assert.equal(await client.get('first'), 'ok')
    assert.equal(await client.get('third'), 'ok', 'a runtime error does not roll the batch back')

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('keyspace events are delivered once enabled, and refused when they are not', { timeout: 20000 }, async () => {
    const [, original] = await admin.config('GET', 'notify-keyspace-events')

    try {
      await admin.config('SET', 'notify-keyspace-events', '')

      const disabled = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
      await disabled.connect()

      await assert.rejects(disabled.subscribeToKeyEvents('expired', () => {}), {
        code: 'KEYSPACE_NOTIFICATIONS_DISABLED'
      }, 'a channel that cannot speak must not look like one that works')
      await disabled.disconnect()

      // Now the server is configured to emit expiration events.
      await admin.config('SET', 'notify-keyspace-events', 'Ex')

      const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
      await client.connect()

      const expired = []
      await client.subscribeToKeyEvents('expired', (key) => expired.push(key))

      await client.setex('it:vanishing', 1, 'value')
      await waitFor(() => expired.includes('it:vanishing'), {
        timeout: 8000,
        message: 'the expiration event to arrive'
      })

      await client.disconnect()
    } finally {
      await admin.config('SET', 'notify-keyspace-events', original)
    }
  })

  // Regression: a blocking read used to survive disconnect() — its promise
  // never settled and its dedicated socket stayed open on the server, so a
  // graceful shutdown hung forever.
  test('disconnect cancels an in-flight blocking read and reclaims its connection', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, connectionName: 'it-shutdown', logger: quietLogger })
    await client.connect()
    await client.set('it:shutdown:warm', '1')

    // BLOCK 0 waits forever: only the shutdown can end this. The expectation
    // is attached now, before disconnect() rejects it — otherwise the
    // rejection lands with no handler and the runner reports it as unhandled.
    const blocked = client.xread({ block: 0 }, ['it:shutdown:stream', '$'])
    const cancelled = assert.rejects(blocked, {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE',
      operation: 'xread'
    }, 'the caller must be told its read was cancelled, not left waiting')

    await waitFor(async () => (await listClientIdsNamed('it-shutdown')).length >= 2, {
      message: 'the dedicated connection to reach the server'
    })

    await client.disconnect()
    await cancelled

    await waitFor(async () => (await listClientIdsNamed('it-shutdown')).length === 0, {
      message: 'every connection of this client to be gone'
    })

    await admin.del('it:shutdown:warm')
  })

  // Regression (AUDIT B7): a blocking read on the shared connection stalled
  // every other command until the block resolved.
  test('blocking xread does not stall concurrent commands', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const blocked = client.xread({ block: 2000 }, ['it:blockstream', '$'])

    const started = Date.now()
    await client.set('it:concurrent', 'concurrent')
    const elapsed = Date.now() - started

    assert.ok(elapsed < 500, `concurrent set took ${elapsed}ms while xread was blocking`)

    assert.equal(await blocked, null)

    await client.del('it:concurrent')
    await client.disconnect()
  })

  // A consumer loop calls a blocking read on every iteration. Opening a fresh
  // connection each time is a full handshake per iteration, so they are pooled
  // — but only if the driver really leaves the socket 'ready' when the block
  // ends. Against a fake it always does; this is the real one saying so.
  test('consecutive blocking reads reuse one connection', { timeout: 20000 }, async () => {
    const client = new RedisClient({
      host: HOST,
      port: PORT,
      connectionName: 'it-blockpool',
      logger: quietLogger
    })
    await client.connect()

    for (let round = 0; round < 3; round++) {
      assert.equal(await client.xread({ block: 50 }, ['it:blockpool:stream', '$']), null)
    }

    const ids = await listClientIdsNamed('it-blockpool')

    assert.equal(ids.length, 2, `three blocking reads must leave one shared + one pooled connection, got ${ids.length}`)

    await client.disconnect()

    await waitFor(async () => (await listClientIdsNamed('it-blockpool')).length === 0, {
      message: 'shutdown to release the pooled connection too'
    })
  })

  // Regression: a quit() parked behind the driver's offline queue never
  // answers, and disconnect() used to await it with no deadline — shutdown
  // hung for the lifetime of the process.
  test('disconnect finishes even with a command stuck in the offline queue', { timeout: 20000 }, async () => {
    const client = new RedisClient({
      host: HOST,
      port: 6399, // nothing listens here: the driver retries forever
      baseRetryDelay: 50,
      maxRetryDelay: 200,
      logger: quietLogger
    })

    await client.connect()

    // The escape hatch the README advertises, used during an outage.
    client.client?.get('it:queued').catch(() => {})

    const started = Date.now()
    await client.disconnect()
    const elapsed = Date.now() - started

    assert.ok(elapsed < 5000, `disconnect took ${elapsed}ms — the deadline did not hold`)
    assert.equal(client.client, null, 'and the client is released')
  })

  // The facade is an EventEmitter: connection lifecycle is observable.
  // Probed before this test was written, because presence is not the contract
  // — order is. Applications wire on('close', pause) / on('ready', resume),
  // and a ready that arrives before its own close inverts their state. Raw
  // sequence from a real CLIENT KILL:
  //
  //       6ms  ready           (connect)
  //      12ms  close           (the kill)
  //      12ms  reconnecting
  //     116ms  ready           (recovered)
  //    2516ms  close           (disconnect)
  //    2517ms  end
  //
  // And the thing the probe settled that no fake would have: a server-side
  // kill emits NO connectionError. The socket closes cleanly from the
  // client's side, so anyone alerting on connectionError is blind to exactly
  // the failure they think they are watching.
  test('emits the connection lifecycle in order across a server-side kill', { timeout: 30000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, logger: quietLogger })
    const events = []
    const errors = []

    for (const name of ['ready', 'close', 'reconnecting', 'end']) {
      client.on(name, () => events.push(name))
    }
    client.on('connectionError', (err) => errors.push(err))

    await client.connect()
    assert.deepEqual(events, ['ready'], 'connect emits ready, and nothing else')

    await client.set('it:events', '1')
    await waitFor(async () => (await listOtherClientIds()).length >= 1, {
      message: 'library connection to appear in CLIENT LIST'
    })
    await killAllOtherClients()

    await waitFor(async () => events.filter((e) => e === 'ready').length >= 2, {
      message: 'a second ready after the server-side kill'
    })

    await client.disconnect()
    await waitFor(async () => events.includes('end'), { message: 'end after disconnect()' })

    // Repetitions are allowed (the driver may bounce more than once); an
    // inversion is not.
    const positionOf = (name) => events.indexOf(name)
    const recovery = events.indexOf('ready', 1)

    assert.ok(positionOf('close') > 0, 'close fires when the connection drops')
    assert.ok(
      positionOf('close') < positionOf('reconnecting'),
      `close must precede reconnecting, got ${JSON.stringify(events)}`
    )
    assert.ok(
      positionOf('reconnecting') < recovery,
      `reconnecting must precede the recovery ready, got ${JSON.stringify(events)}`
    )
    assert.equal(events.at(-1), 'end', 'end is last, and only after disconnect()')

    assert.deepEqual(errors, [], 'a server-side kill is not an error event — do not alert on it')

    await client.del?.('it:events').catch(() => {})
  })

  // withDedicatedConnection isolates WATCH/MULTI/EXEC: the watch must abort
  // the transaction when the watched key changes — something the shared
  // connection could never guarantee under concurrency.
  test('withDedicatedConnection provides working optimistic locking', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'tx:', connectionName: 'it-tx', logger: quietLogger })
    await client.connect()

    await client.set('balance', '10')

    // Happy path: watch + multi + exec commits.
    const committed = await client.withDedicatedConnection(async (conn) => {
      await conn.watch('balance')
      const current = Number(await conn.get('balance'))

      return conn.multi().set('balance', String(current + 5)).exec()
    })
    assert.ok(committed, 'transaction must commit when the key is untouched')
    assert.equal(await client.get('balance'), '15')

    // Conflict path: the shared connection mutates the watched key mid-flight
    // and EXEC must abort (null) — proof the WATCH actually protects.
    const aborted = await client.withDedicatedConnection(async (conn) => {
      await conn.watch('balance')
      await client.set('balance', '99')

      return conn.multi().set('balance', '0').exec()
    })
    assert.equal(aborted, null, 'transaction must abort when the watched key changes')
    assert.equal(await client.get('balance'), '99')

    // The dedicated connections are released afterwards.
    await waitFor(async () => (await listClientIdsNamed('it-tx')).length === 1, {
      message: 'dedicated connections to be released'
    })

    await client.del('balance')
    await client.disconnect()
  })

  test('pub/sub delivers to channel handlers, pattern handlers and facade events', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, connectionName: 'it-pubsub', logger: quietLogger })
    await client.connect()

    const received = { handler: [], event: [], pattern: [] }

    client.on('message', (channel, message) => received.event.push([channel, message]))

    await client.subscribe('news', (message, channel) => received.handler.push([channel, message]))
    await client.psubscribe('logs.*', (message, channel, pattern) => received.pattern.push([pattern, channel, message]))

    assert.equal(await client.publish('news', 'hello'), 1)
    await client.publishJson('logs.app', { level: 'info' })

    await waitFor(() => received.handler.length >= 1 && received.event.length >= 1 && received.pattern.length >= 1, {
      message: 'all subscription paths to deliver'
    })

    assert.deepEqual(received.handler[0], ['news', 'hello'])
    assert.deepEqual(received.event[0], ['news', 'hello'])
    assert.deepEqual(received.pattern[0], ['logs.*', 'logs.app', '{"level":"info"}'])

    // Unsubscribing stops delivery.
    await client.unsubscribe('news')
    await client.publish('news', 'after-unsubscribe')
    await sleep(300)
    assert.equal(received.handler.length, 1, 'no delivery after unsubscribe')

    // disconnect() releases the subscriber connection too.
    await client.disconnect()
    const remaining = (await listClientIdsNamed('it-pubsub')).length
    assert.equal(remaining, 0, `server still sees ${remaining} connection(s) after disconnect()`)
  })

  // The sacred test, pub/sub edition: the subscriber connection is killed
  // server-side and the subscription must survive (driver-owned reconnection
  // + autoResubscribe on the SAME client object).
  test('pub/sub survives a server-side CLIENT KILL', { timeout: 30000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, logger: quietLogger })
    await client.connect()

    const received = []
    await client.subscribe('resilient', (message) => received.push(message))

    await client.publish('resilient', 'before')
    await waitFor(() => received.includes('before'), { message: 'first message to arrive' })

    await killAllOtherClients()

    // Publish until the resubscribed consumer hears us again.
    await waitFor(async () => {
      try {
        await client.publish('resilient', 'after')
      } catch {
        return false
      }

      return received.includes('after')
    }, { timeout: 20000, message: 'delivery to resume after the kill' })

    await client.disconnect()
  })

  // The test above proves delivery RESUMES. It does not — cannot — say what
  // happens to whatever was published while the subscriber was away, because
  // it publishes in a loop until something gets through, which routes around
  // the gap instead of measuring it.
  //
  // Measured with a sequence published every 5ms across three runs:
  //
  //   run 1: published=419 received=400 lost=19  (~95ms window)
  //   run 2: published=426 received=406 lost=20  (~100ms window)
  //   run 3: published=424 received=404 lost=20  (~100ms window)
  //
  // Redis pub/sub is fire-and-forget: a message published while nobody is
  // subscribed is not queued, it is dropped. Reconnection cannot be
  // transparent, only fast. This test pins the part that IS a contract —
  // the gap closes and stays closed — rather than the size of the window.
  test('pub/sub loses messages published during the reconnect window, then recovers cleanly', { timeout: 30000 }, async () => {
    const client = new RedisClient({
      host: HOST,
      port: PORT,
      connectionName: 'it-gap',
      baseRetryDelay: 50,
      maxRetryDelay: 200,
      logger: quietLogger
    })
    await client.connect()

    const seen = new Set()
    await client.subscribe('it:gap', (message) => seen.add(Number(message)))

    let published = 0
    const publisher = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await publisher.connect()

    const pump = setInterval(() => { publisher.publish('it:gap', String(++published)).catch(() => {}) }, 5)

    try {
      await waitFor(async () => seen.size >= 5, { message: 'delivery to start' })

      await killAllOtherClients()
      const killedAround = published

      await waitFor(async () => published > killedAround + 10 && [...seen].some((n) => n > killedAround + 5), {
        timeout: 20000,
        message: 'delivery to resume after the kill'
      })

      // Everything published from here on must arrive: the window is a
      // window, not a leak that keeps dripping.
      const resumedAt = published
      await waitFor(async () => published > resumedAt + 20, { message: 'more traffic after recovery' })
      clearInterval(pump)
      await sleep(300)

      const lostAfterRecovery = []
      for (let n = resumedAt + 2; n <= published - 2; n++) {
        if (!seen.has(n)) lostAfterRecovery.push(n)
      }

      assert.deepEqual(lostAfterRecovery, [], 'once resubscribed, nothing may be dropped')
      assert.ok(seen.size < published, 'and the kill did cost messages — pub/sub is fire-and-forget')
    } finally {
      clearInterval(pump)
      await publisher.disconnect()
      await client.disconnect()
    }
  })

  // Review gap: releases go through Lua/defineCommand — this proves ioredis
  // applies keyPrefix to the script KEYS just like it does to SET.
  test('locks work correctly under a keyPrefix', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'pfx:', logger: quietLogger })
    await client.connect()

    const lock = await client.acquireLock('prefixed', { ttl: 5000 })

    assert.equal(await admin.exists('pfx:lock:prefixed'), 1, 'lock key must live under the prefix')
    assert.equal(await lock.release(), true)
    assert.equal(await admin.exists('pfx:lock:prefixed'), 0, 'release must delete the prefixed key')

    await client.disconnect()
  })

  // Review gap: when the driver exhausts its retries the client must be
  // released ('end') and a later connect() must start a fresh cycle.
  test('exhausted retries release the client and connect() can start over', { timeout: 15000 }, async () => {
    const client = new RedisClient({
      host: HOST,
      port: 1,
      maxRetryAttempts: 1,
      baseRetryDelay: 10,
      logger: quietLogger
    })

    let ends = 0
    client.on('end', () => { ends++ })

    await client.connect()
    await waitFor(() => ends >= 1 && client.client === null, { message: 'driver to give up and release the client' })

    await assert.rejects(client.set('it:giveup', 'x'), { code: 'REDIS_UNAVAILABLE' })

    // A fresh cycle starts (and gives up again — the point is that it CAN).
    await client.connect()
    await waitFor(() => ends >= 2, { message: 'a second full cycle after the first give-up' })
    assert.equal(client.client, null)
  })

  // Review gap: a rejecting async handler must be logged, never crash.
  test('pub/sub handler rejections are caught and logged', { timeout: 15000 }, async () => {
    const errors = []
    const spyLogger = { ...quietLogger, error: (message) => errors.push(String(message)) }

    const client = new RedisClient({ host: HOST, port: PORT, logger: spyLogger })
    await client.connect()

    await client.subscribe('explosive', async () => {
      throw new Error('handler boom')
    })

    await client.publish('explosive', 'trigger')

    await waitFor(() => errors.some((m) => m.includes('handler') && m.includes('boom')), {
      message: 'the handler rejection to be logged'
    })

    // The client is still fully functional afterwards.
    assert.equal(await client.set('it:alive', '1'), 'OK')
    await client.del('it:alive')
    await client.disconnect()
  })

  // Review gap: the documented SyntaxError on non-JSON payloads.
  test('getJson rejects with SyntaxError on malformed payloads', async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    await client.set('it:notjson', '{definitely-not-json')
    await assert.rejects(client.getJson('it:notjson'), SyntaxError)

    await client.del('it:notjson')
    await client.disconnect()
  })

  test('getOrSetJson produces on miss, serves from cache on hit and sets the ttl', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    let calls = 0
    const producer = async () => {
      calls++
      return { expensive: true }
    }

    assert.deepEqual(await client.getOrSetJson('it:cache:miss', 60, producer), { expensive: true })
    assert.equal(calls, 1)

    // Hit: the producer must not run again.
    assert.deepEqual(await client.getOrSetJson('it:cache:miss', 60, producer), { expensive: true })
    assert.equal(calls, 1)

    const ttl = await client.ttl('it:cache:miss')
    assert.ok(ttl > 0 && ttl <= 60, `ttl must be set (got ${ttl})`)

    await client.del('it:cache:miss')
    await client.disconnect()
  })

  // The stampede case: N concurrent misses with lock enabled must collapse
  // into exactly ONE producer call.
  test('getOrSetJson with lock collapses concurrent misses into one producer call', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    let calls = 0
    const slowProducer = async () => {
      calls++
      await sleep(300)
      return { produced: calls }
    }

    const results = await Promise.all(
      Array.from({ length: 10 }, () => client.getOrSetJson('it:cache:stampede', 60, slowProducer, { lock: true }))
    )

    assert.equal(calls, 1, `producer ran ${calls} times — the stampede was not contained`)
    for (const result of results) {
      assert.deepEqual(result, { produced: 1 })
    }

    await client.del('it:cache:stampede')
    await client.disconnect()
  })

  // Review regression: an undefined-returning producer used to store an
  // empty string with a ttl, poisoning every later read with SyntaxError.
  test('getOrSetJson never caches an invalid producer value', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    await assert.rejects(client.getOrSetJson('it:cache:poison', 60, () => undefined), { code: 'INVALID_ARGUMENT' })
    assert.equal(await client.get('it:cache:poison'), null, 'the key must not be written')

    // The key stays healthy for a well-behaved producer afterwards.
    assert.deepEqual(await client.getOrSetJson('it:cache:poison', 60, () => ({ ok: true })), { ok: true })

    await client.del('it:cache:poison')
    await client.disconnect()
  })

  // Review regression: waiters that exhausted the lock retry budget used to
  // reject with LOCK_NOT_ACQUIRED — a lock error leaking from a cache call.
  test('getOrSetJson degrades gracefully when the lock budget is exceeded', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    let calls = 0
    const slowProducer = async () => {
      calls++
      await sleep(600)
      return { ok: true }
    }

    // Tiny budget (~2 retries of 50ms) against a 600ms producer: the loser
    // exhausts its retries while the winner still holds the lock.
    const results = await Promise.allSettled([
      client.getOrSetJson('it:cache:budget', 60, slowProducer, { lock: { retries: 2, retryDelay: 50, retryJitter: 0 } }),
      client.getOrSetJson('it:cache:budget', 60, slowProducer, { lock: { retries: 2, retryDelay: 50, retryJitter: 0 } })
    ])

    for (const result of results) {
      assert.equal(result.status, 'fulfilled', `cache calls must not reject with lock errors (${result.reason?.code})`)
      assert.deepEqual(result.value, { ok: true })
    }

    assert.ok(calls <= 2, `fallback may produce at most once per loser (producer ran ${calls} times)`)

    await client.del('it:cache:budget')
    await client.disconnect()
  })

  test('deleteByPattern unlinks only matching keys inside the prefix', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, keyPrefix: 'dbp:', logger: quietLogger })
    await client.connect()

    await client.mset({ 'cache:a': '1', 'cache:b': '2', 'cache:c': '3', 'keep:me': '4' })
    await admin.set('cache:foreign', 'outside-the-prefix')

    const deleted = await client.deleteByPattern('cache:*')
    assert.equal(deleted, 3)

    assert.equal(await client.get('keep:me'), '4', 'non-matching keys must survive')
    assert.equal(await admin.get('cache:foreign'), 'outside-the-prefix', 'keys outside the prefix must survive')

    await client.deleteByPattern('*')
    assert.equal(await client.get('keep:me'), null)

    await admin.del('cache:foreign')
    await client.disconnect()
  })

  test('withLock autoExtend keeps a lock held beyond its ttl', { timeout: 20000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    await client.withLock('longjob', { ttl: 400, autoExtend: true }, async () => {
      // Well past the original ttl: the watchdog must have extended it.
      await sleep(1200)

      await assert.rejects(client.acquireLock('longjob'), { code: 'LOCK_NOT_ACQUIRED' })
    })

    // Released after fn: acquirable again.
    const lock = await client.acquireLock('longjob')
    assert.equal(await lock.release(), true)

    await client.disconnect()
  })

  test('locks are mutually exclusive and only the holder can release', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const lock = await client.acquireLock('job', { ttl: 5000 })

    await assert.rejects(client.acquireLock('job'), {
      name: 'RedisClientError',
      code: 'LOCK_NOT_ACQUIRED'
    })

    assert.equal(await lock.release(), true)

    // Released: acquirable again.
    const second = await client.acquireLock('job', { ttl: 5000 })
    assert.equal(await second.release(), true)

    await client.disconnect()
  })

  test('an expired lock can be taken over and the old holder cannot release it', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const first = await client.acquireLock('expiring', { ttl: 150 })
    await sleep(300)

    const second = await client.acquireLock('expiring', { ttl: 5000 })

    // The stale holder must not delete the new holder's lock.
    assert.equal(await first.release(), false)
    await assert.rejects(client.acquireLock('expiring'), { code: 'LOCK_NOT_ACQUIRED' })

    assert.equal(await second.release(), true)
    await client.disconnect()
  })

  test('extend() prolongs a held lock', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const lock = await client.acquireLock('extendable', { ttl: 500 })
    assert.equal(await lock.extend(5000), true)

    await sleep(800)

    // Past the original ttl: still held thanks to the extension.
    await assert.rejects(client.acquireLock('extendable'), { code: 'LOCK_NOT_ACQUIRED' })

    assert.equal(await lock.release(), true)
    await client.disconnect()
  })

  test('withLock runs the critical section, releases afterwards and honors retries', { timeout: 15000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, logger: quietLogger })
    await client.connect()

    const result = await client.withLock('section', async (lock) => {
      assert.equal(typeof lock.token, 'string')
      await assert.rejects(client.acquireLock('section'), { code: 'LOCK_NOT_ACQUIRED' })

      return 'done'
    })
    assert.equal(result, 'done')

    // Released after the callback: contention resolved by retries.
    const holder = await client.acquireLock('section', { ttl: 5000 })
    const contender = client.withLock('section', { retries: 20, retryDelay: 100 }, async () => 'finally')

    await sleep(300)
    await holder.release()

    assert.equal(await contender, 'finally')

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

  // One recovery can succeed by luck. Two in a row, with the state carried
  // across both, is what says the client actually rebuilt itself.
  test('recovers across two consecutive server-side kills', { timeout: 60000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, connectionName: 'it-cycles', logger: quietLogger })
    const readyEvents = []

    client.on('ready', () => readyEvents.push(Date.now()))
    await client.connect()

    for (const cycle of [1, 2]) {
      assert.equal(await client.set(`it:cycles:${cycle}`, 'before'), 'OK')

      await waitFor(async () => (await listOtherClientIds()).length >= 1, {
        message: `the connection to be visible before kill ${cycle}`
      })
      assert.ok(await killAllOtherClients() >= 1, `kill ${cycle} must land`)

      await waitFor(async () => {
        try {
          return (await client.set(`it:cycles:${cycle}`, 'after')) === 'OK'
        } catch {
          return false
        }
      }, { timeout: 20000, message: `recovery after kill ${cycle}` })

      assert.equal(await client.get(`it:cycles:${cycle}`), 'after', `cycle ${cycle} must be fully usable again`)
    }

    assert.ok(readyEvents.length >= 3, `each recovery must announce itself: ${readyEvents.length} ready events`)

    // Two full cycles, still one connection: nothing accumulated on the way.
    await sleep(1000)
    assert.equal((await listClientIdsNamed('it-cycles')).length, 1)

    await client.del('it:cycles:1', 'it:cycles:2')
    await client.disconnect()
  })

  // The nastier shape of the same failure: the server drops the client again
  // while it is still recovering from the previous drop.
  test('recovers from a flapping server that keeps dropping it mid-recovery', { timeout: 60000 }, async () => {
    // The backoff cap matters here: attempts that never reach a ready
    // connection keep doubling (2^attempt * base), so without a cap a
    // flapping stretch pushes the next retry half a minute away.
    const client = new RedisClient({
      host: HOST,
      port: PORT,
      baseRetryDelay: 50,
      maxRetryDelay: 200,
      connectionName: 'it-flapping',
      logger: quietLogger
    })
    await client.connect()
    await client.set('it:flapping', 'before')

    // Repeated kills: several land while a reconnection from the previous one
    // is still in flight.
    let kills = 0

    for (let round = 0; round < 8; round++) {
      kills += await killAllOtherClients()
      await sleep(60)
    }

    assert.ok(kills >= 2, `the point is repeated drops, only ${kills} landed`)

    await waitFor(async () => {
      try {
        return (await client.set('it:flapping', 'after')) === 'OK'
      } catch {
        return false
      }
    }, { timeout: 25000, message: 'recovery once the server settles' })

    assert.equal(await client.get('it:flapping'), 'after')

    // Every abandoned attempt must be gone, not lingering as a spare socket.
    await sleep(1000)
    assert.equal((await listClientIdsNamed('it-flapping')).length, 1, 'flapping must not leave a trail of connections')

    await client.del('it:flapping')
    await client.disconnect()
  })

  test('pub/sub survives two consecutive kills and a flapping server', { timeout: 60000 }, async () => {
    const client = new RedisClient({
      host: HOST,
      port: PORT,
      baseRetryDelay: 50,
      maxRetryDelay: 200,
      logger: quietLogger
    })
    await client.connect()

    const received = []
    await client.subscribe('resilient:cycles', (message) => received.push(message))

    const deliverAgain = async (marker) => {
      await waitFor(async () => {
        try {
          await client.publish('resilient:cycles', marker)
        } catch {
          return false
        }

        return received.includes(marker)
      }, { timeout: 25000, message: `delivery of '${marker}'` })
    }

    await deliverAgain('before')

    // Two clean cycles.
    for (const cycle of [1, 2]) {
      await killAllOtherClients()
      await deliverAgain(`after-kill-${cycle}`)
    }

    // Then a flapping stretch on top.
    for (let round = 0; round < 8; round++) {
      await killAllOtherClients()
      await sleep(60)
    }

    await deliverAgain('after-flapping')

    // Deduplicated: delivery is retried until it lands, so a marker can
    // legitimately arrive more than once.
    const markers = [...new Set(received.filter((message) => message.startsWith('after')))].sort()

    assert.deepEqual(markers, [
      'after-flapping', 'after-kill-1', 'after-kill-2'
    ], 'the subscription must be restored every single time')

    await client.disconnect()
  })

  // Regression probe for AUDIT C1/C2: the manual reconnection layer spawns a
  // brand-new ioredis client per attempt while the driver's own retryStrategy
  // also reconnects the old one — every recovery must end with the SAME
  // number of connections it started with.
  test('does not leak extra connections while recovering', { timeout: 60000 }, async () => {
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, connectionName: 'it-leak', logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:leak:probe', '1'), 'OK')
    await waitFor(async () => (await listClientIdsNamed('it-leak')).length >= 1, {
      message: 'library connection to appear in CLIENT LIST'
    })
    const baseline = (await listClientIdsNamed('it-leak')).length

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

    const settled = (await listClientIdsNamed('it-leak')).length
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
    const client = new RedisClient({ host: HOST, port: PORT, baseRetryDelay: 50, connectionName: 'it-final', logger: quietLogger })
    await client.connect()

    assert.equal(await client.set('it:cycle:probe', '1'), 'OK')
    await client.disconnect()

    // Long enough for any wrongly-scheduled reconnection to have fired.
    await sleep(2000)

    assert.equal(client.client, null, 'internal client must remain null after disconnect()')
    assert.equal(client.isConnected, false, 'isConnected must remain false after disconnect()')

    const remaining = (await listClientIdsNamed('it-final')).length
    assert.equal(remaining, 0, `server still sees ${remaining} connection(s) after disconnect()`)

    await assert.rejects(client.set('it:cycle:probe', '2'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE'
    })
  })
})

// Cluster changes what the library can assume: keys live on different nodes,
// a command sent to the wrong one is answered with MOVED (or ASK while slots
// migrate), and there is no cluster-wide SCAN. Gated separately because it
// needs its own topology: docker compose -f docker-compose.cluster.yml up -d
describe('redis cluster integration', { skip: !RUN_CLUSTER && 'set REDIS_CLUSTER_INTEGRATION=1 (needs docker-compose.cluster.yml)' }, () => {
  const createClusterClient = (options = {}) =>
    new RedisClient({ nodes: CLUSTER_NODES, keyPrefix: 'ct:', logger: quietLogger, ...options })

  // Review finding: the documented backoff reached the Cluster object and
  // stopped there. ioredis's ConnectionPool sets `retryStrategy` on each node
  // connection from `clusterNodeRetryStrategy` (default `null`) BEFORE merging
  // redisOptions, and lodash `defaults` never overwrites — so the strategy
  // filed under redisOptions was shadowed and node connections never
  // reconnected. This asserts it against the real pool, because the shadowing
  // happens inside the driver and no unit fake can honestly model it. The
  // duplicate() matters as much as the node: that is what a keyspace-event
  // subscriber is built from, and it inherited the dead policy too.
  test('the retry policy reaches the real node connections and their duplicates', { timeout: 30000 }, async () => {
    const client = createClusterClient({ maxRetryAttempts: Infinity, baseRetryDelay: 10, maxRetryDelay: 100 })
    await client.connect()

    const [master] = client.client.nodes('master')
    const subscriberShaped = master.duplicate()

    try {
      assert.equal(typeof master.options.retryStrategy, 'function', 'a node connection that never retries is a shard that never comes back')
      assert.equal(master.options.retryStrategy(1), 20, 'and it must be the backoff this library documents')

      assert.equal(
        typeof subscriberShaped.options.retryStrategy,
        'function',
        'keyspace-event subscribers are duplicates of a node connection and inherit its policy'
      )
    } finally {
      subscriberShaped.disconnect()
      await client.disconnect()
    }
  })

  test('follows redirections for keys spread across the masters', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()
    await client.deleteByPattern('*')

    // Enough distinct keys that they cannot all hash to one slot: every read
    // that lands on the wrong node has to be redirected to reach its value.
    const keys = Array.from({ length: 60 }, (_, index) => `spread:${index}`)

    for (const key of keys) {
      await client.set(key, `value-${key}`)
    }

    for (const key of keys) {
      assert.equal(await client.get(key), `value-${key}`)
    }

    // Prove they really are spread: ask the cluster which slots they map to.
    const slots = new Set()
    for (const key of keys.slice(0, 10)) {
      slots.add(await client.client.cluster('KEYSLOT', `ct:${key}`))
    }
    assert.ok(slots.size > 1, 'the sample must span more than one slot for this to mean anything')

    const owners = new Set(client.client.nodes('master').map((node) => node.options.port))
    assert.equal(owners.size, 3, 'all three masters are in play')

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('scans and deletes across every master', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()
    await client.deleteByPattern('*')

    const written = {}
    for (let index = 0; index < 30; index++) {
      written[`scan:${index}`] = String(index)
      await client.set(`scan:${index}`, String(index))
    }
    await client.set('other:1', 'untouched')

    // A cluster has no SCAN of its own; this only works if every master is
    // walked and the slices are merged.
    const dumped = Object.assign({}, ...await client.getAllStream('scan:*'))
    assert.deepEqual(dumped, written, 'every key must come back, wherever it lives')

    assert.equal(await client.deleteByPattern('scan:*'), 30)
    assert.deepEqual(await client.getAllStream('scan:*'), [])
    assert.equal(await client.get('other:1'), 'untouched')

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('single-key features work unchanged: locks, cache and counters', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()
    await client.deleteByPattern('*')

    // The lock's Lua scripts declare one key, so the cluster can route them.
    const held = await client.withLock('cluster-job', { ttl: 5000 }, async () => {
      await assert.rejects(client.acquireLock('cluster-job'), { code: 'LOCK_NOT_ACQUIRED' })

      return 'critical section ran'
    })
    assert.equal(held, 'critical section ran')

    let produced = 0
    const cached = await client.getOrSetJson('report', 60, () => { produced++; return { ok: true } }, { lock: true })
    assert.deepEqual(cached, { ok: true })
    assert.deepEqual(await client.getOrSetJson('report', 60, () => { produced++; return { no: true } }), { ok: true })
    assert.equal(produced, 1)

    await client.zadd('board', { ada: 120, grace: 180 })
    assert.deepEqual(await client.zrevrange('board', 0, 0, { withScores: true }), [{ member: 'grace', score: 180 }])

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('multi-key commands need one slot, and hash tags are how you get it', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()
    await client.deleteByPattern('*')

    await client.set('loose:1', 'a')
    await client.set('loose:2', 'b')

    // Two keys, two slots: the server refuses rather than guessing.
    await assert.rejects(client.mget('loose:1', 'loose:2'), /CROSSSLOT/)

    // A hash tag pins both keys to the same slot, so the same call works.
    await client.mset({ '{user:1}:name': 'Ada', '{user:1}:role': 'admin' })
    assert.deepEqual(await client.mget('{user:1}:name', '{user:1}:role'), ['Ada', 'admin'])

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('pub/sub reaches subscribers regardless of which node they are on', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()

    const received = []
    await client.subscribe('cluster:events', (message) => received.push(message))

    await waitFor(async () => (await client.publish('cluster:events', 'hello')) > 0, {
      message: 'the subscription to register across the cluster'
    })
    await waitFor(() => received.includes('hello'), { message: 'the message to arrive' })

    await client.disconnect()
  })

  // Unlike publish, keyspace notifications are NOT carried by the cluster bus:
  // each node emits them for its own slots and never forwards them, while a
  // cluster subscriber attaches to a single node. Subscribing the plain way
  // delivers one shard's events and looks exactly like one that works — so
  // this asserts events arriving from more than one master.
  // The standalone suite proves the lock's Lua scripts survive SCRIPT FLUSH.
  // That says nothing about a cluster, where the SHA cache is PER NODE and
  // locks route by slot: a master that never ran the script answers NOSCRIPT.
  // Probed first — a script loaded on 7001 reports EXISTS=1 there and 0 on
  // both other masters, so the isolation is real and not an assumption.
  test('locks survive a script cache wiped on one master only', { timeout: 40000 }, async () => {
    const client = createClusterClient()
    await client.connect()

    const masters = client.client.nodes('master')
    assert.ok(masters.length >= 3, 'this test needs a multi-master cluster')

    // Which master owns a given key, straight from the protocol.
    const ownerOf = async (key) => {
      const slot = await client.client.cluster('KEYSLOT', key)
      const slots = await client.client.cluster('SLOTS')
      const range = slots.find(([from, to]) => slot >= from && slot <= to)

      return `${range[2][0]}:${range[2][1]}`
    }

    // Lock names that land on three different masters. The lock manager keys
    // them as `lock:<name>`, and the client prefixes with 'ct:'.
    const byOwner = new Map()
    for (let i = 0; i < 200 && byOwner.size < 3; i++) {
      const name = `noscript-${i}`
      const owner = await ownerOf(`ct:lock:${name}`)

      if (!byOwner.has(owner)) byOwner.set(owner, name)
    }
    assert.equal(byOwner.size, 3, 'the locks must be spread across all three masters')

    const held = []
    for (const name of byOwner.values()) {
      held.push(await client.acquireLock(name, { ttl: 30000 }))
    }

    // A script is known to the node that ran it, and to no other.
    const sha = await masters[0].script('LOAD', 'return 1')
    const known = await Promise.all(masters.map(async (node) => (await node.script('EXISTS', sha))[0]))

    assert.deepEqual(known, [1, 0, 0], 'the SHA cache lives on one node, not on the cluster')

    // Wipe the cache of a single master and prove every lock still settles:
    // ioredis has to reload the script on the node that answered NOSCRIPT.
    await masters[0].script('FLUSH')

    for (const lock of held) {
      assert.equal(await lock.extend(30000), true, `extend must survive on '${lock.name}'`)
    }

    for (const lock of held) {
      assert.equal(await lock.release(), true, `release must survive on '${lock.name}'`)
    }

    await client.deleteByPattern('*')
    await client.disconnect()
  })

  test('keyspace events arrive from every master, not just one', { timeout: 30000 }, async () => {
    const client = createClusterClient()
    await client.connect()
    await client.deleteByPattern('*')

    for (const node of client.client.nodes('master')) {
      await node.config('SET', 'notify-keyspace-events', 'Ex')
    }

    const expired = []
    await client.subscribeToKeyEvents('expired', (key) => expired.push(key))

    const keys = Array.from({ length: 24 }, (_, index) => `ttl:${index}`)

    for (const key of keys) {
      await client.set(key, 'x')
    }

    // The premise of the test, asserted rather than assumed: these keys really
    // do live on more than one master, so a subscription pinned to a single
    // node could not possibly report all of them.
    const perMaster = await Promise.all(
      client.client.nodes('master').map((node) => node.dbsize())
    )

    assert.ok(
      perMaster.filter((size) => size > 0).length >= 2,
      `the keys must span at least two masters, got ${JSON.stringify(perMaster)}`
    )

    for (const key of keys) {
      await client.executeCommand('pexpire', key, 50)
    }

    await waitFor(() => new Set(expired).size >= keys.length, {
      timeout: 20000,
      message: `every shard's expirations to arrive (got ${new Set(expired).size}/${keys.length})`
    })

    assert.deepEqual(
      [...new Set(expired)].sort(),
      keys.map((key) => `ct:${key}`).sort(),
      'no shard may go silent'
    )

    await client.disconnect()
  })
})

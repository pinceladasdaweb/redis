import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { describe, test } from 'node:test'
import ConnectionManager from '../src/connection/manager.js'
import { RedisClient } from '../src/index.js'
import { createManualClock } from './helpers/manual-clock.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

const createDriverClient = ({ connectFails = false, quitFails = false } = {}) => {
  const client = new EventEmitter()

  client.status = 'wait'
  client.calls = []
  client.connect = async () => {
    client.calls.push('connect')

    if (connectFails) throw new Error('connection refused')

    client.status = 'ready'
    client.emit('ready')
  }
  // Probed against ioredis 6 (16/08/2026): every teardown path emits
  // 'close' BEFORE 'end', and quit() RESOLVES before either fires. A fake
  // that only emits the "main" event hides every bug in a 'close' handler —
  // the exact blind spot that buried the RabbitMQ lib's worst bug.
  client.quit = async () => {
    client.calls.push('quit')

    if (quitFails) throw new Error('quit failed')

    client.status = 'end'
    setImmediate(() => {
      client.emit('close')
      client.emit('end')
    })
    return 'OK'
  }
  client.disconnect = () => {
    client.calls.push('disconnect')
    client.status = 'end'
    setImmediate(() => {
      client.emit('close')
      client.emit('end')
    })
  }

  return client
}

const createManager = (clientOptions = {}) => {
  const created = []
  const events = []
  const clock = createManualClock()

  const manager = new ConnectionManager({
    redisConfig: {
      createRedisClient: () => {
        const client = createDriverClient(clientOptions)
        created.push(client)
        return client
      }
    },
    logger: quietLogger,
    clock,
    emit: (...args) => events.push(args)
  })

  return { manager, created, events, clock }
}

describe('connection manager', () => {
  test('connect creates exactly one client and reports ready', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()

    assert.equal(created.length, 1)
    assert.equal(manager.isConnected, true)
    assert.deepEqual(events, [['ready']])
  })

  test('concurrent connects share a single attempt', async () => {
    const { manager, created } = createManager()

    await Promise.all([manager.connect(), manager.connect(), manager.connect()])

    assert.equal(created.length, 1, 'a connect mutex must prevent duplicate clients')
  })

  // Regression: the client is assigned synchronously, so a second caller used
  // to short-circuit on "a client already exists" and resolve while the
  // connection was still being established — reporting success too early.
  test('a concurrent connect waits for the attempt already in flight', async () => {
    const { manager } = createManager()
    const client = createDriverClient()

    client.connect = async () => {
      await new Promise((resolve) => setTimeout(resolve, 50))
      client.status = 'ready'
      client.emit('ready')
    }
    manager.redisConfig = { createRedisClient: () => client }

    const first = manager.connect()
    const second = manager.connect()

    await second
    assert.equal(manager.isConnected, true, 'awaiting connect() must mean the connection is usable')

    await first
  })

  test('connecting again with a live client is a no-op', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    await manager.connect()

    assert.equal(created.length, 1)
  })

  test('a failed initial connect keeps the client for background retries', async () => {
    const { manager, created } = createManager({ connectFails: true })

    await manager.connect()

    assert.equal(created.length, 1)
    assert.equal(manager.isConnected, false)
    assert.equal(manager.client, created[0], 'the driver keeps retrying on this client')
  })

  // A driver that rejects with the client already dead must not leave the
  // reference behind: connect() would short-circuit on it forever.
  test('a client that dies during connect is released, and connect can retry', async () => {
    const { manager } = createManager()
    const dead = createDriverClient()

    dead.connect = async () => {
      dead.status = 'end'
      throw new Error('gave up without emitting end')
    }

    const healthy = createDriverClient()
    let next = dead
    manager.redisConfig = { createRedisClient: () => next }

    await manager.connect()

    assert.equal(manager.client, null, 'the dead client must not stay assigned')
    assert.equal(manager.isConnected, false)

    // A later connect() starts a fresh cycle instead of reusing the corpse.
    next = healthy
    await manager.connect()

    assert.equal(manager.client, healthy)
    assert.equal(manager.isConnected, true)
  })

  test('driver lifecycle events reach the facade', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    const client = created[0]

    client.emit('close')
    assert.equal(manager.isConnected, false)

    client.emit('reconnecting', 250)
    client.emit('error', new Error('socket reset'))
    client.emit('end')

    assert.deepEqual(events.map(([name]) => name), ['ready', 'close', 'reconnecting', 'connectionError', 'end'])
    assert.equal(events.find(([name]) => name === 'reconnecting')[1], 250)
    assert.equal(events.some(([name]) => name === 'error'), false, "'error' would crash listener-less processes")
  })

  test('end releases the client so a later connect starts a fresh cycle', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    created[0].emit('end')

    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)

    await manager.connect()
    assert.equal(created.length, 2)
  })

  test('assertReady gates commands on the driver status', async () => {
    const { manager, created } = createManager()

    assert.throws(() => manager.assertReady('set'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE',
      operation: 'set'
    })

    await manager.connect()
    assert.equal(manager.assertReady('set'), created[0])

    created[0].status = 'reconnecting'
    assert.throws(() => manager.assertReady('get'), { code: 'REDIS_UNAVAILABLE' })
  })

  test('disconnect quits, releases and stays disconnected', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit'])
    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)
    assert.deepEqual(events.map(([name]) => name).at(-1), 'end')
  })

  test('disconnect is idempotent and safe before connect', async () => {
    const { manager, created } = createManager()

    await manager.disconnect()
    await manager.connect()
    await manager.disconnect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit'])
  })

  test('disconnect forces the socket closed when quit fails', async () => {
    const { manager, created } = createManager({ quitFails: true })

    await manager.connect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit', 'disconnect'])
    assert.equal(manager.client, null)
  })

  test('disconnect reacts to the end event, never to its escape timer', async () => {
    const { manager, clock } = createManager()

    await manager.connect()
    await manager.disconnect()

    // Resolved without the clock moving at all, and nothing left armed.
    assert.equal(clock.pending(), 0, 'the escape timer must be cleared by the end event')
  })

  // A driver that goes quiet must not hang shutdown forever: the escape timer
  // is the deadline, and it fires exactly when it says it does.
  test('disconnect gives up on a silent driver after exactly two seconds', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].quit = async () => {
      created[0].calls.push('quit')
      return 'OK'
    }

    let done = false
    const shutdown = manager.disconnect().then(() => { done = true })

    await clock.advance(1999)
    assert.equal(done, false, 'one millisecond before the deadline it is still waiting')

    await clock.advance(1)
    await shutdown

    assert.equal(done, true, 'and exactly on it, shutdown completes anyway')
    assert.equal(manager.client, null, 'the client is released regardless')
  })

  // Regression: ioredis only answers QUIT while its offline queue is empty.
  // With anything queued it parks the QUIT behind it and replies once the
  // connection is back — which, under the default infinite retries, is never.
  // The escape timer above used to sit BEHIND this await, so it could not fire
  // and disconnect() hung for the lifetime of the process.
  test('disconnect gives up on a quit that never answers', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    let done = false
    const shutdown = manager.disconnect().then(() => { done = true })

    await clock.advance(1999)
    assert.equal(done, false, 'one millisecond before the deadline it is still waiting')

    await clock.advance(1)
    await shutdown

    assert.equal(done, true, 'and on the deadline it finishes instead of hanging forever')
    assert.deepEqual(created[0].calls, ['connect', 'quit', 'disconnect'], 'the socket is forced closed')
    assert.equal(manager.client, null, 'and the client is released')
  })

  // Regression: disconnect() left the in-flight connect promise in place, so
  // the next connect() joined an attempt whose client had already been closed
  // — it resolved with nothing behind it and every command failed until a
  // third connect() happened to build a real one.
  test('connect after a disconnect mid-attempt starts a fresh cycle', async () => {
    const created = []
    const clock = createManualClock()
    let releaseFirstConnect

    const manager = new ConnectionManager({
      redisConfig: {
        createRedisClient: () => {
          const client = createDriverClient()

          if (created.length === 0) {
            // The first attempt is still negotiating when shutdown arrives.
            client.connect = () => {
              client.calls.push('connect')

              return new Promise((resolve) => { releaseFirstConnect = resolve })
            }
          }

          created.push(client)

          return client
        }
      },
      logger: quietLogger,
      clock,
      emit: () => {}
    })

    const first = manager.connect()
    await new Promise((resolve) => setImmediate(resolve))

    await manager.disconnect()

    const second = manager.connect()
    releaseFirstConnect()
    await Promise.all([first, second])

    assert.equal(created.length, 2, 'the second connect must build its own client')
    assert.equal(manager.client, created[1], 'and leave that client in place')
    assert.equal(manager.isConnected, true)
  })

  // Review finding: the reuse branch never checked liveness, so a connect()
  // during the (up to ~4s) teardown window resolved successfully against a
  // client that 'end' was about to null — "connected", with nothing behind it
  // and no retry in flight.
  test('connect() during disconnect() waits for the teardown and starts fresh', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    // A quit that never answers pins the teardown on its 2s deadline.
    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const teardown = manager.disconnect()
    const reconnect = manager.connect()

    let reconnected = false
    reconnect.then(() => { reconnected = true })

    await clock.advance(1999)
    assert.equal(reconnected, false, 'connect() must wait out the teardown, not race it')
    assert.equal(created.length, 1, 'and must not build a client while the old one is dying')

    await clock.advance(1)
    await teardown
    await reconnect

    assert.equal(created.length, 2, 'a fresh cycle starts once the teardown finished')
    assert.equal(manager.client, created[1], 'and the caller gets the LIVE client')
    assert.equal(manager.isConnected, true)
  })

  test('disconnect() is joined, never doubled, while a teardown is in flight', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const first = manager.disconnect()
    const second = manager.disconnect()

    await clock.advance(2000)
    await Promise.all([first, second])

    assert.equal(created[0].calls.filter((c) => c === 'quit').length, 1, 'one teardown, however many callers')
  })

  // Probed against ioredis 6: on a give-up the driver emits close→end in the
  // SAME synchronous stack, and on a flap close→reconnecting likewise. A
  // supervisor reconnecting from its 'close' handler therefore acts at the
  // one instant the two are indistinguishable. The manager defers that
  // decision one turn, by which point the status says which one it was.
  test('a connect() issued from the close handler of a give-up starts fresh', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    const dying = created[0]

    // The supervisor: reconnect the moment the connection reports closed.
    let reconnect = null
    const supervisor = () => { reconnect = manager.connect() }

    // The give-up cascade, exactly as the driver produces it: status flips
    // and events fire in one synchronous stack.
    dying.status = 'close'
    supervisor()
    dying.emit('close')
    dying.status = 'end'
    dying.emit('end')

    await reconnect

    assert.equal(created.length, 2, 'the supervisor must get a fresh cycle, not the corpse')
    assert.equal(manager.client, created[1])
    assert.equal(manager.isConnected, true)
    assert.deepEqual(events.map(([name]) => name), ['ready', 'close', 'end', 'ready'])
  })

  test('a connect() during a mere flap keeps the flapping client', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    const flapping = created[0]

    // The flap cascade: close→reconnecting in one stack, the driver keeps
    // retrying on the SAME client — building a second one would duplicate it.
    let reconnect = null
    flapping.status = 'close'
    reconnect = manager.connect()
    flapping.emit('close')
    flapping.status = 'reconnecting'
    flapping.emit('reconnecting', 50)

    await reconnect

    assert.equal(created.length, 1, 'the driver owns the retry — no second client')
    assert.equal(manager.client, flapping)
  })

  test('connect() refuses to reuse a client the driver already ended', async () => {
    const { manager, created } = createManager()

    await manager.connect()

    // The give-up path can leave #client set with status 'end' for the tick
    // between the status flip and the 'end' handler running.
    created[0].status = 'end'

    await manager.connect()

    assert.equal(created.length, 2, 'an ended client is never "reused"')
    assert.equal(manager.client, created[1])
    assert.equal(created[0].listenerCount('end'), 0, 'the corpse must be released, not just replaced')
  })

  test('reconnecting is reported even when the driver omits the delay', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    created[0].emit('reconnecting')

    assert.deepEqual(events.at(-1), ['reconnecting', undefined])
  })

  test('disconnect skips quit for an already ended client', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    created[0].status = 'end'
    created[0].calls.length = 0

    await manager.disconnect()

    assert.deepEqual(created[0].calls, [])
    assert.equal(manager.client, null)
  })

  test('disconnecting an already ended client schedules no wait at all', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].status = 'end'

    await manager.disconnect()

    assert.deepEqual(clock.delays(), [], 'there is no end event left to wait for')
  })
})

// The facade must wire the real collaborators together: these run with a fake
// driver but no fake collaborators, so a broken constructor is caught.
describe('facade wiring', () => {
  const createFacade = (options = {}) => {
    const redis = new RedisClient({ logger: quietLogger, ...options })
    const driver = createDriverClient()

    driver.ping = async () => 'PONG'
    redis.connection.redisConfig = { createRedisClient: () => driver }

    return { redis, driver }
  }

  // Review finding: disconnect() never touched the HealthChecker, and an
  // in-flight probe's timer is deliberately ref'd (it is awaited) — a PING
  // that would never be answered kept the loop alive for up to
  // healthCheckTimeout after disconnect() resolved.
  test('disconnect() cancels an in-flight health probe', async () => {
    const clock = createManualClock()
    const { redis, driver } = createFacade({ clock, healthCheckTimeout: 30000 })

    // A wedged server: the PING never settles on its own.
    driver.ping = () => new Promise(() => {})

    await redis.connect()

    const probe = redis.checkHealth()
    await redis.disconnect()

    assert.equal(await probe, false, 'the cancelled probe settles as unhealthy')
    assert.equal(clock.pending(), 0, 'and its 30s timer must not survive disconnect()')
  })

  // The subscriber runs on its own connection, so its traffic has to be
  // bridged onto the facade explicitly. Without this, redis.on('message')
  // stays silent while the handler passed to subscribe() still fires — half
  // the documented API working is worse than none of it.
  test('subscriber traffic reaches the facade events too', async () => {
    const { redis, driver } = createFacade()
    const subscriber = new EventEmitter()

    subscriber.status = 'ready'
    subscriber.subscribe = async () => 1
    subscriber.psubscribe = async () => 1
    driver.duplicate = () => subscriber

    await redis.connect()

    const seen = []
    redis.on('message', (...args) => seen.push(['message', ...args]))
    redis.on('pmessage', (...args) => seen.push(['pmessage', ...args]))
    redis.on('connectionError', (err) => seen.push(['connectionError', err.message]))

    await redis.subscribe('news')
    await redis.psubscribe('logs.*')

    subscriber.emit('message', 'news', 'hello')
    subscriber.emit('pmessage', 'logs.*', 'logs.app', 'entry')
    subscriber.emit('error', new Error('subscriber socket reset'))

    assert.deepEqual(seen, [
      ['message', 'news', 'hello'],
      ['pmessage', 'logs.*', 'logs.app', 'entry'],
      ['connectionError', 'subscriber socket reset']
    ])
  })

  // The library logs through whatever the application injects; the built-in
  // console logger only exists so the out-of-the-box experience still has
  // visible logs. Omitting the option must land on it, not on undefined.
  test('without a logger the client falls back to the built-in one', () => {
    const redis = new RedisClient({ host: 'h', port: 6379 })

    for (const level of ['error', 'warn', 'info', 'debug']) {
      assert.equal(typeof redis.logger[level], 'function', `the fallback logger must expose ${level}()`)
    }
  })

  // Drivers do not always emit an Error: a bare string or a plain object must
  // still produce a readable line instead of "undefined".
  test('a non-Error failure is still logged and re-emitted', async () => {
    const logged = []
    const { redis, driver } = createFacade({
      logger: { ...quietLogger, error: (message) => logged.push(message) }
    })

    await redis.connect()

    const seen = []
    redis.on('connectionError', (err) => seen.push(err))

    driver.emit('error', 'ECONNRESET without an Error wrapper')

    assert.deepEqual(seen, ['ECONNRESET without an Error wrapper'])
    assert.match(logged.at(-1), /ECONNRESET without an Error wrapper/)
  })

  test('driver events are re-emitted by the client itself', async () => {
    const { redis, driver } = createFacade()
    const seen = []

    for (const event of ['ready', 'close', 'reconnecting', 'connectionError', 'end']) {
      redis.on(event, () => seen.push(event))
    }

    await redis.connect()
    driver.emit('close')
    driver.emit('reconnecting', 100)
    driver.emit('error', new Error('socket reset'))
    driver.emit('end')

    assert.deepEqual(seen, ['ready', 'close', 'reconnecting', 'connectionError', 'end'])
  })

  test('the health checker reads the live connection', async () => {
    const { redis } = createFacade()

    assert.equal(await redis.checkHealth(), false, 'no connection means unhealthy')

    await redis.connect()
    assert.equal(await redis.checkHealth(), true)
  })

  test('constructor defaults reach the collaborators', () => {
    const { redis } = createFacade()

    assert.equal(redis.keyPrefix, '', 'no prefix by default')
    assert.equal(redis.redisConfig.maxRetryAttempts, Infinity)
    assert.equal(redis.redisConfig.baseRetryDelay, 1000)
    assert.equal(redis.redisConfig.maxRetryDelay, 30000)
    assert.equal(redis.health.interval, 5000)
    assert.equal(redis.health.timeout, 1000)
    assert.equal(redis.connection.logger, redis.logger, 'collaborators share the injected logger')
    assert.equal(redis.subscriptions.logger, redis.logger)
    assert.equal(redis.locks.logger, redis.logger)
  })

  test('constructor options override every default', () => {
    const { redis } = createFacade({
      keyPrefix: 'app:',
      maxRetryAttempts: 3,
      baseRetryDelay: 25,
      maxRetryDelay: 250,
      healthCheckInterval: 111,
      healthCheckTimeout: 222
    })

    assert.equal(redis.keyPrefix, 'app:')
    assert.equal(redis.redisConfig.maxRetryAttempts, 3)
    assert.equal(redis.redisConfig.baseRetryDelay, 25)
    assert.equal(redis.redisConfig.maxRetryDelay, 250)
    assert.equal(redis.health.interval, 111)
    assert.equal(redis.health.timeout, 222)
  })

  test('zero is honored for every numeric option', () => {
    const { redis } = createFacade({
      maxRetryAttempts: 0,
      baseRetryDelay: 0,
      maxRetryDelay: 0,
      healthCheckInterval: 0,
      healthCheckTimeout: 0
    })

    assert.equal(redis.redisConfig.maxRetryAttempts, 0)
    assert.equal(redis.redisConfig.baseRetryDelay, 0)
    assert.equal(redis.redisConfig.maxRetryDelay, 0)
    assert.equal(redis.health.interval, 0)
    assert.equal(redis.health.timeout, 0)
  })
})

import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { describe, test } from 'node:test'
import ConnectionManager from '../src/connection/manager.js'
import { RedisClient } from '../src/index.js'

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
  client.quit = async () => {
    client.calls.push('quit')

    if (quitFails) throw new Error('quit failed')

    client.status = 'end'
    // The driver emits 'end' asynchronously after quit resolves.
    setImmediate(() => client.emit('end'))
    return 'OK'
  }
  client.disconnect = () => {
    client.calls.push('disconnect')
    client.status = 'end'
    setImmediate(() => client.emit('end'))
  }

  return client
}

const createManager = (clientOptions = {}) => {
  const created = []
  const events = []

  const manager = new ConnectionManager({
    redisConfig: {
      createRedisClient: () => {
        const client = createDriverClient(clientOptions)
        created.push(client)
        return client
      }
    },
    logger: quietLogger,
    emit: (...args) => events.push(args)
  })

  return { manager, created, events }
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

  test('disconnect resolves promptly instead of waiting on its escape timer', async () => {
    const { manager } = createManager()

    await manager.connect()
    const started = Date.now()
    await manager.disconnect()

    assert.ok(Date.now() - started < 500, 'shutdown must react to the end event, not to the 2s fallback')
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

  test('disconnecting an already ended client does not wait either', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    created[0].status = 'end'

    const started = Date.now()
    await manager.disconnect()

    assert.ok(Date.now() - started < 500, 'an ended client has no end event left to wait for')
  })
})

// The facade must wire the real collaborators together: these run with a fake
// driver but no fake collaborators, so a broken constructor is caught.
describe('facade wiring', () => {
  const createFacade = (options = {}) => {
    const redis = new RedisClient({ logger: quietLogger, ...options })
    const driver = createDriverClient()

    driver.ping = (callback) => callback(null, 'PONG')
    redis.connection.redisConfig = { createRedisClient: () => driver }

    return { redis, driver }
  }

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

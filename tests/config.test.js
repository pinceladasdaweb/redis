import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import RedisConfig from '../src/connection/config.js'

const quietLogger = { info () {}, warn () {}, error () {}, debug () {} }

describe('redis config', () => {
  test('sentinel options are only forwarded when configured', () => {
    const standalone = new RedisConfig({ host: 'h', port: 6379, logger: quietLogger })

    assert.equal('sentinels' in standalone.getOptions(), false)
    assert.equal('name' in standalone.getOptions(), false)

    const sentinel = new RedisConfig({
      logger: quietLogger,
      sentinels: [{ host: 's1', port: 26379 }, { host: 's2', port: 26379 }],
      name: 'mymaster',
      sentinelPassword: 'secret',
      role: 'slave'
    })
    const options = sentinel.getOptions()

    assert.deepEqual(options.sentinels, [{ host: 's1', port: 26379 }, { host: 's2', port: 26379 }])
    assert.equal(options.name, 'mymaster')
    assert.equal(options.sentinelPassword, 'secret')
    assert.equal(options.role, 'slave')
  })

  // Regression: an allowlist used to drop every option it did not know,
  // silently including tls — which meant no managed Redis (Upstash,
  // ElastiCache in-transit, Azure Cache) could be reached at all.
  test('forwards every driver option, including the ones added after this code', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      host: 'cache.upstash.io',
      port: 6380,
      tls: { servername: 'cache.upstash.io' },
      family: 6,
      keepAlive: 5000,
      path: '/tmp/redis.sock',
      enableOfflineQueue: false,
      natMap: { '10.0.0.1:6379': { host: '203.0.113.1', port: 6379 } },
      anOptionInventedTomorrow: 'passes through'
    }).getOptions()

    assert.deepEqual(options.tls, { servername: 'cache.upstash.io' })
    assert.equal(options.family, 6)
    assert.equal(options.keepAlive, 5000)
    assert.equal(options.path, '/tmp/redis.sock')
    assert.equal(options.enableOfflineQueue, false)
    assert.deepEqual(options.natMap, { '10.0.0.1:6379': { host: '203.0.113.1', port: 6379 } })
    assert.equal(options.anOptionInventedTomorrow, 'passes through')
  })

  test('keeps the library options to itself', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      clock: {},
      maxRetryAttempts: 5,
      baseRetryDelay: 10,
      maxRetryDelay: 100,
      healthCheckInterval: 1000,
      healthCheckTimeout: 100
    }).getOptions()

    for (const name of ['logger', 'clock', 'maxRetryAttempts', 'baseRetryDelay', 'maxRetryDelay', 'healthCheckInterval', 'healthCheckTimeout']) {
      assert.equal(name in options, false, `${name} is ours and must not reach the driver`)
    }
  })

  test('the reconnection hooks stay under library control', () => {
    const config = new RedisConfig({ logger: quietLogger })
    const options = config.getOptions()

    assert.equal(typeof options.retryStrategy, 'function')
    assert.equal(typeof options.reconnectOnError, 'function')

    for (const hook of ['retryStrategy', 'reconnectOnError']) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, [hook]: () => 1 }), {
        name: 'RedisClientError',
        code: 'INVALID_OPTION',
        operation: 'constructor'
      }, `overriding ${hook} would silently disable the documented retry policy`)
    }
  })

  test('rejects malformed numeric options at construction', () => {
    for (const [name, value] of [
      ['healthCheckTimeout', 'soon'],
      ['healthCheckInterval', -1],
      ['baseRetryDelay', NaN],
      ['maxRetryDelay', null],
      ['commandTimeout', -5],
      ['connectTimeout', '3000']
    ]) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, [name]: value }), {
        code: 'INVALID_OPTION'
      }, `${name}: ${JSON.stringify(value)} must not reach the driver`)
    }

    // Infinity is meaningful for the attempt limit, and zero is legitimate.
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: Infinity }))
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: 0, healthCheckInterval: 0 }))
  })

  test('sentinel mode demands the master group name', () => {
    assert.throws(() => new RedisConfig({ logger: quietLogger, sentinels: [{ host: 's', port: 26379 }] }), {
      code: 'INVALID_OPTION'
    }, 'sentinels without name resolves nothing')

    assert.doesNotThrow(() => new RedisConfig({
      logger: quietLogger,
      sentinels: [{ host: 's', port: 26379 }],
      name: 'mymaster'
    }))
  })

  test('driver defaults follow the documented table', () => {
    const options = new RedisConfig({ logger: quietLogger }).getOptions()

    assert.equal(options.maxRetriesPerRequest, null, 'null means unlimited retries per command')
    assert.equal(options.enableReadyCheck, true)
    assert.equal(options.autoResubscribe, true, 'pub/sub recovery depends on this')
    assert.equal(options.autoResendUnfulfilledCommands, true)
    assert.equal(options.lazyConnect, true)
  })

  test('every driver default can be turned off', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      maxRetriesPerRequest: 3,
      enableReadyCheck: false,
      autoResubscribe: false,
      autoResendUnfulfilledCommands: false,
      lazyConnect: false
    }).getOptions()

    assert.equal(options.maxRetriesPerRequest, 3)
    assert.equal(options.enableReadyCheck, false)
    assert.equal(options.autoResubscribe, false)
    assert.equal(options.autoResendUnfulfilledCommands, false)
    assert.equal(options.lazyConnect, false)
  })

  test('createRedisClient builds a lazy ioredis instance from the options', async () => {
    const logged = []
    const config = new RedisConfig({
      host: '198.51.100.10',
      port: 6379,
      keyPrefix: 'app:',
      logger: { ...quietLogger, info: (message) => logged.push(message) }
    })

    const client = config.createRedisClient()

    try {
      assert.equal(client.options.host, '198.51.100.10')
      assert.equal(client.options.keyPrefix, 'app:')
      assert.equal(client.options.lazyConnect, true)
      assert.equal(client.status, 'wait', 'lazyConnect must not open a socket')
      assert.match(logged[0], /Creating Redis client with host: 198\.51\.100\.10/)
    } finally {
      client.disconnect()
    }
  })

  test('reconnectOnError only reacts to a READONLY reply, and resends the command', () => {
    const config = new RedisConfig({ logger: quietLogger })

    assert.equal(config.reconnectOnError(new Error('READONLY You can\'t write against a read only replica.')), 2)
    assert.equal(config.reconnectOnError(new Error('ERR unknown command')), false)
    assert.equal(config.reconnectOnError(new Error('WRONGTYPE Operation against a key')), false)
    // Guards the startsWith check: the token must lead the reply, never just
    // appear somewhere inside it.
    assert.equal(config.reconnectOnError(new Error('ERR the value mentions READONLY somewhere')), false)
  })

  test('retryStrategy backs off exponentially up to the cap', () => {
    const config = new RedisConfig({
      logger: quietLogger,
      maxRetryAttempts: Infinity,
      baseRetryDelay: 100,
      maxRetryDelay: 500
    })

    assert.equal(config.retryStrategy(1), 200)
    assert.equal(config.retryStrategy(2), 400)
    assert.equal(config.retryStrategy(3), 500)
    assert.equal(config.retryStrategy(10), 500)
  })

  test('retryStrategy gives up once the attempt limit is exceeded', () => {
    const logged = []
    const config = new RedisConfig({
      logger: { ...quietLogger, error: (message) => logged.push(message) },
      maxRetryAttempts: 2,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    })

    assert.equal(typeof config.retryStrategy(2), 'number', 'the last allowed attempt still retries')
    assert.equal(config.retryStrategy(3), null, 'null tells ioredis to stop')
    assert.match(logged.at(-1), /Max retry attempts \(2\) reached/)
  })

  test('maxRetryAttempts: 0 disables retries entirely', () => {
    const config = new RedisConfig({
      logger: quietLogger,
      maxRetryAttempts: 0,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    })

    assert.equal(config.retryStrategy(1), null)
  })
})

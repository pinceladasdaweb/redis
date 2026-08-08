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
        code: 'INVALID_OPTION',
        operation: 'constructor'
      }, `${name}: ${JSON.stringify(value)} must not reach the driver`)
    }

    // Infinity is meaningful for the attempt limit, and zero is legitimate.
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: Infinity }))
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: 0, healthCheckInterval: 0 }))
  })

  test('sentinel mode demands the master group name', () => {
    assert.throws(() => new RedisConfig({ logger: quietLogger, sentinels: [{ host: 's', port: 26379 }] }), {
      code: 'INVALID_OPTION',
      operation: 'constructor'
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

  // The cluster option split is the whole reason this class knows about
  // clusters: ioredis reads cluster-level options off the top and node-level
  // ones out of redisOptions, and an option filed under the wrong one is
  // silently ignored — the exact failure this library removed for standalone.
  test('cluster options are split between the cluster and its nodes', () => {
    const config = new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }, { host: 'n2', port: 7002 }],
      keyPrefix: 'app:',
      password: 'secret',
      maxRedirections: 32,
      scaleReads: 'slave',
      connectTimeout: 250,
      maxRetryAttempts: Infinity,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    })
    const options = config.getOptions()

    assert.equal(config.isCluster, true)
    assert.deepEqual(config.nodes, [{ host: 'n1', port: 7001 }, { host: 'n2', port: 7002 }])

    // Cluster-level: read by the Cluster itself.
    assert.equal(options.maxRedirections, 32)
    assert.equal(options.scaleReads, 'slave')
    assert.equal(options.lazyConnect, true)
    assert.equal(typeof options.clusterRetryStrategy, 'function')

    // Node-level: read by each connection the cluster opens.
    assert.equal(options.redisOptions.password, 'secret')
    assert.equal(options.redisOptions.keyPrefix, 'app:')
    assert.equal(options.redisOptions.connectTimeout, 250)
    assert.equal(options.redisOptions.maxRetriesPerRequest, null)
    assert.equal(typeof options.redisOptions.retryStrategy, 'function')

    // And neither level may carry the other's options, or the one that
    // landed in the wrong place is dropped without a word.
    assert.equal('nodes' in options, false, 'startup nodes are a constructor argument, not an option')
    assert.equal('maxRedirections' in options.redisOptions, false)
    assert.equal('password' in options, false)

    // The backoff a cluster uses is the one this library documents.
    assert.equal(options.clusterRetryStrategy(1), 20, 'exponential backoff, same as a single node')
  })

  // Every name in the split is load-bearing. ioredis reads cluster-level
  // options off the top and node-level ones out of `redisOptions`, and an
  // option filed on the wrong side is not an error — it is silently ignored,
  // which is the exact failure this library already removed once for `tls`.
  // So each one gets asserted by name, not sampled.
  test('every cluster-level option lands at cluster level, and nothing else does', () => {
    const clusterLevel = {
      dnsLookup: () => {},
      enableOfflineQueue: false,
      enableReadyCheck: false,
      scaleReads: 'slave',
      maxRedirections: 32,
      retryDelayOnFailover: 111,
      retryDelayOnClusterDown: 222,
      retryDelayOnTryAgain: 333,
      retryDelayOnMoved: 444,
      slotsRefreshTimeout: 555,
      slotsRefreshInterval: 666,
      natMap: { '10.0.0.1:6379': { host: '127.0.0.1', port: 7001 } },
      enableAutoPipelining: true,
      lazyConnect: false
    }
    const nodeLevel = {
      password: 'secret',
      keyPrefix: 'app:',
      connectTimeout: 250,
      commandTimeout: 300,
      tls: {},
      family: 6,
      connectionName: 'worker-1'
    }

    const options = new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      ...clusterLevel,
      ...nodeLevel,
      maxRetryAttempts: Infinity,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    }).getOptions()

    for (const [name, value] of Object.entries(clusterLevel)) {
      assert.deepEqual(options[name], value, `${name} belongs to the cluster itself`)
      assert.equal(name in options.redisOptions, false, `${name} must NOT reach redisOptions`)
    }

    for (const [name, value] of Object.entries(nodeLevel)) {
      assert.deepEqual(options.redisOptions[name], value, `${name} describes each node`)
      assert.equal(name in options, false, `${name} must NOT sit at cluster level`)
    }

    // `nodes` is a constructor argument, never an option on either side.
    assert.equal('nodes' in options, false)
    assert.equal('nodes' in options.redisOptions, false)
  })

  test('the cluster keeps database 0 and rejects anything else', () => {
    assert.throws(() => new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      db: 3
    }), { code: 'INVALID_OPTION', operation: 'constructor', message: /only supports database 0 \(got 3\)/ })

    // Zero is the one valid value, and must not be read as "not provided".
    assert.doesNotThrow(() => new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      db: 0
    }))
  })

  test('startup nodes must be a non-empty array', () => {
    for (const nodes of [[], 'n1:7001', {}, null]) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, nodes }), {
        code: 'INVALID_OPTION',
        operation: 'constructor',
        message: /non-empty array of startup nodes/
      }, `nodes: ${JSON.stringify(nodes)} must be refused`)
    }
  })

  test('cluster and sentinel are different topologies and cannot be combined', () => {
    assert.throws(() => new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      sentinels: [{ host: 's1', port: 26379 }],
      name: 'mymaster'
    }), { code: 'INVALID_OPTION', operation: 'constructor', message: /different topologies/ })
  })

  // Construction only: lazyConnect means no socket is opened here, so this
  // asserts which driver class is built without needing a live cluster.
  test('createRedisClient builds a Cluster for nodes and a Redis otherwise', async () => {
    const { default: Redis } = await import('ioredis')
    const announced = []
    const logger = { ...quietLogger, info: (message) => announced.push(message) }

    const cluster = new RedisConfig({
      logger,
      nodes: [{ host: '127.0.0.1', port: 7001 }, { host: '127.0.0.1', port: 7002 }]
    }).createRedisClient()

    assert.ok(cluster instanceof Redis.Cluster, 'nodes must produce a cluster client')
    assert.match(announced.at(-1), /cluster client with 2 startup node\(s\)/)
    cluster.disconnect()

    const standalone = new RedisConfig({ logger, host: '127.0.0.1', port: 6379 }).createRedisClient()

    assert.ok(standalone instanceof Redis, 'no nodes must produce a plain client')
    assert.equal(standalone instanceof Redis.Cluster, false)
    assert.match(announced.at(-1), /client with host: 127\.0\.0\.1/)
    standalone.disconnect()
  })
})

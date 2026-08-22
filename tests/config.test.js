import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
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

    for (const hook of ['retryStrategy', 'reconnectOnError', 'clusterRetryStrategy', 'clusterNodeRetryStrategy']) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, [hook]: () => 1 }), {
        name: 'RedisClientError',
        code: 'INVALID_OPTION',
        operation: 'constructor',
        // The refusal has to say what to use instead, or it is a dead end for
        // whoever hit it.
        message: /maxRetryAttempts, baseRetryDelay and maxRetryDelay/
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
      ['connectTimeout', '3000'],
      // The one option that ACCEPTS Infinity still has to reject everything
      // else: its exemption is for that value, not from validation.
      ['maxRetryAttempts', -1],
      ['maxRetryAttempts', 'many']
    ]) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, [name]: value }), {
        code: 'INVALID_OPTION',
        operation: 'constructor'
      }, `${name}: ${JSON.stringify(value)} must not reach the driver`)
    }

    // The value has to be readable in the message: JSON.stringify renders
    // Infinity and NaN alike as "null", so numbers are shown with String()
    // and everything else quoted as itself.
    assert.throws(() => new RedisConfig({ logger: quietLogger, healthCheckTimeout: 'soon' }), {
      message: /got "soon"\)\.$/
    }, 'a non-number must be quoted, so "soon" is not read as a variable name')

    assert.throws(() => new RedisConfig({ logger: quietLogger, healthCheckTimeout: NaN }), {
      message: /got NaN\)\.$/
    }, 'NaN must be named, never rendered as null')

    // Infinity is meaningful for the attempt limit, and zero is legitimate.
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: Infinity }))
    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: 0, healthCheckInterval: 0 }))
  })

  // Review finding: Infinity used to be waved through for ALL of them, and it
  // only means anything for an attempt COUNT. Every other name is a delay or a
  // timeout that ends up in a timer, where Node clamps an out-of-range value to
  // 1ms — so `commandTimeout: Infinity` written for "no timeout" failed every
  // command after 1ms, and `maxRetryDelay: Infinity` reconnected in a hot loop.
  // The tightest possible limit is not a defensible reading of "no limit".
  test('Infinity is only meaningful for the attempt limit', () => {
    for (const name of ['baseRetryDelay', 'maxRetryDelay', 'healthCheckInterval', 'healthCheckTimeout', 'commandTimeout', 'connectTimeout']) {
      assert.throws(() => new RedisConfig({ logger: quietLogger, [name]: Infinity }), {
        code: 'INVALID_OPTION',
        operation: 'constructor',
        // JSON.stringify renders Infinity as "null", which is the one thing
        // the reader must not be told here.
        message: /got Infinity.*clamps it to 1ms/
      }, `${name}: Infinity must not reach a timer`)
    }

    // ...and that explanation belongs ONLY to Infinity. On an ordinary typo it
    // would send the reader chasing a timer that has nothing to do with it.
    assert.throws(() => new RedisConfig({ logger: quietLogger, commandTimeout: -5 }), {
      message: /^commandTimeout must be a finite non-negative number \(got -5\)\.$/
    })

    assert.doesNotThrow(() => new RedisConfig({ logger: quietLogger, maxRetryAttempts: Infinity }))
  })

  // Review finding: this library parses replies positionally in their
  // RESP2-compatible shape — CONFIG GET as a flat array, WITHSCORES as
  // alternating member/score. Under ioredis 6 that shape survives RESP3 only
  // because the default reply mapping flattens maps and doubles, so asking for
  // the 'resp3' mapping turns CONFIG GET into an object and breaks
  // keyspaceNotifications() from underneath, with no error pointing at why.
  test('options this library parses against cannot be swapped out', () => {
    assert.throws(() => new RedisConfig({ logger: quietLogger, replyMapping: 'resp3' }), {
      code: 'INVALID_OPTION',
      operation: 'constructor',
      message: /RESP2-compatible shape/
    })

    // And redisOptions is built here from the cluster split: a caller-supplied
    // one is filed as a node-level option and ends up nested inside the real
    // one, where nothing ever reads it.
    assert.throws(() => new RedisConfig({ logger: quietLogger, nodes: [{ host: 'n1', port: 7001 }], redisOptions: { password: 'x' } }), {
      code: 'INVALID_OPTION',
      operation: 'constructor',
      message: /cluster option split/
    })
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

  // An option handed over as an explicit `undefined` — the shape every
  // `{ lazyConnect: opts.lazyConnect }` produces when the caller did not set it
  // — must not overwrite the default with nothing. Spreading it would leave
  // `lazyConnect: undefined`, which ioredis then reads as its OWN default
  // (false), quietly opening a socket at construction.
  test('an option passed as undefined leaves the default standing', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      lazyConnect: undefined,
      enableReadyCheck: undefined,
      maxRetriesPerRequest: undefined,
      autoResubscribe: undefined,
      host: undefined
    }).getOptions()

    assert.equal(options.lazyConnect, true)
    assert.equal(options.enableReadyCheck, true)
    assert.equal(options.maxRetriesPerRequest, null)
    assert.equal(options.autoResubscribe, true)
    assert.equal('host' in options, false, 'and an undefined passthrough never reaches the driver at all')
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
    assert.equal(typeof options.redisOptions.reconnectOnError, 'function')

    // And neither level may carry the other's options, or the one that
    // landed in the wrong place is dropped without a word.
    assert.equal('nodes' in options, false, 'startup nodes are a constructor argument, not an option')
    assert.equal('maxRedirections' in options.redisOptions, false)
    assert.equal('password' in options, false)

    // The backoff a cluster uses is the one this library documents.
    assert.equal(options.clusterRetryStrategy(1), 20, 'exponential backoff, same as a single node')
  })

  // Review finding: the documented backoff reached the Cluster object and
  // nothing else. ioredis's ConnectionPool sets `retryStrategy` on every node
  // connection itself — from `clusterNodeRetryStrategy`, which defaults to
  // `null` — and merges redisOptions in with lodash `defaults`, which never
  // overwrites a key already present. So the retryStrategy this library filed
  // under redisOptions was shadowed on every path, and cluster node
  // connections did not reconnect at all. Probed against a live three-master
  // cluster (22/08/2026): `nodes('master')[0].options.retryStrategy` was
  // `null`, and so was `duplicate().options.retryStrategy` — which is what a
  // keyspace-event subscriber is built from.
  test('the documented backoff reaches the node connections, not just the cluster', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      maxRetryAttempts: Infinity,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    }).getOptions()

    assert.equal(typeof options.clusterNodeRetryStrategy, 'function', 'per-node reconnection must be configured')
    assert.equal(options.clusterNodeRetryStrategy(1), 20, 'and use the same backoff as everything else')

    // And it is NOT left under redisOptions, where ioredis's own type omits it
    // and its ConnectionPool shadows it — carrying it there says the policy
    // applies when it does not.
    assert.equal(
      'retryStrategy' in options.redisOptions,
      false,
      'a shadowed option under redisOptions is a claim the driver never honours'
    )
  })

  test('maxRetryAttempts: 0 disables node reconnection too', () => {
    const options = new RedisConfig({
      logger: quietLogger,
      nodes: [{ host: 'n1', port: 7001 }],
      maxRetryAttempts: 0,
      baseRetryDelay: 10,
      maxRetryDelay: 100
    }).getOptions()

    assert.equal(options.clusterNodeRetryStrategy(1), null, 'the attempt limit governs every connection this library opens')
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

  // Review finding: the split is a hand-kept list against a dependency that
  // grows its own, and ioredis 6 had already added useSRVRecords, resolveSrv,
  // shardedSubscribers and autoPipeliningIgnoredCommands without this list
  // hearing about it. Filed under redisOptions they are not an error — they are
  // silence, the same failure this library removed once for `tls`. So the list
  // is checked against ioredis's OWN declaration, and the next dependency major
  // that adds a cluster-level option fails here instead of in production.
  test('the cluster split covers every option ioredis reads at cluster level', async () => {
    const source = await readFile(new URL('../node_modules/ioredis/built/cluster/ClusterOptions.d.ts', import.meta.url), 'utf8')
    const start = source.indexOf('export interface ClusterOptions')

    assert.notEqual(start, -1, 'ioredis must still declare ClusterOptions where this test looks')

    // Its own declarations only — the interface ends at the first unindented
    // closing brace, so the types that follow it are not swept in.
    const body = source.slice(start, source.indexOf('\n}', start))
    const declared = [...body.matchAll(/^ {4}(\w+)\??:/gm)].map(([, name]) => name)

    assert.ok(declared.length > 15, `the parse must find the options, not a handful (${declared.length})`)

    // The ones this library owns rather than forwards: it BUILDS redisOptions
    // from the split, and both retry strategies are the documented backoff.
    const owned = new Set(['redisOptions', 'clusterRetryStrategy', 'clusterNodeRetryStrategy'])

    for (const name of declared) {
      if (owned.has(name)) {
        assert.throws(() => new RedisConfig({ logger: quietLogger, nodes: [{ host: 'n1', port: 7001 }], [name]: 'anything' }), {
          code: 'INVALID_OPTION'
        }, `${name} is this library's to set, and overriding it must be refused`)

        continue
      }

      const options = new RedisConfig({
        logger: quietLogger,
        nodes: [{ host: 'n1', port: 7001 }],
        [name]: 'sentinel',
        maxRetryAttempts: Infinity,
        baseRetryDelay: 10,
        maxRetryDelay: 100
      }).getOptions()

      assert.equal(options[name], 'sentinel', `ioredis reads ${name} at cluster level — it must land there`)
      assert.equal(name in options.redisOptions, false, `${name} under redisOptions is silently ignored`)
    }
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

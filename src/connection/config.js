import Redis from 'ioredis'
import RedisClientError from '../utils/errors.js'

// Options this library owns: they are consumed here (or by the facade) and
// must never reach the driver, which would reject or misread them.
const LIBRARY_OPTIONS = new Set([
  'logger',
  'clock',
  'maxRetryAttempts',
  'baseRetryDelay',
  'maxRetryDelay',
  'healthCheckInterval',
  'healthCheckTimeout'
])

// Options the library will not let a caller replace, each with the reason —
// every one of them is something this library's own correctness rests on, and
// overriding it fails silently rather than loudly.
const RETRY_IS_OURS = 'reconnection belongs to this library — use maxRetryAttempts, baseRetryDelay and maxRetryDelay instead'
const RESERVED_OPTIONS = new Map([
  ['retryStrategy', RETRY_IS_OURS],
  ['reconnectOnError', RETRY_IS_OURS],
  ['clusterRetryStrategy', RETRY_IS_OURS],
  ['clusterNodeRetryStrategy', RETRY_IS_OURS],
  // The facade parses replies positionally in their RESP2-compatible shape:
  // CONFIG GET as a flat array, WITHSCORES as alternating member/score. Under
  // ioredis 6 that shape survives RESP3 only because the default reply mapping
  // flattens maps and doubles; asking for the 'resp3' mapping turns CONFIG GET
  // into an object and breaks keyspaceNotifications() from underneath.
  ['replyMapping', "this library parses replies in their RESP2-compatible shape, which the 'resp3' mapping changes underneath it"],
  // Built here from the flat option list (see the cluster split below). A
  // caller-supplied one would be filed as a node-level option and end up
  // nested inside the real one, where nothing ever reads it.
  ['redisOptions', 'this library builds it from the cluster option split — pass node options at the top level'],
  // Every integer reply becomes a string: deleteByPattern's running total
  // would concatenate ("011…") and every counter-returning wrapper would
  // hand back text.
  ['stringNumbers', 'this library sums and compares the integers Redis returns']
])

// In cluster mode ioredis splits its options in two: these belong to the
// cluster itself, everything else describes each node and travels under
// `redisOptions`. Getting the split wrong means an option is silently
// ignored — exactly the failure this library just removed for standalone.
//
// The list is checked against ioredis's own ClusterOptions by a test, because
// a dependency major can add a cluster-level option without ever mentioning
// this library — and the symptom is silence, not an error.
const CLUSTER_LEVEL_OPTIONS = new Set([
  'dnsLookup',
  'enableOfflineQueue',
  'enableReadyCheck',
  'scaleReads',
  'maxRedirections',
  'retryDelayOnFailover',
  'retryDelayOnClusterDown',
  'retryDelayOnTryAgain',
  'retryDelayOnMoved',
  'slotsRefreshTimeout',
  'slotsRefreshInterval',
  'natMap',
  'enableAutoPipelining',
  'autoPipeliningIgnoredCommands',
  'lazyConnect',
  'useSRVRecords',
  'resolveSrv',
  'shardedSubscribers',
  'scripts',
  'himportFieldsets'
])

// Numbers that must be finite and non-negative if given at all. A typo here
// used to travel all the way to a hung timer or an infinite retry loop.
const NON_NEGATIVE_NUMBERS = [
  'maxRetryAttempts',
  'baseRetryDelay',
  'maxRetryDelay',
  'healthCheckInterval',
  'healthCheckTimeout',
  'commandTimeout',
  'connectTimeout'
]

// Infinity means "never give up" for an attempt COUNT and nothing sane for
// anything else: every other name above is a delay or a timeout that ends up
// in a timer, and Node clamps an out-of-range delay to 1ms. Waving Infinity
// through for those turns "no limit" into the tightest limit there is —
// commandTimeout: Infinity failing every command after 1ms, maxRetryDelay:
// Infinity reconnecting in a hot loop.
const ALLOWS_INFINITY = new Set(['maxRetryAttempts'])

// Timeouts for which 0 is a 0ms timer, not "disabled" (see #assertValid).
const ZERO_MEANS_INSTANT = new Set(['commandTimeout', 'healthCheckTimeout'])

class RedisConfig {
  constructor (options = {}) {
    this.logger = options.logger

    this.#assertValid(options)

    // Everything the caller passes reaches ioredis untouched — tls, family,
    // connectTimeout, keepAlive, path, natMap, enableOfflineQueue and whatever
    // the driver grows next. An allowlist here silently dropped tls, which
    // meant no managed Redis (Upstash, ElastiCache in-transit, Azure) could be
    // reached at all, with no error pointing at the cause.
    const passthrough = Object.fromEntries(
      Object.entries(options).filter(([key, value]) =>
        value !== undefined && !LIBRARY_OPTIONS.has(key) && !RESERVED_OPTIONS.has(key))
    )

    const defaults = {
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      autoResubscribe: true,
      autoResendUnfulfilledCommands: true,
      lazyConnect: true
    }

    const settings = {
      // Defaults first, so a caller can override any of them...
      ...defaults,
      ...passthrough,
      // ...except these, which the library owns.
      retryStrategy: this.retryStrategy.bind(this),
      reconnectOnError: this.reconnectOnError.bind(this)
    }

    // A cluster follows MOVED/ASK redirections and rediscovers slots on its
    // own; what it needs from us is the option split and the same backoff.
    this.nodes = options.nodes ?? null
    this.isCluster = this.nodes !== null

    if (this.isCluster) {
      const clusterLevel = {}
      const nodeLevel = {}

      for (const [key, value] of Object.entries(settings)) {
        // Startup nodes are a constructor argument, never an option.
        if (key === 'nodes') continue

        // Not under redisOptions: ioredis's ConnectionPool stamps every node
        // connection with its own retryStrategy BEFORE merging redisOptions
        // in, and lodash `defaults` never overwrites a key already present —
        // so a retryStrategy filed here is shadowed on every path, and
        // carrying it would claim a policy the driver never applies. ioredis's
        // own type Omits it from redisOptions for exactly that reason.
        if (key === 'retryStrategy') continue

        ;(CLUSTER_LEVEL_OPTIONS.has(key) ? clusterLevel : nodeLevel)[key] = value
      }

      // Per-node reconnection (clusterNodeRetryStrategy) is deliberately left
      // at the driver's default of NONE, and the option is reserved so nobody
      // turns it on by accident. The pool notices a dead master only when its
      // connection ENDS: that removes the node, rejects what it held with
      // CONNECTION_CLOSED, and triggers the slots refresh that finds the
      // promoted replica. Probed against ioredis 6: with a retry policy on the
      // nodes, a dead master's connection sat in 'reconnecting' forever, no
      // '-node' ever fired, no refresh ever ran (there is no periodic one by
      // default), the Cluster stayed 'ready', and every command to that shard
      // hung in its offline queue — a failover became a permanent hang. The
      // one place the library wants reconnection on a node-derived socket is
      // the keyspace-event subscribers, and they get it as a duplicate()
      // override (see SubscriptionManager), which the pool never sees.
      this.configOptions = {
        ...clusterLevel,
        clusterRetryStrategy: this.retryStrategy.bind(this),
        redisOptions: nodeLevel
      }
    } else {
      this.configOptions = settings
    }

    // Read straight from the caller: under cluster these live inside
    // redisOptions, and the facade should not have to know that.
    this.keyPrefix = options.keyPrefix ?? ''
    this.db = options.db ?? 0
    // A connection the caller pointed at a replica on purpose (Sentinel
    // `role: 'slave'`, or the driver's `readOnly`) is SUPPOSED to answer
    // READONLY to a write; see reconnectOnError.
    this.readOnly = options.role === 'slave' || options.readOnly === true

    this.maxRetryAttempts = options.maxRetryAttempts
    this.baseRetryDelay = options.baseRetryDelay
    this.maxRetryDelay = options.maxRetryDelay
  }

  // Fail at construction, not at the first command under load.
  #assertValid (options) {
    for (const name of NON_NEGATIVE_NUMBERS) {
      const value = options[name]

      if (value === undefined || (value === Infinity && ALLOWS_INFINITY.has(name))) {
        continue
      }

      if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        // JSON.stringify renders Infinity and NaN alike as "null", which is
        // the one thing the reader must not be told here.
        const shown = typeof value === 'number' ? String(value) : JSON.stringify(value)
        const trap = value === Infinity
          ? ' — Infinity reaches a timer, where Node clamps it to 1ms; omit the option to leave it unbounded'
          : ''

        throw new RedisClientError(
          `${name} must be a finite non-negative number (got ${shown})${trap}.`,
          'constructor',
          'INVALID_OPTION'
        )
      }

      // Zero is the other end of the same trap. For a TIMEOUT it is not
      // "none": ioredis's Command.setTimeout has no `ms > 0` guard, so
      // commandTimeout: 0 arms a 0ms timer that beats every socket reply and
      // fails every command, and healthCheckTimeout: 0 does the same to the
      // PING. (connectTimeout: 0 IS "none" — the driver checks truthiness
      // there — and 0 is a legitimate interval or delay elsewhere.)
      if (value === 0 && ZERO_MEANS_INSTANT.has(name)) {
        throw new RedisClientError(
          `${name}: 0 is not "no timeout" — it is a 0ms timer that fires before any reply can arrive. Omit the option to leave it unbounded.`,
          'constructor',
          'INVALID_OPTION'
        )
      }
    }

    for (const [name, reason] of RESERVED_OPTIONS) {
      if (options[name] !== undefined) {
        throw new RedisClientError(
          `${name} is managed by this library and cannot be overridden: ${reason}.`,
          'constructor',
          'INVALID_OPTION'
        )
      }
    }

    if (options.sentinels !== undefined && !options.name) {
      throw new RedisClientError(
        'sentinels requires name: the master group to resolve.',
        'constructor',
        'INVALID_OPTION'
      )
    }

    if (options.nodes !== undefined) {
      if (!Array.isArray(options.nodes) || options.nodes.length === 0) {
        throw new RedisClientError(
          'nodes must be a non-empty array of startup nodes ({ host, port }).',
          'constructor',
          'INVALID_OPTION'
        )
      }

      if (options.sentinels !== undefined) {
        throw new RedisClientError(
          'nodes and sentinels are different topologies — configure one or the other.',
          'constructor',
          'INVALID_OPTION'
        )
      }

      // Redis Cluster only has database 0; asking for another one would
      // silently read from the wrong place.
      if (options.db) {
        throw new RedisClientError(
          `Redis Cluster only supports database 0 (got ${options.db}).`,
          'constructor',
          'INVALID_OPTION'
        )
      }
    }
  }

  getOptions () {
    return this.configOptions
  }

  createRedisClient () {
    if (this.isCluster) {
      this.logger.info(`Creating Redis cluster client with ${this.nodes.length} startup node(s)`)

      return new Redis.Cluster(this.nodes, this.configOptions)
    }

    this.logger.info(`Creating Redis client with host: ${this.configOptions.host}`)

    return new Redis(this.configOptions)
  }

  retryStrategy (times) {
    if (this.maxRetryAttempts !== Infinity && times > this.maxRetryAttempts) {
      this.logger.error(`Max retry attempts (${this.maxRetryAttempts}) reached. Stopping retry.`)
      return null
    }

    const delay = Math.min(
      Math.pow(2, times) * this.baseRetryDelay,
      this.maxRetryDelay
    )

    this.logger.info(`Redis reconnection attempt ${times}. Trying again in ${delay}ms...`)
    return delay
  }

  // Called by ioredis for COMMAND errors only (server error replies) —
  // socket errors like ECONNREFUSED/ENOTFOUND never reach here; those belong
  // to retryStrategy (the previous branches for them were dead code). Redis
  // error replies start with the error-code token, so startsWith is the
  // structured check (never match substrings mid-message).
  reconnectOnError (err) {
    if (err.message.startsWith('READONLY')) {
      // On a connection meant for a replica, READONLY is the correct answer
      // to a write, not a failover. Reconnecting would land on a replica
      // again, resend, get READONLY again — an infinite loop whose promise
      // never settled, and one the caller could not opt out of because this
      // hook is reserved. Let the error reach them.
      if (this.readOnly) {
        return false
      }

      this.logger.warn('READONLY error detected. Reconnecting to the new master and resending the command.')

      // 2 = reconnect AND resend the failed command once reconnected.
      return 2
    }

    return false
  }
}

export { RedisConfig }
export default RedisConfig

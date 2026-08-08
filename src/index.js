import { EventEmitter } from 'node:events'
import { createClock } from './utils/clock.js'
import LockManager from './resilience/lock.js'
import RedisConfig from './connection/config.js'
import RedisClientError from './utils/errors.js'
import HealthChecker from './connection/health.js'
import SubscriptionManager from './messaging/pubsub.js'
import ConnectionManager from './connection/manager.js'
import Logger, { createLogger } from './utils/logger.js'
import { parseScore, parseScoredMembers } from './utils/scores.js'
import scanKeyspace, { deletePattern } from './keyspace/scanner.js'

// Which notify-keyspace-events class each event needs, for the ones worth
// naming. Anything else is only checked for the 'E' (key-event) flag.
const KEY_EVENT_CLASSES = {
  expired: 'x',
  evicted: 'e',
  set: '$',
  del: 'g',
  rename_from: 'g',
  rename_to: 'g',
  expire: 'g',
  lpush: 'l',
  rpush: 'l',
  sadd: 's',
  hset: 'h',
  zadd: 'z',
  xadd: 't',
  new: 'n'
}

// How many idle connections the blocking-read pool keeps around. A consumer
// loop reuses one connection instead of paying a full handshake per iteration
// (a whole cluster pool per iteration, in cluster mode); the cap stops a
// concurrency spike from leaving sockets parked forever.
const MAX_IDLE_BLOCKING_CONNECTIONS = 4

// Thin facade: wires the collaborators together through a small context
// (logger, config, emit) and exposes the command surface. Mutable state is
// always reached through getters — never captured references.
class RedisClient extends EventEmitter {
  // Dedicated connections currently in use, so shutdown can reclaim them.
  #dedicated = new Set()
  // Connections a blocking read finished with, ready for the next one.
  #idleBlocking = []

  constructor (options = {}) {
    super()

    const retryConfig = {
      maxRetryAttempts: options.maxRetryAttempts ?? Infinity,
      baseRetryDelay: options.baseRetryDelay ?? 1000,
      maxRetryDelay: options.maxRetryDelay ?? 30000
    }

    this.logger = options.logger || Logger

    this.redisConfig = new RedisConfig({
      ...options,
      ...retryConfig,
      logger: this.logger
    })

    this.config = this.redisConfig.getOptions()
    // Read from the config object, not from the driver options: under cluster
    // these live inside redisOptions.
    this.keyPrefix = this.redisConfig.keyPrefix

    // One clock for the whole facade: every timer and every reading of "now"
    // in the collaborators goes through it, so time is drivable in tests.
    this.clock = options.clock ?? createClock()

    this.connection = new ConnectionManager({
      redisConfig: this.redisConfig,
      logger: this.logger,
      clock: this.clock,
      emit: (event, ...args) => this.emit(event, ...args)
    })

    this.health = new HealthChecker({
      getClient: () => this.connection.client,
      logger: this.logger,
      clock: this.clock,
      interval: options.healthCheckInterval ?? 5000,
      timeout: options.healthCheckTimeout ?? 1000
    })

    this.subscriptions = new SubscriptionManager({
      connection: this.connection,
      logger: this.logger,
      clock: this.clock,
      emit: (event, ...args) => this.emit(event, ...args)
    })

    this.locks = new LockManager({
      connection: this.connection,
      logger: this.logger,
      clock: this.clock
    })
  }

  get client () {
    return this.connection.client
  }

  get isConnected () {
    return this.connection.isConnected
  }

  async connect () {
    return this.connection.connect()
  }

  async disconnect () {
    await this.subscriptions.close()
    this.#releaseDedicatedConnections()

    return this.connection.disconnect()
  }

  // A blocking read parked on a dedicated connection would otherwise outlive
  // the client: its promise never settles and its socket keeps the process
  // alive, so a graceful shutdown never finishes. Idle pooled connections go
  // too — nothing may hold the loop open past disconnect().
  #releaseDedicatedConnections () {
    for (const held of this.#dedicated) {
      held.cancelled = true
      held.client.disconnect()
    }

    for (const client of this.#idleBlocking.splice(0)) {
      client.disconnect()
    }
  }

  async checkHealth () {
    return this.health.check()
  }

  // Escape hatch for anything that must not share the main connection:
  // WATCH/MULTI/EXEC transactions, blocking reads, SUBSCRIBE experiments.
  // The dedicated client inherits the full configuration (prefix, retries)
  // and is always released, whatever fn does.
  async withDedicatedConnection (fn) {
    return this.#withDedicatedConnection('withDedicatedConnection', fn)
  }

  async #withDedicatedConnection (operation, fn, { reuse = false } = {}) {
    const client = this.connection.assertReady(operation)
    const held = { client: this.#lease(client, reuse), cancelled: false, reuse }

    this.#dedicated.add(held)

    try {
      return await fn(held.client)
    } catch (err) {
      // Shutdown closed this connection under a command that was still
      // waiting: that is a cancellation, not a failure of its own, and the
      // caller needs a code it can branch on to leave its loop.
      if (held.cancelled) {
        throw new RedisClientError(
          `disconnect() closed the connection while '${operation}' was still waiting.`,
          operation,
          'REDIS_UNAVAILABLE'
        )
      }

      throw err
    } finally {
      this.#dedicated.delete(held)
      this.#return(held)
    }
  }

  // A pooled connection when the caller can share one, a fresh one otherwise.
  #lease (client, reuse) {
    if (reuse) {
      const pooled = this.#idleBlocking.pop()

      if (pooled?.status === 'ready') {
        return pooled
      }

      // Recycled from a cycle that already ended: not worth reviving.
      pooled?.disconnect()
    }

    const fresh = client.duplicate()

    fresh.on('error', (err) => {
      this.logger.debug?.(`Dedicated connection error: ${err.message}`)
    })

    return fresh
  }

  #return ({ client, cancelled, reuse }) {
    // One-shot connections may carry per-connection state the caller left
    // behind (WATCH, MULTI, SUBSCRIBE), and a cancelled or unhealthy socket is
    // never worth recycling.
    const recyclable = reuse &&
      !cancelled &&
      client.status === 'ready' &&
      this.#idleBlocking.length < MAX_IDLE_BLOCKING_CONNECTIONS

    if (recyclable) {
      this.#idleBlocking.push(client)

      return
    }

    client.disconnect()
  }

  // Blocking commands (XREAD/XREADGROUP with BLOCK) run on a dedicated
  // connection: on the shared one they would stall every other command of the
  // application until the block resolves. The connection is pooled afterwards
  // — a consumer loop calls this on every iteration, and a handshake per
  // iteration is a cost nobody asked for.
  async executeBlockingCommand (command, args) {
    try {
      return await this.#withDedicatedConnection(
        command,
        (client) => client[command](...args),
        { reuse: true }
      )
    } catch (err) {
      this.logError(err, command)

      throw err
    }
  }

  async executeCommand (command, ...args) {
    const client = this.connection.assertReady(command)

    try {
      if (command === 'getAllStream') {
        return await this._getAllStream(...args)
      }

      return await client[command](...args)
    } catch (err) {
      this.logError(err, command)

      throw err
    }
  }

  async get (key) {
    return this.executeCommand('get', key)
  }

  async getAllStream (pattern = '*') {
    return this.executeCommand('getAllStream', pattern)
  }

  async set (key, value) {
    return this.executeCommand('set', key, value)
  }

  async setex (key, seconds, value) {
    return this.executeCommand('setex', key, seconds, value)
  }

  async del (...keys) {
    return this.executeCommand('del', ...keys)
  }

  async incr (key) {
    return this.executeCommand('incr', key)
  }

  async decr (key) {
    return this.executeCommand('decr', key)
  }

  async hset (key, ...args) {
    return this.executeCommand('hset', key, ...args)
  }

  async hget (key, field) {
    return this.executeCommand('hget', key, field)
  }

  async hgetall (key) {
    return this.executeCommand('hgetall', key)
  }

  async lpush (key, ...values) {
    return this.executeCommand('lpush', key, ...values)
  }

  async rpop (key) {
    return this.executeCommand('rpop', key)
  }

  async sadd (key, ...members) {
    return this.executeCommand('sadd', key, ...members)
  }

  async smembers (key) {
    return this.executeCommand('smembers', key)
  }

  async expire (key, seconds) {
    return this.executeCommand('expire', key, seconds)
  }

  async ttl (key) {
    return this.executeCommand('ttl', key)
  }

  async setJson (key, value) {
    return this.executeCommand('set', key, JSON.stringify(value))
  }

  async getJson (key) {
    const value = await this.executeCommand('get', key)

    return value ? JSON.parse(value) : null
  }

  async setexJson (key, seconds, value) {
    return this.executeCommand('setex', key, seconds, JSON.stringify(value))
  }

  // Cache-aside: return the cached value, or produce it, store it (SETEX)
  // and return it. With `lock`, concurrent misses collapse into a single
  // producer call — the winner fills the cache while the others wait on the
  // library's own lock and re-read (dogpile/stampede protection).
  async getOrSet (key, ttlSeconds, producer, options = {}) {
    return this.#getOrSet(key, ttlSeconds, producer, options, {
      encode: (value) => {
        if (typeof value !== 'string' && typeof value !== 'number') {
          throw new RedisClientError(
            'getOrSet caches strings and numbers only — use getOrSetJson for anything else.',
            'getOrSet',
            'INVALID_ARGUMENT'
          )
        }

        return String(value)
      },
      decode: (raw) => raw
    })
  }

  async getOrSetJson (key, ttlSeconds, producer, options = {}) {
    return this.#getOrSet(key, ttlSeconds, producer, options, {
      encode: (value) => {
        const encoded = JSON.stringify(value)

        // JSON.stringify returns undefined for undefined/functions/symbols;
        // caching that would poison the key (an empty string that every
        // later read fails to parse until the ttl expires).
        if (typeof encoded !== 'string') {
          throw new RedisClientError(
            'getOrSetJson requires the producer to return a JSON-serializable value (got undefined, a function or a symbol).',
            'getOrSetJson',
            'INVALID_ARGUMENT'
          )
        }

        return encoded
      },
      decode: (raw) => JSON.parse(raw)
    })
  }

  async #getOrSet (key, ttlSeconds, producer, { lock } = {}, { encode, decode }) {
    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
      throw new RedisClientError(
        `getOrSet requires a positive integer ttl in seconds (got ${ttlSeconds}).`,
        'getOrSet',
        'INVALID_ARGUMENT'
      )
    }

    if (typeof producer !== 'function') {
      throw new RedisClientError(
        'getOrSet requires a producer function.',
        'getOrSet',
        'INVALID_ARGUMENT'
      )
    }

    const cached = await this.get(key)

    if (cached !== null) {
      return decode(cached)
    }

    // Every caller gets the value in its cached form (a decode of what was
    // stored), so winner and waiters always see consistent types.
    const produceAndStore = async () => {
      const value = await producer()
      const encoded = encode(value)
      await this.setex(key, ttlSeconds, encoded)

      return decode(encoded)
    }

    if (!lock) {
      return produceAndStore()
    }

    const lockOptions = {
      ttl: 10000,
      retries: 100,
      retryDelay: 50,
      retryJitter: 50,
      // Producers slower than the lock ttl must not reopen the stampede.
      autoExtend: true,
      ...(typeof lock === 'object' ? lock : {})
    }

    try {
      return await this.locks.withLock(`cache:${key}`, lockOptions, async () => {
        // Double-check: the winner may have filled the cache while we waited.
        const refreshed = await this.get(key)

        if (refreshed !== null) {
          return decode(refreshed)
        }

        return produceAndStore()
      })
    } catch (err) {
      if (err?.code !== 'LOCK_NOT_ACQUIRED') {
        throw err
      }

      // A cache call must not surface lock errors. Waiters land here after
      // waiting out their whole retry budget, so the winner has probably
      // filled the cache by now — re-read, and only produce unprotected as
      // the last resort (availability beats perfect stampede protection).
      this.logger.debug?.(`Cache lock for '${key}' not acquired within the retry budget — falling back.`)

      const fallback = await this.get(key)

      if (fallback !== null) {
        return decode(fallback)
      }

      return produceAndStore()
    }
  }

  // Non-blocking bulk deletion (SCAN + UNLINK batches) confined to the
  // prefixed keyspace, same semantics as getAllStream. Returns the number
  // of keys removed. The pattern is required — '*' wipes the whole
  // (prefixed) keyspace and must be an explicit choice.
  async deleteByPattern (pattern) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
      throw new RedisClientError(
        "deleteByPattern requires a non-empty pattern (use '*' explicitly to wipe the prefixed keyspace).",
        'deleteByPattern',
        'INVALID_ARGUMENT'
      )
    }

    const client = this.connection.assertReady('deleteByPattern')

    return deletePattern({
      client,
      keyPrefix: this.keyPrefix,
      logger: this.logger,
      pattern
    })
  }

  // HMSET is deprecated since Redis 4.0: delegate to variadic HSET.
  // Note: returns the number of newly created fields, not 'OK'.
  async hmset (key, obj) {
    return this.executeCommand('hset', key, obj)
  }

  async hmget (key, ...fields) {
    return this.executeCommand('hmget', key, ...fields)
  }

  async hincrby (key, field, increment) {
    return this.executeCommand('hincrby', key, field, increment)
  }

  async hexists (key, field) {
    return this.executeCommand('hexists', key, field)
  }

  async hdel (key, ...fields) {
    return this.executeCommand('hdel', key, ...fields)
  }

  async lrange (key, start, stop) {
    return this.executeCommand('lrange', key, start, stop)
  }

  async llen (key) {
    return this.executeCommand('llen', key)
  }

  async lrem (key, count, value) {
    return this.executeCommand('lrem', key, count, value)
  }

  async lpushx (key, value) {
    return this.executeCommand('lpushx', key, value)
  }

  async rpushx (key, value) {
    return this.executeCommand('rpushx', key, value)
  }

  async sismember (key, member) {
    return this.executeCommand('sismember', key, member)
  }

  async scard (key) {
    return this.executeCommand('scard', key)
  }

  // Without a count Redis returns a single member; with one it returns an
  // array — never force a count, or the return type silently changes.
  async spop (key, count) {
    return count === undefined
      ? this.executeCommand('spop', key)
      : this.executeCommand('spop', key, count)
  }

  async srem (key, ...members) {
    return this.executeCommand('srem', key, ...members)
  }

  // Sorted sets. Scores come back as numbers (Redis speaks strings) and
  // WITHSCORES replies as { member, score } pairs instead of a flat array.
  async zadd (key, ...args) {
    const [first] = args

    // The common case reads better as { member: score }; arrays and anything
    // else are passed straight through, so flags like NX/GT/CH stay available.
    const isMemberMap = args.length === 1 &&
      typeof first === 'object' &&
      first !== null &&
      !Array.isArray(first)

    const commandArgs = isMemberMap
      ? Object.entries(first).flatMap(([member, score]) => [score, member])
      : args

    if (commandArgs.length === 0) {
      throw new RedisClientError(
        'zadd requires at least one member to add.',
        'zadd',
        'INVALID_ARGUMENT'
      )
    }

    return this.executeCommand('zadd', key, ...commandArgs)
  }

  async zscore (key, member) {
    return parseScore(await this.executeCommand('zscore', key, member))
  }

  async zincrby (key, increment, member) {
    return parseScore(await this.executeCommand('zincrby', key, increment, member))
  }

  async zcard (key) {
    return this.executeCommand('zcard', key)
  }

  async zcount (key, min, max) {
    return this.executeCommand('zcount', key, min, max)
  }

  async zrank (key, member) {
    return this.executeCommand('zrank', key, member)
  }

  async zrevrank (key, member) {
    return this.executeCommand('zrevrank', key, member)
  }

  async zrem (key, ...members) {
    return this.executeCommand('zrem', key, ...members)
  }

  async zrange (key, start, stop, options = {}) {
    const args = [key, start, stop]

    if (options.byScore) args.push('BYSCORE')
    if (options.byLex) args.push('BYLEX')
    if (options.rev) args.push('REV')
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)
    if (options.withScores) args.push('WITHSCORES')

    const reply = await this.executeCommand('zrange', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zrevrange (key, start, stop, options = {}) {
    const args = [key, start, stop]

    if (options.withScores) args.push('WITHSCORES')

    const reply = await this.executeCommand('zrevrange', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zrangebyscore (key, min, max, options = {}) {
    const args = [key, min, max]

    if (options.withScores) args.push('WITHSCORES')
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)

    const reply = await this.executeCommand('zrangebyscore', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zremrangebyrank (key, start, stop) {
    return this.executeCommand('zremrangebyrank', key, start, stop)
  }

  async zremrangebyscore (key, min, max) {
    return this.executeCommand('zremrangebyscore', key, min, max)
  }

  // Without a count Redis pops a single member; with one it pops up to count.
  async zpopmin (key, count) {
    return this.#popScored('zpopmin', key, count)
  }

  async zpopmax (key, count) {
    return this.#popScored('zpopmax', key, count)
  }

  async #popScored (command, key, count) {
    const reply = count === undefined
      ? await this.executeCommand(command, key)
      : await this.executeCommand(command, key, count)

    const entries = parseScoredMembers(reply)

    return count === undefined ? entries[0] ?? null : entries
  }

  async sort (key, options = {}) {
    const args = [key]

    if (options.by) args.push('BY', options.by)
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)
    if (options.get) args.push('GET', options.get)
    if (options.direction) args.push(options.direction)
    if (options.alpha) args.push('ALPHA')

    return this.executeCommand('sort', ...args)
  }

  // Values are sent as-is (ioredis accepts the object form directly). No
  // magic JSON serialization: mget/get would hand back raw strings anyway —
  // use the *Json helpers for objects.
  async mset (obj) {
    return this.executeCommand('mset', obj)
  }

  async mget (...keys) {
    return this.executeCommand('mget', ...keys)
  }

  async exists (key) {
    return this.executeCommand('exists', key)
  }

  async type (key) {
    return this.executeCommand('type', key)
  }

  async rename (key, newkey) {
    return this.executeCommand('rename', key, newkey)
  }

  async renamenx (key, newkey) {
    return this.executeCommand('renamenx', key, newkey)
  }

  async persist (key) {
    return this.executeCommand('persist', key)
  }

  async multi () {
    return this.connection.assertReady('multi').multi()
  }

  // WATCH state is per-connection: on the shared connection, concurrent
  // flows watching keys poison each other (any EXEC/UNWATCH clears ALL
  // watches). Refusing loudly beats silently-wrong optimistic locking —
  // use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.
  async watch () {
    throw new RedisClientError(
      'watch() is not supported on the shared connection. Use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.',
      'watch',
      'UNSUPPORTED_OPERATION'
    )
  }

  async unwatch () {
    throw new RedisClientError(
      'unwatch() is not supported on the shared connection. Use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.',
      'unwatch',
      'UNSUPPORTED_OPERATION'
    )
  }

  // Channels are not keys: keyPrefix does not apply to pub/sub.
  async publish (channel, message) {
    return this.executeCommand('publish', channel, message)
  }

  async publishJson (channel, value) {
    return this.executeCommand('publish', channel, JSON.stringify(value))
  }

  async subscribe (channel, handler) {
    return this.subscriptions.subscribe(channel, handler)
  }

  async unsubscribe (channel) {
    return this.subscriptions.unsubscribe(channel)
  }

  async psubscribe (pattern, handler) {
    return this.subscriptions.psubscribe(pattern, handler)
  }

  async punsubscribe (pattern) {
    return this.subscriptions.punsubscribe(pattern)
  }

  /** The server's current `notify-keyspace-events` flags (empty when disabled). */
  async keyspaceNotifications () {
    const [first] = await this.#keyspaceFlagsByNode()

    return first?.flags ?? ''
  }

  // CONFIG has no key to route on, so a cluster has to be asked node by node —
  // and every master must answer, because each one is configured on its own and
  // each one emits only its own slots' events.
  async #keyspaceFlagsByNode () {
    const client = this.connection.assertReady('keyspaceNotifications')
    const isCluster = typeof client.nodes === 'function'
    const targets = isCluster ? client.nodes('master') : [client]

    return Promise.all(targets.map(async (target) => {
      const [, flags] = await target.config('GET', 'notify-keyspace-events')

      return {
        node: isCluster ? `${target.options?.host}:${target.options?.port}` : null,
        flags: flags ?? ''
      }
    }))
  }

  // Keyspace events only exist if the server was configured to emit them, and
  // a subscription to a silent channel looks exactly like one that works.
  // Probing turns that silence into an error that says what to enable.
  async subscribeToKeyEvents (event, handler, options = {}) {
    await this.#assertKeyspaceNotifications(event)

    const db = options.db ?? this.redisConfig.db

    // Not subscribe(): in a cluster these events are node-local, so they need
    // one subscriber per master (see SubscriptionManager).
    return this.subscriptions.subscribeEverywhere(`__keyevent@${db}__:${event}`, handler)
  }

  async #assertKeyspaceNotifications (event) {
    let readings

    try {
      readings = await this.#keyspaceFlagsByNode()
    } catch (err) {
      // Managed providers commonly block CONFIG. Refusing to subscribe would
      // be worse than subscribing without the guarantee.
      this.logger.warn(`Could not read notify-keyspace-events (${err.message}). Subscribing without verifying it.`)

      return
    }

    const required = KEY_EVENT_CLASSES[event]

    // One misconfigured master is enough to lose that shard's events silently,
    // so the weakest node decides the verdict — not the first one asked.
    for (const { node, flags } of readings) {
      const missing = []

      if (!flags.includes('E')) missing.push('E')
      if (required && !flags.includes('A') && !flags.includes(required)) missing.push(required)

      if (missing.length > 0) {
        const where = node ? ` on cluster node ${node}` : ''

        throw new RedisClientError(
          `Keyspace notifications are not enabled for '${event}'${where}: notify-keyspace-events is "${flags}", missing "${missing.join('')}". Enable it with CONFIG SET notify-keyspace-events "${flags}${missing.join('')}".`,
          'subscribeToKeyEvents',
          'KEYSPACE_NOTIFICATIONS_DISABLED'
        )
      }
    }
  }

  // Single-instance distributed lock (SET NX PX + token-checked Lua release).
  async acquireLock (name, options) {
    return this.locks.acquire(name, options)
  }

  async withLock (name, options, fn) {
    return this.locks.withLock(name, options, fn)
  }

  async xadd (key, id, ...args) {
    return this.executeCommand('xadd', key, id, ...args)
  }

  // block: 0 is a legitimate value (block forever) — test against null,
  // never truthiness. Blocking reads run on a dedicated connection.
  async xread (options = {}, streams) {
    const args = []

    if (options.count != null) args.push('COUNT', options.count)
    if (options.block != null) args.push('BLOCK', options.block)

    args.push('STREAMS', ...streams)

    return options.block != null
      ? this.executeBlockingCommand('xread', args)
      : this.executeCommand('xread', ...args)
  }

  async xreadgroup (groupName, consumerName, options = {}, streams) {
    const args = ['GROUP', groupName, consumerName]

    if (options.count != null) args.push('COUNT', options.count)
    if (options.block != null) args.push('BLOCK', options.block)
    if (options.noack) args.push('NOACK')

    args.push('STREAMS', ...streams)

    return options.block != null
      ? this.executeBlockingCommand('xreadgroup', args)
      : this.executeCommand('xreadgroup', ...args)
  }

  // ioredis applies keyPrefix by argument position, and XGROUP/XINFO carry
  // their key *after* a subcommand — a position it does not recognize. Left
  // alone, a prefixed client would create consumer groups on unprefixed keys
  // while XADD/XREADGROUP used the prefixed ones.
  #prefixed (key) {
    return `${this.keyPrefix}${key}`
  }

  // Each XGROUP subcommand has its own arity — a blanket trailing id turned
  // documented calls like xgroup('DESTROY', key, group) into protocol errors.
  async xgroup (command, key, groupName, ...rest) {
    const subcommand = String(command).toUpperCase()
    const args = [subcommand, this.#prefixed(key), groupName]

    switch (subcommand) {
      case 'CREATE': {
        const [id = '$', mkstream] = rest
        args.push(id)

        if (mkstream) {
          args.push('MKSTREAM')
        }

        break
      }
      case 'SETID': {
        const [id = '$'] = rest
        args.push(id)

        break
      }
      case 'CREATECONSUMER':
      case 'DELCONSUMER': {
        args.push(rest[0])

        break
      }
      // DESTROY takes no extra arguments.
    }

    return this.executeCommand('xgroup', ...args)
  }

  async xlen (key) {
    return this.executeCommand('xlen', key)
  }

  async xinfo (subcommand, key, ...args) {
    return this.executeCommand('xinfo', subcommand, this.#prefixed(key), ...args)
  }

  async xrange (key, start, end, options = {}) {
    const args = [key, start, end]

    if (options.count != null) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrange', ...args)
  }

  async xrevrange (key, end, start, options = {}) {
    const args = [key, end, start]

    if (options.count != null) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrevrange', ...args)
  }

  async xdel (key, ...ids) {
    return this.executeCommand('xdel', key, ...ids)
  }

  async xtrim (key, strategy, approx = false, count) {
    if (count == null) {
      throw new RedisClientError(
        'xtrim requires a count/threshold value (e.g. xtrim(key, \'MAXLEN\', false, 1000)).',
        'xtrim',
        'INVALID_ARGUMENT'
      )
    }

    const args = [key, strategy]

    if (approx) {
      args.push('~')
    }

    args.push(count)

    return this.executeCommand('xtrim', ...args)
  }

  // Two shapes in one command: without a range it returns the group summary
  // ([total, minId, maxId, consumers]), with one it returns the pending
  // entries. Silently dropping a partial range would answer a different
  // question than the caller asked, in a different shape.
  async xpending (key, group, options = {}) {
    const { start, end, count, consumer } = options
    const wantsRange = start != null || end != null || count != null

    if (wantsRange && (start == null || end == null || count == null)) {
      throw new RedisClientError(
        "xpending needs start, end and count together to list entries (e.g. xpending(key, group, { start: '-', end: '+', count: 10 })). Pass no options for the group summary.",
        'xpending',
        'INVALID_ARGUMENT'
      )
    }

    if (!wantsRange && consumer != null) {
      throw new RedisClientError(
        'xpending can only filter by consumer together with start, end and count.',
        'xpending',
        'INVALID_ARGUMENT'
      )
    }

    const args = [key, group]

    if (wantsRange) {
      args.push(start, end, count)

      if (consumer) {
        args.push(consumer)
      }
    }

    return this.executeCommand('xpending', ...args)
  }

  // Settling a consumer-group entry: without it, every delivered entry stays
  // in the group's pending list forever.
  async xack (key, group, ...ids) {
    return this.executeCommand('xack', key, group, ...ids)
  }

  // Sweeps the group's pending list for entries idle longer than
  // minIdleTime and hands them to `consumer` — the recovery path for a
  // consumer that died holding deliveries. The reply is positional
  // ([cursor, entries, deleted]); it is returned as named fields so callers
  // do not index into it.
  async xautoclaim (key, group, consumer, minIdleTime, start = '0-0', options = {}) {
    const args = [key, group, consumer, minIdleTime, start]

    if (options.count != null) args.push('COUNT', options.count)
    if (options.justId) args.push('JUSTID')

    const [cursor, entries, deleted] = await this.executeCommand('xautoclaim', ...args)

    return { cursor, entries: entries ?? [], deleted: deleted ?? [] }
  }

  async xclaim (key, group, consumer, minIdleTime, ...ids) {
    const args = [key, group, consumer, minIdleTime, ...ids]

    return this.executeCommand('xclaim', ...args)
  }

  async _getAllStream (pattern = '*') {
    return scanKeyspace({
      client: this.connection.client,
      keyPrefix: this.keyPrefix,
      logger: this.logger,
      pattern
    })
  }

  omitPrefix (key) {
    return this.keyPrefix && key.startsWith(this.keyPrefix)
      ? key.slice(this.keyPrefix.length)
      : key
  }

  logError (err, operation) {
    if (err instanceof RedisClientError) {
      this.logger.error(`Redis operation '${err.operation}' failed: ${err.message}`)
    } else {
      this.logger.error(`Unexpected error in Redis '${operation}' operation: ${err.message}`)
    }
  }
}

export { RedisClient, RedisClientError, createLogger }
export default RedisClient

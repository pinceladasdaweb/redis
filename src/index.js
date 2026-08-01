import { EventEmitter } from 'node:events'
import LockManager from './resilience/lock.js'
import RedisConfig from './connection/config.js'
import RedisClientError from './utils/errors.js'
import HealthChecker from './connection/health.js'
import SubscriptionManager from './messaging/pubsub.js'
import ConnectionManager from './connection/manager.js'
import Logger, { createLogger } from './utils/logger.js'
import scanKeyspace, { deletePattern } from './keyspace/scanner.js'

// Thin facade: wires the collaborators together through a small context
// (logger, config, emit) and exposes the command surface. Mutable state is
// always reached through getters — never captured references.
class RedisClient extends EventEmitter {
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
    this.keyPrefix = this.config.keyPrefix ?? ''

    this.connection = new ConnectionManager({
      redisConfig: this.redisConfig,
      logger: this.logger,
      emit: (event, ...args) => this.emit(event, ...args)
    })

    this.health = new HealthChecker({
      getClient: () => this.connection.client,
      logger: this.logger,
      interval: options.healthCheckInterval ?? 5000,
      timeout: options.healthCheckTimeout ?? 1000
    })

    this.subscriptions = new SubscriptionManager({
      connection: this.connection,
      logger: this.logger,
      emit: (event, ...args) => this.emit(event, ...args)
    })

    this.locks = new LockManager({
      connection: this.connection,
      logger: this.logger
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

    return this.connection.disconnect()
  }

  async checkHealth () {
    return this.health.check()
  }

  // Escape hatch for anything that must not share the main connection:
  // WATCH/MULTI/EXEC transactions, blocking reads, SUBSCRIBE experiments.
  // The dedicated client inherits the full configuration (prefix, retries)
  // and is always released, whatever fn does.
  async withDedicatedConnection (fn) {
    const client = this.connection.assertReady('withDedicatedConnection')
    const dedicated = client.duplicate()

    dedicated.on('error', (err) => {
      this.logger.debug?.(`Dedicated connection error: ${err.message}`)
    })

    try {
      return await fn(dedicated)
    } finally {
      dedicated.disconnect()
    }
  }

  // Blocking commands (XREAD/XREADGROUP with BLOCK) run on a short-lived
  // dedicated connection: on the shared one they would stall every other
  // command of the application until the block resolves.
  async executeBlockingCommand (command, args) {
    try {
      return await this.withDedicatedConnection((client) => client[command](...args))
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

  async xpending (key, group, options = {}) {
    const args = [key, group]

    if (options.start != null && options.end != null && options.count != null) {
      args.push(options.start, options.end, options.count)

      if (options.consumer) {
        args.push(options.consumer)
      }
    }

    return this.executeCommand('xpending', ...args)
  }

  // Settling a consumer-group entry: without it, every delivered entry stays
  // in the group's pending list forever.
  async xack (key, group, ...ids) {
    return this.executeCommand('xack', key, group, ...ids)
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

import LockManager from './lock.js'
import RedisConfig from './config.js'
import HealthChecker from './health.js'
import scanKeyspace from './scanner.js'
import RedisClientError from './errors.js'
import { EventEmitter } from 'node:events'
import ConnectionManager from './connection.js'
import Logger, { createLogger } from './logger.js'
import SubscriptionManager from './pubsub.js'

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

  // Each XGROUP subcommand has its own arity — a blanket trailing id turned
  // documented calls like xgroup('DESTROY', key, group) into protocol errors.
  async xgroup (command, key, groupName, ...rest) {
    const subcommand = String(command).toUpperCase()
    const args = [subcommand, key, groupName]

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
    return this.executeCommand('xinfo', subcommand, key, ...args)
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

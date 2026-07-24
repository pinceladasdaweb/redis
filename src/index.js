import Logger from './logger.js'
import RedisConfig from './config.js'

class RedisClientError extends Error {
  constructor (message, operation, code = 'REDIS_CLIENT_ERROR') {
    super(message)
    this.name = 'RedisClientError'
    this.operation = operation
    this.code = code
  }
}

class RedisClient {
  #connectPromise = null

  constructor (options = {}) {
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
    this.client = null
    this.keyPrefix = this.config.keyPrefix ?? ''
    this.isConnected = false
    this.lastHealthCheckTime = 0
    this.healthCheckInterval = options.healthCheckInterval ?? 5000
    this.healthCheckTimeout = options.healthCheckTimeout ?? 1000
    this.healthCheckPromise = null
  }

  // Reconnection is owned entirely by the ioredis driver (retryStrategy /
  // reconnectOnError in RedisConfig): a single client instance survives the
  // whole connect()..disconnect() cycle and the listeners below only track
  // state. The library never creates a second client for the same cycle.
  async connect () {
    if (this.client) {
      this.logger.debug?.('Redis client already exists. Reusing existing connection.')
      return
    }

    if (!this.#connectPromise) {
      this.#connectPromise = this.#establishConnection().finally(() => {
        this.#connectPromise = null
      })
    }

    return this.#connectPromise
  }

  async #establishConnection () {
    const client = this.redisConfig.createRedisClient()
    this.client = client

    client.on('ready', () => {
      this.isConnected = true
      this.logger.info('Redis connection is ready')
    })

    client.on('error', (err) => {
      this.logger.error(`Redis client error: ${err.message || err}`)
    })

    client.on('close', () => {
      this.isConnected = false
      this.logger.warn('Redis connection closed')
    })

    client.on('reconnecting', (delay) => {
      this.logger.info(`Redis client is reconnecting${typeof delay === 'number' ? ` in ${delay}ms` : ''}...`)
    })

    client.on('end', () => {
      // Final state: emitted after quit() or when retryStrategy gives up.
      // Release the instance so a later connect() starts a fresh cycle.
      this.isConnected = false
      client.removeAllListeners()

      if (this.client === client) {
        this.client = null
      }
    })

    try {
      await client.connect()
    } catch (err) {
      if (client.status === 'end') {
        this.logger.error(`Failed to connect to Redis: ${err.message}`)
        return
      }

      // The driver keeps retrying in the background per retryStrategy;
      // commands stay gated by the health check until it succeeds.
      this.logger.error(`Failed to connect to Redis: ${err.message}. Reconnection attempts continue in the background.`)
    }
  }

  async checkHealth () {
    const now = Date.now()

    if (now - this.lastHealthCheckTime < this.healthCheckInterval && this.healthCheckPromise) {
      return this.healthCheckPromise
    }

    this.lastHealthCheckTime = now
    this.healthCheckPromise = this.performHealthCheck()
    return this.healthCheckPromise
  }

  // Explicit health probe (real PING with a timeout) for operational checks
  // like readiness endpoints. Connection state itself is owned by the driver
  // events — this method observes, it does not mutate.
  async performHealthCheck () {
    if (!this.client || this.client.status !== 'ready') {
      return false
    }

    try {
      const pong = await this.timeoutOperation(
        (callback) => {
          this.client.ping((err, result) => {
            if (err) {
              callback(err)
            } else {
              callback(null, result)
            }
          })
        },
        this.healthCheckTimeout
      )

      return pong === 'PONG'
    } catch (error) {
      this.logger.error(`Redis health check failed: ${error.message}`)

      return false
    }
  }

  timeoutOperation (operation, ms) {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error('Operation timed out'))
      }, ms)

      operation((err, result) => {
        clearTimeout(timeoutId)

        if (err) {
          reject(err)
        } else {
          resolve(result)
        }
      })
    })
  }

  // Fail-fast gate: a cheap local probe of the driver's own status, never a
  // network round-trip. Commands issued while disconnected throw a structured
  // REDIS_UNAVAILABLE error instead of silently resolving to null — writes
  // must never look successful when nothing happened. Reconnection is not
  // this gate's job: the driver already owns it.
  #assertReady (operation) {
    const client = this.client

    if (!client || client.status !== 'ready') {
      this.logger.debug?.(`Redis is not connected. Rejecting '${operation}'.`)

      throw new RedisClientError(
        `Redis is not connected. Cannot execute '${operation}'.`,
        operation,
        'REDIS_UNAVAILABLE'
      )
    }

    return client
  }

  async executeCommand (command, ...args) {
    const client = this.#assertReady(command)

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

  async hset (key, field, value) {
    return this.executeCommand('hset', key, field, value)
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

  async hmset (key, obj) {
    return this.executeCommand('hmset', key, obj)
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

  async spop (key, count = 1) {
    return this.executeCommand('spop', key, count)
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

  async mset (obj) {
    const args = []

    for (const [key, value] of Object.entries(obj)) {
      args.push(key, typeof value === 'object' ? JSON.stringify(value) : value)
    }

    return this.executeCommand('mset', ...args)
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
    return this.#assertReady('multi').multi()
  }

  async watch (...keys) {
    return this.executeCommand('watch', ...keys)
  }

  async unwatch () {
    return this.executeCommand('unwatch')
  }

  async xadd (key, id, ...args) {
    return this.executeCommand('xadd', key, id, ...args)
  }

  async xread (options = {}, streams) {
    const args = []

    if (options.count) args.push('COUNT', options.count)
    if (options.block) args.push('BLOCK', options.block)

    args.push('STREAMS', ...streams)

    return this.executeCommand('xread', ...args)
  }

  async xreadgroup (groupName, consumerName, options = {}, streams) {
    const args = ['GROUP', groupName, consumerName]

    if (options.count) args.push('COUNT', options.count)
    if (options.block) args.push('BLOCK', options.block)
    if (options.noack) args.push('NOACK')

    args.push('STREAMS', ...streams)

    return this.executeCommand('xreadgroup', ...args)
  }

  async xgroup (command, key, groupName, id = '$') {
    const args = [command, key, groupName, id]

    if (command.toUpperCase() === 'CREATE' && arguments.length > 4 && arguments[4]) {
      args.push('MKSTREAM')
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

    if (options.count) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrange', ...args)
  }

  async xrevrange (key, end, start, options = {}) {
    const args = [key, end, start]

    if (options.count) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrevrange', ...args)
  }

  async xdel (key, ...ids) {
    return this.executeCommand('xdel', key, ...ids)
  }

  async xtrim (key, strategy, approx = false, count) {
    const args = [key, strategy]

    if (approx) {
      args.push('~')
    }

    args.push(count)

    return this.executeCommand('xtrim', ...args)
  }

  async xpending (key, group, options = {}) {
    const args = [key, group]

    if (options.start && options.end && options.count) {
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
    const client = this.client

    return new Promise((resolve, reject) => {
      const data = []
      const seen = new Set()

      // ioredis does NOT apply keyPrefix to SCAN MATCH patterns: prefix it
      // ourselves, so the scan is confined to this client's keyspace instead
      // of sweeping the whole database (and other applications' keys).
      const stream = client.scanStream({
        match: `${this.keyPrefix}${pattern}`,
        count: 100
      })

      stream.on('data', (keys) => {
        if (keys.length === 0) {
          return
        }

        // One pipelined round-trip per SCAN batch (bounded concurrency), and
        // per-key errors — e.g. WRONGTYPE for non-string keys — skip that key
        // instead of rejecting the whole scan.
        stream.pause()

        const properties = keys.map((key) => this.omitPrefix(key))

        client.pipeline(properties.map((property) => ['get', property])).exec()
          .then((results) => {
            results.forEach(([err, value], index) => {
              const property = properties[index]

              if (err) {
                this.logger.debug?.(`getAllStream skipped key '${property}': ${err.message}`)
                return
              }

              // SCAN may return a key more than once; null means the key
              // expired or was deleted between SCAN and GET.
              if (value !== null && !seen.has(property)) {
                seen.add(property)
                data.push({ [property]: value })
              }
            })

            stream.resume()
          })
          .catch((err) => {
            this.logger.error(`Error in getAllStream: ${err.message}`)
            stream.destroy()
            reject(err)
          })
      })

      stream.on('end', () => {
        this.logger.debug?.(`Redis getAllStream is complete. Entries: ${data.length}`)
        resolve(data)
      })

      stream.on('error', (error) => {
        this.logger.error(`Error in getAllStream: ${error.message}`)
        reject(error)
      })
    })
  }

  // Final and idempotent: quit() makes the driver emit 'end', which releases
  // the client (see #establishConnection) without ever scheduling a
  // reconnection. A later connect() starts a brand-new cycle.
  async disconnect () {
    const client = this.client

    if (!client) {
      return
    }

    try {
      if (client.status !== 'end') {
        await client.quit()
      }

      this.logger.info('Redis client disconnected successfully')
    } catch (err) {
      this.logger.warn(`Error during Redis quit: ${err.message}. Forcing the connection closed.`)
      client.disconnect()
    } finally {
      client.removeAllListeners()

      if (this.client === client) {
        this.client = null
      }

      this.isConnected = false
    }
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

export { RedisClient, RedisClientError }
export default RedisClient

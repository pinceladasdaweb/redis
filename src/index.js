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

  // Blocking commands (XREAD/XREADGROUP with BLOCK) run on a short-lived
  // dedicated connection: on the shared one they would stall every other
  // command of the application until the block resolves.
  async executeBlockingCommand (command, args) {
    const client = this.#assertReady(command)
    const blockingClient = client.duplicate()

    blockingClient.on('error', (err) => {
      this.logger.debug?.(`Blocking '${command}' connection error: ${err.message}`)
    })

    try {
      return await blockingClient[command](...args)
    } catch (err) {
      this.logError(err, command)

      throw err
    } finally {
      blockingClient.disconnect()
    }
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
    return this.#assertReady('multi').multi()
  }

  // WATCH state is per-connection: on the shared connection, concurrent
  // flows watching keys poison each other (any EXEC/UNWATCH clears ALL
  // watches). Refusing loudly beats silently-wrong optimistic locking; a
  // transaction API with a dedicated connection is planned.
  async watch () {
    throw new RedisClientError(
      'watch() is not supported on the shared connection. Use multi() for atomic batches.',
      'watch',
      'UNSUPPORTED_OPERATION'
    )
  }

  async unwatch () {
    throw new RedisClientError(
      'unwatch() is not supported on the shared connection. Use multi() for atomic batches.',
      'unwatch',
      'UNSUPPORTED_OPERATION'
    )
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

import Logger from './logger'
import RedisConfig from './config'

class RedisClientError extends Error {
  constructor (message, operation) {
    super(message)
    this.name = 'RedisClientError'
    this.operation = operation
  }
}

class RedisClient {
  constructor (options = {}) {
    const retryConfig = {
      maxRetryAttempts: options.maxRetryAttempts || Infinity,
      baseRetryDelay: options.baseRetryDelay || 1000,
      maxRetryDelay: options.maxRetryDelay || 30000
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
    this.isConnecting = false
    this.reconnectInterval = options.reconnectInterval || 5000
    this.maxReconnectAttempts = retryConfig.maxRetryAttempts
    this.reconnectAttempts = 0
    this.isConnected = false
    this.lastHealthCheckTime = 0
    this.healthCheckInterval = options.healthCheckInterval || 5000
    this.healthCheckTimeout = options.healthCheckTimeout || 1000
    this.healthCheckPromise = null
    this.initialConnectionAttempted = false
  }

  async connect () {
    if (this.client) {
      this.logger.info('Redis client already exists. Reusing existing connection.')
      return
    }

    if (this.isConnecting) {
      this.logger.info('Redis connection attempt already in progress.')
      return
    }

    this.isConnecting = true
    await this.attemptConnection()
  }

  async attemptConnection () {
    try {
      this.client = this.redisConfig.createRedisClient()

      this.client.on('connect', () => {
        this.logger.info('Redis is connected')
        this.isConnecting = false
        this.reconnectAttempts = 0
        this.isConnected = true
      })

      this.client.on('error', (err) => {
        this.logger.error(`Redis client error: ${err.message || err}`)
        this.isConnected = false

        if (!this.isConnecting) {
          this.isConnecting = true
          this.scheduleReconnect()
        }
      })

      this.client.on('close', () => {
        this.logger.warn('Redis connection closed')
        this.isConnected = false

        if (!this.isConnecting) {
          this.isConnecting = true
          this.scheduleReconnect()
        }
      })

      this.client.on('reconnecting', () => {
        this.logger.info('Redis client is reconnecting...')
      })

      if (!this.initialConnectionAttempted) {
        await this.client.ping()
        this.initialConnectionAttempted = true
      }

      this.isConnecting = false
      this.reconnectAttempts = 0
      this.isConnected = true
    } catch (err) {
      this.logger.error(`Failed to connect to Redis: ${err.message}`)
      this.isConnecting = false
      this.isConnected = false

      if (!this.initialConnectionAttempted) {
        this.initialConnectionAttempted = true
        this.scheduleReconnect()
      }
    }
  }

  scheduleReconnect () {
    if (this.maxReconnectAttempts > 0 && this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.error('Max reconnection attempts reached. Stopping reconnection attempts.')
      this.isConnecting = false

      return
    }

    this.reconnectAttempts++
    this.logger.info(`Scheduling Redis reconnection attempt ${this.reconnectAttempts} in ${this.reconnectInterval}ms`)
    setTimeout(() => this.attemptConnection(), this.reconnectInterval)
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

  async performHealthCheck () {
    if (!this.client) {
      this.isConnected = false

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

      this.isConnected = pong === 'PONG'

      return this.isConnected
    } catch (error) {
      this.logger.error(`Redis health check failed: ${error.message}`)
      this.isConnected = false

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

  async executeCommand (command, ...args) {
    const isHealthy = await this.checkHealth()

    if (!isHealthy) {
      this.logger.warn(`Redis is not healthy. Skipping ${command} operation.`)
      return null
    }

    try {
      if (command === 'getAllStream') {
        return this._getAllStream(...args)
      }

      return await this.client[command](...args)
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
    if (!this.client) {
      throw new RedisClientError('Redis client is not initialized.', 'multi')
    }

    return this.client.multi()
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
    return new Promise((resolve, reject) => {
      const data = []
      const pending = []
      let totalKeys = 0
      let nullValues = 0

      const stream = this.client.scanStream({
        match: pattern
      })

      stream.on('data', (keys) => {
        totalKeys += keys.length

        const promises = keys.map(async (key) => {
          const property = this.omitPrefix(key)
          const value = await this.client.get(property)

          if (value === null) {
            nullValues++

            this.logger.warn(`Null value found for key: ${property}`)

            const ttl = await this.client.ttl(property)
            const type = await this.client.type(property)

            this.logger.info(`Key ${property} - Type: ${type}, TTL: ${ttl}`)
          } else {
            data.push({ [property]: value })
          }
        })

        pending.push(...promises)
      })

      stream.on('end', async () => {
        try {
          await Promise.all(pending)

          this.logger.info(`Redis getAllStream is complete. Total keys: ${totalKeys}, null values: ${nullValues}, valid entries: ${data.length}`)

          resolve(data)
        } catch (err) {
          this.logger.error(`Error in getAllStream: ${err.message}`)

          reject(err)
        }
      })

      stream.on('error', (error) => {
        this.logger.error(`Error in getAllStream: ${error.message}`)

        reject(error)
      })
    })
  }

  async disconnect () {
    if (this.client) {
      try {
        await this.client.quit()

        this.client = null
        this.isConnected = false

        this.logger.info('Redis client disconnected successfully')
      } catch (err) {
        this.logger.error(`Error disconnecting Redis client: ${err.message}`)

        throw err
      }
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

export { RedisClient }
export default RedisClient

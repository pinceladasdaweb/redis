import Redis from 'ioredis'

class RedisConfig {
  constructor (options = {}) {
    this.logger = options.logger
    this.configOptions = {
      host: options.host,
      port: options.port,
      username: options.username,
      password: options.password,
      db: options.db,
      keyPrefix: options.keyPrefix,
      connectionName: options.connectionName,
      commandTimeout: options.commandTimeout,
      retryStrategy: this.retryStrategy.bind(this),
      reconnectOnError: this.reconnectOnError.bind(this),
      maxRetriesPerRequest: options.maxRetriesPerRequest ?? null,
      enableReadyCheck: options.enableReadyCheck ?? true,
      autoResubscribe: options.autoResubscribe ?? true,
      autoResendUnfulfilledCommands: options.autoResendUnfulfilledCommands ?? true,
      lazyConnect: options.lazyConnect ?? true
    }

    this.maxRetryAttempts = options.maxRetryAttempts
    this.baseRetryDelay = options.baseRetryDelay
    this.maxRetryDelay = options.maxRetryDelay
  }

  getOptions () {
    return this.configOptions
  }

  createRedisClient () {
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

  reconnectOnError (err) {
    if (err.message.includes('READONLY')) {
      this.logger.warn('READONLY error detected. Reconnecting to potential new master.')

      return true
    }

    if (err.message.includes('ECONNREFUSED')) {
      this.logger.warn('Connection refused. Attempting to reconnect.')

      return true
    }

    if (err.message.includes('ENOTFOUND')) {
      this.logger.warn('Host not found. The Redis service might not be ready yet. Attempting to reconnect.')

      return true
    }

    return false
  }
}

export { RedisConfig }
export default RedisConfig

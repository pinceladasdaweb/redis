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
      // Sentinel mode (high availability): only forwarded when configured,
      // so standalone options stay exactly as ioredis expects them.
      ...(options.sentinels
        ? {
            sentinels: options.sentinels,
            name: options.name,
            sentinelPassword: options.sentinelPassword,
            role: options.role
          }
        : {}),
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

  // Called by ioredis for COMMAND errors only (server error replies) —
  // socket errors like ECONNREFUSED/ENOTFOUND never reach here; those belong
  // to retryStrategy (the previous branches for them were dead code). Redis
  // error replies start with the error-code token, so startsWith is the
  // structured check (never match substrings mid-message).
  reconnectOnError (err) {
    if (err.message.startsWith('READONLY')) {
      this.logger.warn('READONLY error detected. Reconnecting to the new master and resending the command.')

      // 2 = reconnect AND resend the failed command once reconnected.
      return 2
    }

    return false
  }
}

export { RedisConfig }
export default RedisConfig

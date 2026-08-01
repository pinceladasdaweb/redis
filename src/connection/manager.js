import RedisClientError from '../utils/errors.js'

// Owns the client lifecycle. Reconnection itself belongs entirely to the
// ioredis driver (retryStrategy / reconnectOnError in RedisConfig): a single
// client instance survives the whole connect()..disconnect() cycle and the
// listeners here only track state and surface events on the facade.
class ConnectionManager {
  #connectPromise = null
  #client = null
  #isConnected = false

  constructor ({ redisConfig, logger, emit }) {
    this.redisConfig = redisConfig
    this.logger = logger
    this.emit = emit
  }

  get client () {
    return this.#client
  }

  get isConnected () {
    return this.#isConnected
  }

  async connect () {
    // An attempt in flight must be joined, never skipped: #establishConnection
    // assigns the client synchronously, so a concurrent caller that checked
    // for the client first would resolve before the connection was ready.
    if (this.#connectPromise) {
      return this.#connectPromise
    }

    if (this.#client) {
      this.logger.debug?.('Redis client already exists. Reusing existing connection.')
      return
    }

    this.#connectPromise = this.#establishConnection().finally(() => {
      this.#connectPromise = null
    })

    return this.#connectPromise
  }

  async #establishConnection () {
    const client = this.redisConfig.createRedisClient()
    this.#client = client

    client.on('ready', () => {
      this.#isConnected = true
      this.logger.info('Redis connection is ready')
      this.emit('ready')
    })

    client.on('error', (err) => {
      this.logger.error(`Redis client error: ${err.message || err}`)
      // Never re-emitted as 'error': an EventEmitter 'error' without a
      // listener crashes the process. Consumers subscribe on their terms.
      this.emit('connectionError', err)
    })

    client.on('close', () => {
      this.#isConnected = false
      this.logger.warn('Redis connection closed')
      this.emit('close')
    })

    client.on('reconnecting', (delay) => {
      this.logger.info(`Redis client is reconnecting${typeof delay === 'number' ? ` in ${delay}ms` : ''}...`)
      this.emit('reconnecting', delay)
    })

    client.on('end', () => {
      // Final state: emitted after quit() or when retryStrategy gives up.
      // Release the instance so a later connect() starts a fresh cycle.
      this.#isConnected = false
      client.removeAllListeners()

      if (this.#client === client) {
        this.#client = null
      }

      this.emit('end')
    })

    try {
      await client.connect()
    } catch (err) {
      if (client.status === 'end') {
        this.logger.error(`Failed to connect to Redis: ${err.message}`)

        // The driver is done with this client. Release it here too: if it
        // never emits 'end', a stale reference would make every later
        // connect() short-circuit and leave the caller unable to reconnect.
        this.#isConnected = false

        if (this.#client === client) {
          this.#client = null
        }

        return
      }

      // The driver keeps retrying in the background per retryStrategy;
      // commands stay gated by assertReady until it succeeds.
      this.logger.error(`Failed to connect to Redis: ${err.message}. Reconnection attempts continue in the background.`)
    }
  }

  // Final and idempotent: quit() makes the driver emit 'end', which releases
  // the client (see #establishConnection) without ever scheduling a
  // reconnection. A later connect() starts a brand-new cycle.
  async disconnect () {
    const client = this.#client

    if (!client) {
      return
    }

    // 'end' fires asynchronously after quit() resolves — wait for it so the
    // handler releases the client and emits the facade event, with a timed
    // escape route in case the driver never gets there.
    const ended = client.status === 'end'
      ? Promise.resolve()
      : new Promise((resolve) => {
        client.once('end', resolve)

        const timer = setTimeout(resolve, 2000)
        timer.unref?.()
      })

    try {
      if (client.status !== 'end') {
        await client.quit()
      }

      this.logger.info('Redis client disconnected successfully')
    } catch (err) {
      this.logger.warn(`Error during Redis quit: ${err.message}. Forcing the connection closed.`)
      client.disconnect()
    }

    await ended

    // Safety net: idempotent with the 'end' handler, and the only release
    // path when 'end' never fired.
    client.removeAllListeners()

    if (this.#client === client) {
      this.#client = null
    }

    this.#isConnected = false
  }

  // Fail-fast gate: a cheap local probe of the driver's own status, never a
  // network round-trip. Commands issued while disconnected throw a structured
  // REDIS_UNAVAILABLE error instead of silently resolving to null — writes
  // must never look successful when nothing happened. Reconnection is not
  // this gate's job: the driver already owns it.
  assertReady (operation) {
    const client = this.#client

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
}

export { ConnectionManager }
export default ConnectionManager

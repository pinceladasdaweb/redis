import withDeadline from '../utils/deadline.js'
import RedisClientError from '../utils/errors.js'

// How long a shutdown may wait on the driver, per step. Nothing here may block
// forever: a graceful shutdown that never finishes is worse than an abrupt one.
const SHUTDOWN_DEADLINE_MS = 2000

// Owns the client lifecycle. Reconnection itself belongs entirely to the
// ioredis driver (retryStrategy / reconnectOnError in RedisConfig): a single
// client instance survives the whole connect()..disconnect() cycle and the
// listeners here only track state and surface events on the facade.
class ConnectionManager {
  #connectPromise = null
  #disconnectPromise = null
  #client = null
  #isConnected = false

  constructor ({ redisConfig, logger, clock, emit }) {
    this.redisConfig = redisConfig
    this.logger = logger
    this.clock = clock
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

    // A teardown in flight must be waited out, never raced: for the whole
    // quit window #client still points at the dying client, and "reusing"
    // it would resolve this call successfully moments before 'end' nulls
    // the client out from under the caller — connected, with nothing behind
    // it. Waiting lets the fresh cycle below start from a clean slate.
    if (this.#disconnectPromise) {
      await this.#disconnectPromise
    }

    // At 'close', a flap and a give-up are indistinguishable — the driver
    // emits close→reconnecting or close→end in the SAME synchronous stack,
    // so a supervisor reconnecting from a 'close' handler would decide with
    // incomplete information. One deferral later the status has already
    // become 'reconnecting' (reuse: the driver owns the retry) or 'end'
    // (build fresh), and the ambiguity is gone.
    if (this.#client?.status === 'close') {
      await new Promise((resolve) => setImmediate(resolve))
    }

    if (this.#client) {
      // 'end' normally releases the client, but a caller can reach here in
      // the same tick the driver gave up, before the handler ran. A client
      // in that state can never carry a command again — reusing it would be
      // the same "connected with nothing behind it" lie as above.
      if (this.#client.status === 'end') {
        this.#client.removeAllListeners()
        this.#client = null
      } else {
        this.logger.debug?.('Redis client already exists. Reusing existing connection.')
        return
      }
    }

    // Only the attempt that is still current may clear the slot: disconnect()
    // drops it on purpose, and a settling older attempt must not wipe the
    // brand-new one that replaced it.
    const attempt = this.#establishConnection().finally(() => {
      if (this.#connectPromise === attempt) {
        this.#connectPromise = null
      }
    })

    this.#connectPromise = attempt

    return attempt
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
  // reconnection. A later connect() starts a brand-new cycle — and one that
  // arrives DURING this teardown waits for it (see connect()), which is what
  // makes that promise true rather than aspirational.
  async disconnect () {
    if (this.#disconnectPromise) {
      return this.#disconnectPromise
    }

    const teardown = this.#teardown().finally(() => {
      this.#disconnectPromise = null
    })

    this.#disconnectPromise = teardown

    return teardown
  }

  async #teardown () {
    const client = this.#client

    // An attempt still in flight is abandoned here: its client is the one
    // being closed, so a later connect() must start a fresh cycle instead of
    // joining a promise that will resolve with nothing behind it.
    this.#connectPromise = null

    if (!client) {
      return
    }

    // 'end' fires asynchronously after quit() resolves — wait for it so the
    // handler releases the client and emits the facade event, with a timed
    // escape route in case the driver never gets there.
    // The escape timer is awaited, so it must be able to fire (an unref'd
    // timer never does once the loop is otherwise idle) — and it is cleared
    // the moment 'end' arrives, so a clean shutdown never waits on it.
    const ended = client.status === 'end'
      ? Promise.resolve()
      : new Promise((resolve) => {
        const timer = this.clock.setTimeout(resolve, SHUTDOWN_DEADLINE_MS)

        client.once('end', () => {
          this.clock.clearTimeout(timer)
          resolve()
        })
      })

    try {
      if (client.status !== 'end') {
        // quit() only answers immediately while the offline queue is empty:
        // with anything queued the driver parks the QUIT behind it and only
        // replies once it reconnects — which, with the default infinite
        // retries, may be never. Without a deadline here the escape route
        // above is unreachable and shutdown hangs forever.
        await withDeadline(client.quit(), {
          clock: this.clock,
          ms: SHUTDOWN_DEADLINE_MS,
          operation: 'quit'
        })
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

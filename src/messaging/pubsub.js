// Pub/Sub manager. A connection in subscriber mode cannot execute regular
// commands, so subscriptions live on a dedicated connection created lazily
// from the main one. Re-subscribing after a reconnection is handled by the
// driver (autoResubscribe) — the same client object survives the whole cycle,
// which is exactly what makes that guarantee hold.
class SubscriptionManager {
  #subscriber = null
  #channelHandlers = new Map()
  #patternHandlers = new Map()

  constructor ({ connection, logger, emit }) {
    this.connection = connection
    this.logger = logger
    this.emit = emit
  }

  #dispatch (handler, message, channel, pattern) {
    if (!handler) {
      return
    }

    // Handlers may be async: a rejection must never become an unhandled
    // rejection inside an EventEmitter callback.
    Promise.resolve()
      .then(() => handler(message, channel, pattern))
      .catch((err) => {
        this.logger.error(`Pub/sub handler for '${pattern ?? channel}' failed: ${err.message}`)
      })
  }

  #ensureSubscriber () {
    if (this.#subscriber) {
      return this.#subscriber
    }

    const subscriber = this.connection.assertReady('subscribe').duplicate()
    this.#subscriber = subscriber

    subscriber.on('error', (err) => {
      this.logger.error(`Redis subscriber error: ${err.message || err}`)
      this.emit('connectionError', err)
    })

    subscriber.on('message', (channel, message) => {
      this.emit('message', channel, message)
      this.#dispatch(this.#channelHandlers.get(channel), message, channel)
    })

    subscriber.on('pmessage', (pattern, channel, message) => {
      this.emit('pmessage', pattern, channel, message)
      this.#dispatch(this.#patternHandlers.get(pattern), message, channel, pattern)
    })

    subscriber.on('end', () => {
      // The subscriber's own retryStrategy gave up: release the dead client
      // so a later subscribe() starts a fresh connection, and never lose the
      // subscriptions silently. (A normal close() detaches this handler
      // before quitting, so it only fires for real give-ups.)
      if (this.#subscriber === subscriber) {
        this.#subscriber = null

        if (this.#channelHandlers.size > 0 || this.#patternHandlers.size > 0) {
          this.logger.warn('Redis subscriber connection ended permanently: active subscriptions were lost. Subscribe again to restore them.')
        }
      }

      subscriber.removeAllListeners()
    })

    return subscriber
  }

  // One handler per channel/pattern — a re-subscribe replaces it (last one
  // wins); the facade 'message'/'pmessage' events allow fan-out when needed.
  // On a failed subscribe the previous handler is restored.
  async subscribe (channel, handler) {
    const subscriber = this.#ensureSubscriber()
    const previous = this.#channelHandlers.get(channel)

    if (handler) {
      this.#channelHandlers.set(channel, handler)
    }

    try {
      return await subscriber.subscribe(channel)
    } catch (err) {
      this.#restore(this.#channelHandlers, channel, previous, handler)

      throw err
    }
  }

  async unsubscribe (channel) {
    this.#channelHandlers.delete(channel)

    if (!this.#subscriber) {
      return 0
    }

    return this.#subscriber.unsubscribe(channel)
  }

  async psubscribe (pattern, handler) {
    const subscriber = this.#ensureSubscriber()
    const previous = this.#patternHandlers.get(pattern)

    if (handler) {
      this.#patternHandlers.set(pattern, handler)
    }

    try {
      return await subscriber.psubscribe(pattern)
    } catch (err) {
      this.#restore(this.#patternHandlers, pattern, previous, handler)

      throw err
    }
  }

  #restore (handlers, key, previous, attempted) {
    if (!attempted) {
      return
    }

    if (previous) {
      handlers.set(key, previous)
    } else {
      handlers.delete(key)
    }
  }

  async punsubscribe (pattern) {
    this.#patternHandlers.delete(pattern)

    if (!this.#subscriber) {
      return 0
    }

    return this.#subscriber.punsubscribe(pattern)
  }

  // Called from the facade's disconnect(): the subscriber connection has its
  // own lifecycle and must be released explicitly.
  async close () {
    const subscriber = this.#subscriber

    if (!subscriber) {
      return
    }

    this.#subscriber = null
    this.#channelHandlers.clear()
    this.#patternHandlers.clear()

    subscriber.removeAllListeners()

    try {
      if (subscriber.status !== 'end') {
        await subscriber.quit()
      }
    } catch {
      subscriber.disconnect()
    }
  }
}

export { SubscriptionManager }
export default SubscriptionManager

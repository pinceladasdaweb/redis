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

    return subscriber
  }

  async subscribe (channel, handler) {
    const subscriber = this.#ensureSubscriber()

    if (handler) {
      this.#channelHandlers.set(channel, handler)
    }

    return subscriber.subscribe(channel)
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

    if (handler) {
      this.#patternHandlers.set(pattern, handler)
    }

    return subscriber.psubscribe(pattern)
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

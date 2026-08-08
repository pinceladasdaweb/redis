// Explicit health probe (real PING with a timeout) for operational checks
// like readiness endpoints. Connection state itself is owned by the driver
// events — this probe observes, it never mutates. Results are shared within
// `interval` so concurrent callers reuse one in-flight PING.
class HealthChecker {
  #lastCheckTime = 0
  #lastResult = null
  #inFlight = null

  constructor ({ getClient, logger, clock, interval = 5000, timeout = 1000 }) {
    this.getClient = getClient
    this.logger = logger
    this.clock = clock
    this.interval = interval
    this.timeout = timeout
  }

  async check () {
    // Concurrent callers always share the probe that is already running.
    if (this.#inFlight) {
      return this.#inFlight
    }

    const now = this.clock.now()

    // Only a healthy result is cached, and only while the connection is still
    // up. Caching a failure would keep reporting "down" after the connection
    // recovered; serving a stale "up" is worse — a readiness endpoint would
    // keep traffic flowing to a connection that already dropped. Checking the
    // driver's own status is a free local read.
    if (this.#lastResult === true && now - this.#lastCheckTime < this.interval && this.#readyClient()) {
      return true
    }

    this.#lastCheckTime = now
    this.#inFlight = this.#performCheck()
      .then((healthy) => {
        this.#lastResult = healthy

        return healthy
      })
      .finally(() => {
        this.#inFlight = null
      })

    return this.#inFlight
  }

  #readyClient () {
    const client = this.getClient()

    return client && client.status === 'ready' ? client : null
  }

  async #performCheck () {
    const client = this.#readyClient()

    if (!client) {
      return false
    }

    try {
      const pong = await this.#timeoutOperation(
        (callback) => {
          client.ping((err, result) => {
            if (err) {
              callback(err)
            } else {
              callback(null, result)
            }
          })
        },
        this.timeout
      )

      return pong === 'PONG'
    } catch (error) {
      this.logger.error(`Redis health check failed: ${error.message}`)

      return false
    }
  }

  #timeoutOperation (operation, ms) {
    return new Promise((resolve, reject) => {
      // Deliberately not unref'd: the caller awaits this promise, and an
      // unref'd timer never fires once the event loop has nothing else
      // scheduled — the probe would hang instead of timing out. It is
      // cleared as soon as the reply lands, so it holds nothing open.
      const timeoutId = this.clock.setTimeout(() => {
        reject(new Error('Operation timed out'))
      }, ms)

      operation((err, result) => {
        this.clock.clearTimeout(timeoutId)

        if (err) {
          reject(err)
        } else {
          resolve(result)
        }
      })
    })
  }
}

export { HealthChecker }
export default HealthChecker

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

  // Cancels an in-flight probe: clears its timer and settles it as unhealthy.
  // Shutdown needs this — the probe's timer is deliberately ref'd (it is
  // awaited, and an unref'd awaited timer never fires on an idle loop), so a
  // PING that will never be answered would otherwise hold the process open
  // for up to `timeout` after disconnect() resolved.
  stop () {
    this.#cancelInFlight?.()
  }

  #cancelInFlight = null

  async #performCheck () {
    const client = this.#readyClient()

    if (!client) {
      return false
    }

    try {
      const pong = await new Promise((resolve, reject) => {
        // Deliberately not unref'd: the caller awaits this promise, and an
        // unref'd timer never fires once the event loop has nothing else
        // scheduled — the probe would hang instead of timing out. It is
        // cleared as soon as the reply lands (or stop() runs), so it holds
        // nothing open past its answer.
        const timeoutId = this.clock.setTimeout(() => {
          this.#cancelInFlight = null
          reject(new Error('Operation timed out'))
        }, this.timeout)

        const settle = (fn) => (value) => {
          this.clock.clearTimeout(timeoutId)
          this.#cancelInFlight = null
          fn(value)
        }

        this.#cancelInFlight = settle(resolve).bind(null, null)

        client.ping().then(settle(resolve), settle(reject))
      })

      return pong === 'PONG'
    } catch (error) {
      this.logger.error(`Redis health check failed: ${error.message}`)

      return false
    }
  }
}

export { HealthChecker }
export default HealthChecker

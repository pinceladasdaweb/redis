// Explicit health probe (real PING with a timeout) for operational checks
// like readiness endpoints. Connection state itself is owned by the driver
// events — this probe observes, it never mutates. Results are shared within
// `interval` so concurrent callers reuse one in-flight PING.
class HealthChecker {
  #lastCheckTime = 0
  #checkPromise = null

  constructor ({ getClient, logger, interval = 5000, timeout = 1000 }) {
    this.getClient = getClient
    this.logger = logger
    this.interval = interval
    this.timeout = timeout
  }

  async check () {
    const now = Date.now()

    if (now - this.#lastCheckTime < this.interval && this.#checkPromise) {
      return this.#checkPromise
    }

    this.#lastCheckTime = now
    this.#checkPromise = this.#performCheck()

    return this.#checkPromise
  }

  async #performCheck () {
    const client = this.getClient()

    if (!client || client.status !== 'ready') {
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
      const timeoutId = setTimeout(() => {
        reject(new Error('Operation timed out'))
      }, ms)

      timeoutId.unref?.()

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
}

export { HealthChecker }
export default HealthChecker

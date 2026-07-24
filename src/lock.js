import { randomUUID } from 'node:crypto'
import RedisClientError from './errors.js'

// Single-instance distributed lock: SET NX PX to acquire, Lua compare-and-del
// to release (a client can only ever release or extend a lock it still
// holds). This is a best-effort mutex against ONE Redis instance — it is
// deliberately NOT Redlock and makes no multi-node quorum claims.
//
// Scripts run through defineCommand, so ioredis caches the SHA and reloads
// transparently on NOSCRIPT (e.g. after a server restart).

const RELEASE_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`

const EXTEND_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
else
  return 0
end
`

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

class LockManager {
  constructor ({ connection, logger }) {
    this.connection = connection
    this.logger = logger
  }

  // The main client is recreated on every connect() cycle, so the custom
  // commands are (re)defined lazily against whatever client is current.
  #client (operation) {
    const client = this.connection.assertReady(operation)

    if (typeof client.releaseLock !== 'function') {
      client.defineCommand('releaseLock', { numberOfKeys: 1, lua: RELEASE_SCRIPT })
      client.defineCommand('extendLock', { numberOfKeys: 1, lua: EXTEND_SCRIPT })
    }

    return client
  }

  async acquire (name, options = {}) {
    const ttl = options.ttl ?? 30000
    const retries = options.retries ?? 0
    const retryDelay = options.retryDelay ?? 100
    const key = `lock:${name}`
    const token = randomUUID()

    for (let attempt = 0; attempt <= retries; attempt++) {
      const client = this.#client('acquireLock')
      const result = await client.set(key, token, 'PX', ttl, 'NX')

      if (result === 'OK') {
        this.logger.debug?.(`Lock '${name}' acquired (ttl ${ttl}ms)`)

        return {
          name,
          token,
          release: async () => {
            const released = Number(await this.#client('releaseLock').releaseLock(key, token)) === 1

            if (!released) {
              this.logger.warn(`Lock '${name}' was not held on release — it may have expired mid-hold; consider a longer ttl.`)
            }

            return released
          },
          extend: async (ttlMs = ttl) =>
            Number(await this.#client('extendLock').extendLock(key, token, ttlMs)) === 1
        }
      }

      if (attempt < retries) {
        await sleep(retryDelay)
      }
    }

    throw new RedisClientError(
      `Could not acquire lock '${name}' after ${retries + 1} attempt(s).`,
      'acquireLock',
      'LOCK_NOT_ACQUIRED'
    )
  }

  async withLock (name, options, fn) {
    if (typeof options === 'function') {
      fn = options
      options = {}
    }

    const lock = await this.acquire(name, options)

    try {
      return await fn(lock)
    } finally {
      // Releasing must never mask fn's outcome: failures are logged, not
      // thrown (the ttl is the final safety net anyway).
      await lock.release().catch((err) => {
        this.logger.warn(`Failed to release lock '${name}': ${err.message}`)
      })
    }
  }
}

export { LockManager }
export default LockManager

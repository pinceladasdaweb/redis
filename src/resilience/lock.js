import { randomUUID } from 'node:crypto'
import RedisClientError from '../utils/errors.js'

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

class LockManager {
  constructor ({ connection, logger, clock }) {
    this.connection = connection
    this.logger = logger
    this.clock = clock
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
    // Random extra delay per attempt: under contention, fixed delays make
    // every waiter retry in lockstep; jitter spreads them out.
    const retryJitter = options.retryJitter ?? 0
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
        await this.clock.sleep(retryDelay + (retryJitter > 0 ? Math.floor(Math.random() * retryJitter) : 0))
      }
    }

    const failure = new RedisClientError(
      `Could not acquire lock '${name}' after ${retries + 1} attempt(s).`,
      'acquireLock',
      'LOCK_NOT_ACQUIRED'
    )

    // Which lock refused, as structure: `code` and `operation` are the same
    // for every lock, and the name must never have to be parsed out of the
    // message — a caller holding locks inside a producer needs to tell its
    // own failure from the cache's (see #getOrSet).
    failure.lockName = name

    throw failure
  }

  async withLock (name, options, fn) {
    if (typeof options === 'function') {
      fn = options
      options = {}
    }

    const lock = await this.acquire(name, options)
    let watchdog = null

    // Opt-in watchdog: keeps extending the lock at half-ttl intervals while
    // fn runs, for critical sections that may outlive the ttl. If the lock
    // is definitively lost (extend returns false), the watchdog stops and
    // warns — transient errors keep it trying while the ttl is still alive.
    if (options.autoExtend) {
      const ttl = options.ttl ?? 30000
      const interval = Math.max(Math.floor(ttl / 2), 50)

      watchdog = this.clock.setInterval(async () => {
        try {
          const extended = await lock.extend(ttl)

          if (!extended) {
            this.clock.clearInterval(watchdog)
            this.logger.warn(`Lock '${name}' could not be extended — it was lost (expired or taken over).`)
          }
        } catch (err) {
          this.logger.warn(`Failed to extend lock '${name}': ${err.message}`)
        }
      }, interval)

      watchdog.unref?.()
    }

    try {
      return await fn(lock)
    } finally {
      if (watchdog) {
        this.clock.clearInterval(watchdog)
      }

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

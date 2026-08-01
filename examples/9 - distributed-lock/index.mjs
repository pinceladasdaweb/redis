// Making sure only one worker runs a critical section at a time.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('9 — Distributed locking')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:lock:' })
await redis.connect()
await redis.deleteByPattern('*')

// Two workers race for the same job. withLock acquires, runs and always
// releases — even if the callback throws.
let concurrent = 0
let maxConcurrent = 0
const order = []

const worker = async (name) => {
  await redis.withLock('nightly-report', { ttl: 5000, retries: 50, retryDelay: 50, retryJitter: 25 }, async () => {
    concurrent++
    maxConcurrent = Math.max(maxConcurrent, concurrent)
    order.push(name)

    await new Promise((resolve) => setTimeout(resolve, 150))

    concurrent--
  })
}

await Promise.all([worker('worker-a'), worker('worker-b')])
console.log(`  execution order     → ${order.join(' then ')}`)
console.log(`  peak concurrency    → ${maxConcurrent} (mutual exclusion held)`)

// Manual control, when the critical section spans more than one function.
const lock = await redis.acquireLock('deploy', { ttl: 3000 })
console.log(`  acquireLock         → token ${lock.token.slice(0, 8)}…`)

// A second attempt fails fast instead of waiting forever.
await assert.rejects(redis.acquireLock('deploy'), { code: 'LOCK_NOT_ACQUIRED' })
console.log('  contending attempt  → LOCK_NOT_ACQUIRED (no silent waiting)')

console.log(`  release             → ${await lock.release()}`)

// Releasing is token-checked: a holder whose lock already expired can never
// delete the lock someone else acquired in the meantime.
console.log(`  release again       → ${await lock.release()} (already gone, nothing deleted)`)

// The callback's failure propagates, but the lock is still released.
await assert.rejects(redis.withLock('failing', async () => { throw new Error('job failed') }), /job failed/)
const afterFailure = await redis.acquireLock('failing')
console.log('  after a failure     → lock is free again')
await afterFailure.release()

assert.equal(maxConcurrent, 1, 'the whole point: never two at once')
assert.deepEqual(order.length, 2)

await redis.deleteByPattern('*')
await redis.disconnect()
done('Two workers, one at a time — locks released on success and on failure')

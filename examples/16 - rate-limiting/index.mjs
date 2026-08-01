// A fixed-window rate limiter built from atomic counters.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('16 — Rate limiting recipe')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:ratelimit:' })
await redis.connect()
await redis.deleteByPattern('*')

const LIMIT = 5
const WINDOW_SECONDS = 2

// INCR is atomic, so concurrent requests cannot both "see" the same count.
// The first request of a window also sets the expiration, which makes the
// window slide forward on its own.
const consume = async (identifier) => {
  const key = `user:${identifier}`
  const used = await redis.incr(key)

  if (used === 1) {
    await redis.expire(key, WINDOW_SECONDS)
  }

  return { allowed: used <= LIMIT, used, remaining: Math.max(0, LIMIT - used) }
}

const outcomes = []
for (let attempt = 1; attempt <= 7; attempt++) {
  const result = await consume('42')
  outcomes.push(result.allowed)
  console.log(`  request ${attempt}          → ${result.allowed ? 'allowed' : 'BLOCKED'} (remaining: ${result.remaining})`)
}

console.log(`  window ttl          → ${await redis.ttl('user:42')}s`)

// Concurrency safety: 10 simultaneous requests from a fresh user consume
// exactly 10 slots, never fewer.
const burst = await Promise.all(Array.from({ length: 10 }, () => consume('99')))
const allowedInBurst = burst.filter((result) => result.allowed).length
console.log(`  10 parallel requests → ${allowedInBurst} allowed, ${10 - allowedInBurst} blocked`)

// After the window expires the budget resets.
await new Promise((resolve) => setTimeout(resolve, WINDOW_SECONDS * 1000 + 200))
const afterWindow = await consume('42')
console.log(`  after the window    → ${afterWindow.allowed ? 'allowed' : 'BLOCKED'} again (used: ${afterWindow.used})`)

assert.deepEqual(outcomes, [true, true, true, true, true, false, false])
assert.equal(allowedInBurst, LIMIT, 'atomic counters must not over-admit under concurrency')
assert.equal(afterWindow.used, 1, 'the window must reset by expiration')

await redis.deleteByPattern('*')
await redis.disconnect()
done(`${LIMIT} requests per ${WINDOW_SECONDS}s enforced, including under a 10-way burst`)

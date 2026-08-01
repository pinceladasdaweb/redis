// What happens when a hot key expires and 50 requests miss at once.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('5 — Stampede protection')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:stampede:' })
await redis.connect()
await redis.deleteByPattern('*')

const CONCURRENT_REQUESTS = 50

const expensiveQuery = async (counter) => {
  counter.runs++
  await new Promise((resolve) => setTimeout(resolve, 200))

  return { rows: 1000, at: counter.runs }
}

// Without protection: every concurrent miss runs the producer. This is the
// dogpile effect — the database gets hit hardest exactly when the cache
// stopped shielding it.
const unprotected = { runs: 0 }
await Promise.all(
  Array.from({ length: CONCURRENT_REQUESTS }, () =>
    redis.getOrSetJson('report:unprotected', 60, () => expensiveQuery(unprotected)))
)
console.log(`  without lock        → ${unprotected.runs} producer runs for ${CONCURRENT_REQUESTS} requests`)

// With { lock: true }: the winner produces while everyone else waits on the
// library's own lock, then reads the value the winner just cached.
const protectedRun = { runs: 0 }
const results = await Promise.all(
  Array.from({ length: CONCURRENT_REQUESTS }, () =>
    redis.getOrSetJson('report:protected', 60, () => expensiveQuery(protectedRun), { lock: true }))
)
console.log(`  with lock           → ${protectedRun.runs} producer run for ${CONCURRENT_REQUESTS} requests`)

// Everyone gets the same value, whether they produced it or waited for it.
const distinct = new Set(results.map((value) => JSON.stringify(value)))
console.log(`  distinct results    → ${distinct.size} (all callers agree)`)

assert.ok(unprotected.runs > 1, 'the unprotected case is expected to stampede')
assert.equal(protectedRun.runs, 1, 'the lock must collapse the stampede into one run')
assert.equal(distinct.size, 1)

await redis.deleteByPattern('*')
await redis.disconnect()
done(`${CONCURRENT_REQUESTS} concurrent misses → ${unprotected.runs} database hits unprotected, ${protectedRun.runs} with { lock: true }`)

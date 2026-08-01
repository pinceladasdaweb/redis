// Lists for ordered work, sets for membership.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('8 — Lists and sets')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:collections:' })
await redis.connect()

// A simple queue: push on one end, pop from the other.
await redis.lpush('jobs', 'job-1', 'job-2', 'job-3')
console.log(`  llen jobs           → ${await redis.llen('jobs')}`)
console.log(`  lrange (all)        → ${JSON.stringify(await redis.lrange('jobs', 0, -1))}`)

const next = await redis.rpop('jobs')
console.log(`  rpop (oldest first) → ${next}`)

// lpushx only appends when the list already exists — useful to avoid
// resurrecting a queue that was drained and deleted.
await redis.lpushx('jobs', 'job-4')
await redis.lpushx('missing-queue', 'ignored')
console.log(`  lpushx on missing   → ${await redis.llen('missing-queue')} (nothing created)`)

await redis.lrem('jobs', 1, 'job-2')
console.log(`  after lrem job-2    → ${JSON.stringify(await redis.lrange('jobs', 0, -1))}`)

// Sets: unique members, membership tests in O(1).
await redis.sadd('tags', 'redis', 'node', 'cache', 'redis')
console.log(`  scard tags          → ${await redis.scard('tags')} (duplicates collapse)`)
console.log(`  sismember redis     → ${await redis.sismember('tags', 'redis')}`)
console.log(`  smembers            → ${(await redis.smembers('tags')).sort().join(', ')}`)

await redis.srem('tags', 'cache')
const popped = await redis.spop('tags')
console.log(`  spop (random)       → ${popped}`)

assert.equal(await redis.llen('missing-queue'), 0, 'lpushx must not create the key')
assert.deepEqual(await redis.lrange('jobs', 0, -1), ['job-4', 'job-3'])
assert.equal(await redis.scard('tags'), 1, 'one removed, one popped out of three')

await redis.del('jobs', 'tags')
await redis.disconnect()
done('Queue drained and set membership exercised')

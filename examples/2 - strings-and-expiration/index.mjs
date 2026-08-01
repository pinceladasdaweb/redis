// Everyday key/value work: writing, reading, expiring and counting.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('2 — Strings, counters and expiration')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:strings:' })
await redis.connect()

// Plain write/read.
await redis.set('greeting', 'hello')
console.log(`  get greeting        → ${await redis.get('greeting')}`)

// Write with a lifetime (seconds). setex is a single atomic command, so the
// key never exists without its expiration.
await redis.setex('session', 60, 'token-abc')
console.log(`  ttl session         → ${await redis.ttl('session')}s`)

// Expiration can also be set (and removed) later.
await redis.expire('greeting', 30)
await redis.persist('greeting')
console.log(`  ttl greeting        → ${await redis.ttl('greeting')} (-1 means "no expiration")`)

// Counters are atomic: safe under concurrency, unlike read-modify-write.
await redis.set('views', '10')
await redis.incr('views')
await redis.incr('views')
await redis.decr('views')
console.log(`  views after +2 −1   → ${await redis.get('views')}`)

// Multi-key writes and reads in one round-trip.
await redis.mset({ 'user:1': 'ada', 'user:2': 'alan' })
console.log(`  mget user:1 user:2  → ${JSON.stringify(await redis.mget('user:1', 'user:2'))}`)

console.log(`  exists user:1       → ${await redis.exists('user:1')}`)
console.log(`  type user:1         → ${await redis.type('user:1')}`)

// A missing key reads as null — never an error.
console.log(`  get missing         → ${await redis.get('nope')}`)

assert.equal(await redis.get('views'), '11')
assert.equal(await redis.ttl('greeting'), -1, 'persist() must clear the expiration')
assert.deepEqual(await redis.mget('user:1', 'user:2'), ['ada', 'alan'])
assert.equal(await redis.get('nope'), null)

const removed = await redis.del('greeting', 'session', 'views', 'user:1', 'user:2')
assert.equal(removed, 5)

await redis.disconnect()
done(`Wrote, expired and counted — ${removed} keys cleaned up afterwards`)

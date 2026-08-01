// Hashes: partial updates without rewriting the whole document.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('7 — Hashes')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:hash:' })
await redis.connect()

// Write several fields at once...
await redis.hset('user:1', { name: 'Ada', email: 'ada@example.com', visits: '0' })

// ...or a single one.
await redis.hset('user:1', 'plan', 'pro')

console.log(`  hget name           → ${await redis.hget('user:1', 'name')}`)
console.log(`  hmget name, plan    → ${JSON.stringify(await redis.hmget('user:1', 'name', 'plan'))}`)

// Atomic field increments: no read-modify-write race between processes.
await redis.hincrby('user:1', 'visits', 1)
await redis.hincrby('user:1', 'visits', 4)
console.log(`  visits after +1 +4  → ${await redis.hget('user:1', 'visits')}`)

console.log(`  hexists email       → ${await redis.hexists('user:1', 'email')}`)

const all = await redis.hgetall('user:1')
console.log(`  hgetall             → ${JSON.stringify(all)}`)

await redis.hdel('user:1', 'email')
console.log(`  after hdel email    → ${Object.keys(await redis.hgetall('user:1')).sort().join(', ')}`)

// Hashes beat JSON documents when you update single fields often: only the
// field travels, and increments stay atomic.
assert.equal(all.visits, '5')
assert.equal(await redis.hexists('user:1', 'email'), 0)
assert.deepEqual(Object.keys(await redis.hgetall('user:1')).sort(), ['name', 'plan', 'visits'])

await redis.del('user:1')
await redis.disconnect()
done('Fields written, incremented and removed individually')

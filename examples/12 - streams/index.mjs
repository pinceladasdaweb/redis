// Streams: a durable log with consumer groups and acknowledgements.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('12 — Streams and consumer groups')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:stream:' })
await redis.connect()
await redis.deleteByPattern('*')

const STREAM = 'events'
const GROUP = 'workers'

// Producers append entries; '*' lets the server assign the id.
await redis.xadd(STREAM, '*', 'type', 'signup', 'user', '1')
await redis.xadd(STREAM, '*', 'type', 'purchase', 'user', '2')
await redis.xadd(STREAM, '*', 'type', 'signup', 'user', '3')
console.log(`  xlen                → ${await redis.xlen(STREAM)} entries`)

// A consumer group tracks what each consumer has seen. MKSTREAM (the last
// argument) creates the stream if it does not exist yet.
await redis.xgroup('CREATE', STREAM, GROUP, '0', true)

// '>' means "entries never delivered to this group".
const batch = await redis.xreadgroup(GROUP, 'worker-1', { count: 10 }, [STREAM, '>'])
const entries = batch[0][1]
console.log(`  xreadgroup          → ${entries.length} entries delivered to worker-1`)

// Until they are acknowledged, entries stay in the group's pending list —
// that is what makes redelivery after a crash possible.
let pending = await redis.xpending(STREAM, GROUP)
console.log(`  pending before ack  → ${pending[0]}`)

// Acknowledge the ones actually processed. Here worker-1 fails on the last.
const processed = entries.slice(0, 2).map(([id]) => id)
const acked = await redis.xack(STREAM, GROUP, ...processed)
console.log(`  xack                → ${acked} settled, 1 left pending on purpose`)

pending = await redis.xpending(STREAM, GROUP)
console.log(`  pending after ack   → ${pending[0]} (the unprocessed one)`)

// Another worker can claim entries idle for too long — this is how a crashed
// consumer's work gets picked up.
const claimed = await redis.xclaim(STREAM, GROUP, 'worker-2', 0, entries[2][0])
console.log(`  xclaim              → worker-2 took over ${claimed.length} entry`)
await redis.xack(STREAM, GROUP, entries[2][0])

// Reading a range without a group, for inspection or replay.
const recent = await redis.xrevrange(STREAM, '+', '-', { count: 2 })
console.log(`  xrevrange           → last ${recent.length} entries`)

// Keeping the log bounded.
await redis.xtrim(STREAM, 'MAXLEN', true, 2)
console.log(`  after xtrim ~2      → ${await redis.xlen(STREAM)} entries`)

assert.equal(entries.length, 3)
assert.equal(acked, 2)
assert.equal((await redis.xpending(STREAM, GROUP))[0], 0, 'everything must end up acknowledged')

await redis.xgroup('DESTROY', STREAM, GROUP)
await redis.deleteByPattern('*')
await redis.disconnect()
done('3 entries produced, delivered, acknowledged and claimed across two workers')

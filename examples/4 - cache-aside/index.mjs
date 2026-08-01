// The cache-aside pattern in one call: read, or produce and store.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('4 — Cache-aside (getOrSet)')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:cache:' })
await redis.connect()
await redis.deleteByPattern('*')

let databaseHits = 0

// Pretend this is an expensive query.
const loadReport = async () => {
  databaseHits++
  await new Promise((resolve) => setTimeout(resolve, 120))

  return { revenue: 12345, generatedAt: '2026-07-24T00:00:00.000Z' }
}

// First call: cache miss → the producer runs, the result is stored with a ttl.
let started = Date.now()
const first = await redis.getOrSetJson('report:daily', 300, loadReport)
console.log(`  first call          → ${Date.now() - started}ms (database hits: ${databaseHits})`)

// Second call: cache hit → the producer is never invoked.
started = Date.now()
const second = await redis.getOrSetJson('report:daily', 300, loadReport)
console.log(`  second call         → ${Date.now() - started}ms (database hits: ${databaseHits})`)

console.log(`  ttl                 → ${await redis.ttl('report:daily')}s`)

// The string flavour, for values that are not documents.
const flag = await redis.getOrSet('feature:beta', 60, () => 'enabled')
console.log(`  getOrSet (string)   → ${flag}`)

// The producer's value must be storable: a forgotten return would otherwise
// poison the key with garbage until it expired.
await assert.rejects(
  redis.getOrSetJson('broken', 60, () => undefined),
  { code: 'INVALID_ARGUMENT' }
)
console.log('  invalid producer    → rejected before writing anything')

assert.deepEqual(first, second, 'both calls must return the same document')
assert.equal(databaseHits, 1, 'the second call must not touch the database')
assert.equal(await redis.get('broken'), null)

await redis.deleteByPattern('*')
await redis.disconnect()
done(`Two reads, ${databaseHits} database hit — the cache absorbed the rest`)

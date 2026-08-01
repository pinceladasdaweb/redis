// Invalidating a family of keys without ever running KEYS in production.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('6 — Cache invalidation by pattern')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:invalidation:' })
await redis.connect()
await redis.deleteByPattern('*')

// A typical cache layout: many entries per entity, plus unrelated keys.
await redis.setJson('user:1:profile', { name: 'Ada' })
await redis.setJson('user:1:permissions', ['read', 'write'])
await redis.setJson('user:2:profile', { name: 'Alan' })
await redis.set('config:theme', 'dark')

// getAllStream dumps the (prefixed) keyspace: SCAN plus pipelined reads, never
// a blocking KEYS. Note it only returns string values by design.
const everything = Object.assign({}, ...await redis.getAllStream())
console.log(`  keys in cache       → ${Object.keys(everything).sort().join(', ')}`)

// Invalidate one entity: SCAN + UNLINK in batches, so the server never blocks
// on a big delete.
const removed = await redis.deleteByPattern('user:1:*')
console.log(`  deleteByPattern     → removed ${removed} key(s) for user:1`)

const survivors = Object.assign({}, ...await redis.getAllStream())
console.log(`  remaining           → ${Object.keys(survivors).sort().join(', ')}`)

// The pattern is mandatory: wiping the whole keyspace has to be deliberate.
await assert.rejects(redis.deleteByPattern(''), { code: 'INVALID_ARGUMENT' })
console.log('  empty pattern       → rejected (wiping everything must be explicit)')

assert.equal(removed, 2)
assert.deepEqual(Object.keys(survivors).sort(), ['config:theme', 'user:2:profile'])

await redis.deleteByPattern('*')
await redis.disconnect()
done(`Invalidated ${removed} keys of one entity, left the rest untouched`)

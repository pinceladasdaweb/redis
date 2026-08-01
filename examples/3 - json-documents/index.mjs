// Storing objects without hand-rolling JSON.stringify everywhere.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('3 — JSON documents')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:json:' })
await redis.connect()

const profile = {
  id: 42,
  name: 'Ada Lovelace',
  roles: ['admin', 'engineer'],
  preferences: { theme: 'dark', notifications: { email: true } }
}

await redis.setJson('user:42', profile)

const loaded = await redis.getJson('user:42')
console.log(`  round-trip          → ${loaded.name}, roles: ${loaded.roles.join(', ')}`)
console.log(`  nested access       → theme=${loaded.preferences.theme}`)

// With an expiration, for cached documents.
await redis.setexJson('user:42:summary', 60, { name: profile.name, roles: profile.roles.length })
console.log(`  summary ttl         → ${await redis.ttl('user:42:summary')}s`)

// Missing keys read as null, so callers can branch without try/catch.
console.log(`  missing document    → ${await redis.getJson('user:999')}`)

// Serialization is explicit on purpose: plain set/get never transform values,
// so what you store is exactly what you read back.
await redis.set('raw', JSON.stringify({ manual: true }))
console.log(`  raw get             → ${await redis.get('raw')} (a string, untouched)`)

assert.deepEqual(loaded, profile, 'the document must survive the round-trip intact')
assert.equal(await redis.getJson('user:999'), null)
assert.equal(typeof await redis.get('raw'), 'string')

await redis.del('user:42', 'user:42:summary', 'raw')
await redis.disconnect()
done('Objects stored and read back with structure intact')

// Atomic batches, and optimistic locking that actually works.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('13 — Transactions and optimistic locking')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:tx:' })
await redis.connect()
await redis.deleteByPattern('*')

// multi() batches commands so they run atomically, in one round-trip.
const transaction = await redis.multi()
const results = await transaction.set('counter', '10').incr('counter').expire('counter', 60).exec()
console.log(`  multi              → ${results.length} commands executed atomically`)
console.log(`  counter            → ${await redis.get('counter')}`)

// WATCH is per-connection state: on the shared connection, concurrent flows
// would silently poison each other. The library refuses it out loud...
await assert.rejects(redis.watch('counter'), { code: 'UNSUPPORTED_OPERATION' })
console.log('  watch (shared)     → UNSUPPORTED_OPERATION, on purpose')

// ...and hands you an isolated connection instead.
await redis.set('balance', '100')

const committed = await redis.withDedicatedConnection(async (connection) => {
  await connection.watch('balance')
  const current = Number(await connection.get('balance'))

  return connection.multi().set('balance', String(current - 30)).exec()
})
console.log(`  clean transaction  → committed, balance is ${await redis.get('balance')}`)

// Now the conflict case: the watched key changes mid-transaction, so EXEC
// aborts and returns null instead of overwriting someone else's write.
const aborted = await redis.withDedicatedConnection(async (connection) => {
  await connection.watch('balance')
  const current = Number(await connection.get('balance'))

  // A concurrent writer touches the key we are watching.
  await redis.set('balance', '999')

  return connection.multi().set('balance', String(current - 50)).exec()
})
console.log(`  conflicting one    → ${aborted} (aborted, nothing was overwritten)`)
console.log(`  balance            → ${await redis.get('balance')} (the concurrent write survived)`)

assert.ok(committed, 'an untouched key must commit')
assert.equal(aborted, null, 'a changed key must abort the transaction')
assert.equal(await redis.get('balance'), '999')

await redis.deleteByPattern('*')
await redis.disconnect()
done('One transaction committed, one aborted safely on conflict')

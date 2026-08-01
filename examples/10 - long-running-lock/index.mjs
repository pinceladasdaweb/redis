// Holding a lock for a job that outlives its own ttl.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('10 — Long jobs and lock auto-extension')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:longjob:' })
await redis.connect()
await redis.deleteByPattern('*')

// The ttl is a safety net: if a holder dies, the lock frees itself. That is
// also the trap — a job slower than its ttl would lose the lock mid-flight
// and let a second worker in.
//
// autoExtend starts a watchdog that renews the lock at half-ttl intervals
// while the callback runs.
let intruderGotIn = false

await redis.withLock('import', { ttl: 2000, autoExtend: true }, async () => {
  console.log('  job started         → ttl is 2000ms, work takes 3000ms')

  for (let elapsed = 0; elapsed < 3000; elapsed += 750) {
    await new Promise((resolve) => setTimeout(resolve, 750))

    // Someone else tries to grab the lock while we are still working.
    try {
      const stolen = await redis.acquireLock('import')
      intruderGotIn = true
      await stolen.release()
    } catch {
      console.log(`  ${String(elapsed + 300).padStart(4)}ms elapsed     → lock still held`)
    }
  }
})

console.log('  job finished        → lock released')

// Once the callback returns, the watchdog stops and the lock is free.
const afterJob = await redis.acquireLock('import')
console.log('  after the job       → lock acquirable again')
await afterJob.release()

// Without autoExtend the same job would have lost the lock: this is the
// difference between a ttl that protects you and one that betrays you.
assert.equal(intruderGotIn, false, 'the lock must survive the whole job')

await redis.deleteByPattern('*')
await redis.disconnect()
done('A 3000ms job held a 2000ms lock from start to finish')

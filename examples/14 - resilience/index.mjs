// What the client does when Redis is not there.

import assert from 'node:assert/strict'
import RedisClient, { RedisClientError } from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('14 — Failure handling')

// A client pointed at a port where nothing listens.
const unreachable = new RedisClient({
  host: baseConfig.host,
  port: 1,
  baseRetryDelay: 50,
  maxRetryAttempts: 2,
  logger: quietLogger
})

const events = []
unreachable.on('reconnecting', () => events.push('reconnecting'))
unreachable.on('end', () => events.push('end'))

// connect() resolves even when the server is down: the driver keeps retrying
// in the background instead of leaving you with a rejected promise to babysit.
await unreachable.connect()

// Commands fail fast and loudly. They are never queued and never pretend to
// have worked — the difference between a cache miss and an outage stays
// visible to the caller.
try {
  await unreachable.set('key', 'value')
} catch (error) {
  console.log(`  set while down      → ${error.constructor.name} (${error.code})`)
  console.log(`  operation           → ${error.operation}`)
  assert.ok(error instanceof RedisClientError)
  assert.equal(error.code, 'REDIS_UNAVAILABLE')
}

// Branch on the code, never on the message: codes are contract, text is not.
const degrade = async () => {
  try {
    return await unreachable.getJson('profile:1')
  } catch (error) {
    if (error.code === 'REDIS_UNAVAILABLE') return { fallback: true }

    throw error
  }
}
console.log(`  graceful degradation → ${JSON.stringify(await degrade())}`)

console.log(`  health check        → ${await unreachable.checkHealth()}`)

await unreachable.disconnect()

// A healthy client for contrast: same call, real answer.
const healthy = new RedisClient({ ...baseConfig, logger: quietLogger })
await healthy.connect()
console.log(`  healthy client      → checkHealth is ${await healthy.checkHealth()}`)
await healthy.disconnect()

assert.ok(events.includes('reconnecting'), 'the driver must keep retrying in the background')

done('Failures surfaced as REDIS_UNAVAILABLE, never as a silent no-op')

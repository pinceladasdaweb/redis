// Connecting, observing the connection lifecycle and shutting down cleanly.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('1 — Connection lifecycle')

const redis = new RedisClient({
  ...baseConfig,
  logger: quietLogger,
  // Names the connection on the server: it shows up in CLIENT LIST, which
  // makes it possible to spot (or kill) this client during an incident.
  connectionName: 'example-connection'
})

const seen = []

// The client is an EventEmitter: every connection transition is observable.
redis.on('ready', () => { seen.push('ready'); console.log('  ready       → connection established, commands accepted') })
redis.on('close', () => { seen.push('close'); console.log('  close       → connection dropped') })
redis.on('reconnecting', (delay) => console.log(`  reconnecting → retrying in ${delay}ms`))
redis.on('connectionError', (err) => console.log(`  error       → ${err.message}`))
redis.on('end', () => { seen.push('end'); console.log('  end         → client released, no further retries') })

await redis.connect()

// checkHealth() is an explicit probe (a real PING with a timeout), meant for
// readiness endpoints. Regular commands do not pay for it.
const healthy = await redis.checkHealth()
console.log(`  checkHealth → ${healthy}`)

console.log(`  isConnected → ${redis.isConnected}`)

await redis.disconnect()
console.log(`  isConnected → ${redis.isConnected} (after disconnect)`)

assert.deepEqual(seen, ['ready', 'close', 'end'], 'the full lifecycle must be observable')
assert.equal(healthy, true)
assert.equal(redis.isConnected, false)

done('Connected, probed and shut down — lifecycle events: ready → close → end')

// Broadcasting events to every interested process.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('11 — Pub/Sub')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger })
await redis.connect()

const received = []
const patternMatches = []

// Subscriptions live on a dedicated connection managed by the library: a
// connection in subscriber mode cannot run normal commands, so publishing and
// subscribing from the same client works transparently.
await redis.subscribe('orders:new', (message, channel) => {
  const order = JSON.parse(message)
  received.push(order.id)
  console.log(`  handler             → ${channel}: order ${order.id} (${order.total})`)
})

// Pattern subscriptions receive everything that matches.
await redis.psubscribe('audit:*', (message, channel, pattern) => {
  patternMatches.push(channel)
  console.log(`  pattern handler     → ${pattern} matched ${channel}: ${message}`)
})

// Messages also arrive as events, if a single listener suits you better.
redis.on('message', (channel) => console.log(`  event               → message on ${channel}`))

await redis.publishJson('orders:new', { id: 'ORD-1', total: '99.90' })
await redis.publish('audit:login', 'user 42 signed in')
await redis.publish('audit:logout', 'user 42 signed out')

// Delivery is asynchronous: give the subscriber a moment.
await new Promise((resolve) => setTimeout(resolve, 200))

// Pub/sub has no delivery receipt: publish returns how many subscribers got
// the message, and zero is not an error — it means nobody was listening and
// the message is gone. Check the count when that matters.
const listeners = await redis.publish('orders:new', '{"id":"ORD-2","total":"10.00"}')
console.log(`  subscribers reached → ${listeners}`)

const nobody = await redis.publish('orders:nobody-listens', 'lost')
console.log(`  publish to nobody   → ${nobody} receivers (silently dropped)`)

await new Promise((resolve) => setTimeout(resolve, 100))

await redis.unsubscribe('orders:new')
await redis.publish('orders:new', '{"id":"ORD-3"}')
await new Promise((resolve) => setTimeout(resolve, 100))
console.log(`  after unsubscribe   → ${received.length} orders received (ORD-3 ignored)`)

assert.deepEqual(received, ['ORD-1', 'ORD-2'], 'unsubscribing must stop delivery')
assert.equal(nobody, 0, 'publishing to an empty channel reports zero receivers')
assert.deepEqual(patternMatches.sort(), ['audit:login', 'audit:logout'])

// disconnect() releases the subscriber connection too.
await redis.disconnect()
done(`${received.length} channel messages and ${patternMatches.length} pattern matches delivered`)

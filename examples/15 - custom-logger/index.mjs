// Making the library log through your application's pipeline.

import assert from 'node:assert/strict'
import RedisClient, { createLogger } from '../../src/index.js'
import { baseConfig, heading, done } from '../config.mjs'

heading('15 — Bring your own logger')

// Any object with error/warn/info (and optionally debug) works — a pino,
// winston or bunyan instance drops straight in. Here we capture the calls so
// the example can show what the library actually reports.
const captured = []

const applicationLogger = {
  error: (message, ...args) => captured.push(['error', message, ...args]),
  warn: (message) => captured.push(['warn', message]),
  info: (message) => captured.push(['info', message]),
  debug: (message) => captured.push(['debug', message])
}

const redis = new RedisClient({ ...baseConfig, logger: applicationLogger, keyPrefix: 'example:logging:' })

await redis.connect()

// Hot paths log at debug level only: one line per command would wreck
// throughput, so the log volume must not grow with the work being done.
const afterConnect = captured.length

for (let i = 0; i < 20; i++) {
  await redis.set(`key:${i}`, 'value')
  await redis.get(`key:${i}`)
}

const duringCommands = captured.length - afterConnect

await redis.deleteByPattern('*')
await redis.disconnect()

console.log('  captured by the application logger:')
for (const [level, message] of captured) {
  console.log(`    ${level.padEnd(5)} ${message}`)
}

console.log(`  40 commands produced → ${duringCommands} log entries (lifecycle only)`)

// Without injection you get a dependency-free console logger. It is exported,
// so the same format is available to your own code.
const standalone = createLogger('debug')
console.log('  built-in fallback   → next line comes from createLogger()')
standalone.info('hello from the default logger')

assert.ok(captured.length > 0, 'the injected logger must actually receive the library logs')
assert.equal(duringCommands, 0, 'the hot path must not log at all on the injected logger')

done(`Library logs routed into the application logger (${captured.length} entries)`)

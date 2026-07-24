# Redis

A resilient Redis client for Node.js built on [ioredis](https://github.com/redis/ioredis), with driver-owned automatic reconnection, fail-fast structured errors, observable connection lifecycle events and JSON helpers.

Every reliability claim in this README is enforced by the integration suite against a real Redis — including a test that kills the connection server-side (`CLIENT KILL`) and proves full recovery.

## Features

- **Driver-owned reconnection**: a single ioredis client per connection cycle; exponential backoff (`baseRetryDelay` → `maxRetryDelay`), configurable attempt limit, automatic recovery after server-side kills and failovers (`READONLY` replies reconnect *and* resend the failed command).
- **Fail-fast structured errors**: commands issued while disconnected reject immediately with `code: 'REDIS_UNAVAILABLE'` — a write never looks successful when nothing happened. No network round-trip is added to the hot path.
- **Observable lifecycle**: `RedisClient` is an `EventEmitter` — `ready`, `close`, `reconnecting`, `end` and `connectionError` tell you exactly what the connection is doing.
- **Dedicated connections when they matter**: blocking stream reads (`BLOCK`) never stall the shared connection, and `withDedicatedConnection()` gives you isolated `WATCH`/`MULTI`/`EXEC` optimistic locking that actually works under concurrency.
- **Pub/Sub that survives outages**: subscriptions live on a dedicated connection and are automatically restored after reconnections — enforced by a server-side `CLIENT KILL` test.
- **Distributed locking (single instance)**: `withLock()`/`acquireLock()` with `SET NX PX` acquisition and token-checked Lua release — a holder can never release or extend someone else's lock.
- **Bring your own logger**: inject any pino/winston/bunyan instance; the built-in fallback is a dependency-free leveled console logger.
- **JSON helpers**: `setJson`/`getJson`/`setexJson` with explicit serialization — never magic.
- **Prefixed keyspace scan**: `getAllStream(pattern)` dumps your keys (and only yours — `keyPrefix` is honored, unlike raw `SCAN`), skipping non-string types gracefully.
- **TypeScript declarations**: hand-maintained `index.d.ts`, checked with `tsc --strict` in CI.
- **One runtime dependency**: ioredis. Nothing else.

## Installation

```bash
npm install @pinceladasdaweb/redis
```

Works with `import` and `require`:

```javascript
import RedisClient from '@pinceladasdaweb/redis'
// or
const { RedisClient } = require('@pinceladasdaweb/redis')
```

Requires Node.js >= 22.

## Quick start

```javascript
import RedisClient from '@pinceladasdaweb/redis'

const redis = new RedisClient({ host: '127.0.0.1', port: 6379 })

redis.on('reconnecting', (delay) => console.log(`redis down, retrying in ${delay}ms`))

await redis.connect()

await redis.set('greeting', 'hello')
console.log(await redis.get('greeting')) // 'hello'

await redis.setJson('user:1', { name: 'Ada' })
console.log(await redis.getJson('user:1')) // { name: 'Ada' }

await redis.disconnect()
```

## Constructor options

### Connection

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | — | Redis server hostname |
| `port` | `number` | — | Redis server port |
| `username` | `string` | — | Authentication username |
| `password` | `string` | — | Authentication password |
| `db` | `number` | `0` | Database number |
| `keyPrefix` | `string` | `''` | Prefix applied to every key, including `getAllStream` scans |
| `connectionName` | `string` | — | `CLIENT SETNAME` value; makes the client identifiable in `CLIENT LIST` |

### Reconnection

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `maxRetryAttempts` | `number` | `Infinity` | Attempts before the driver gives up. `0` means never retry |
| `baseRetryDelay` | `number` | `1000` | Base for the exponential backoff (ms) |
| `maxRetryDelay` | `number` | `30000` | Backoff cap (ms) |

Reconnection is handled entirely by the ioredis driver — there is exactly one reconnection loop. When the attempts are exhausted the client emits `end` and releases its resources; a later `connect()` starts a fresh cycle.

### Health check

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `healthCheckInterval` | `number` | `5000` | Minimum interval between real PINGs issued by `checkHealth()` (ms) |
| `healthCheckTimeout` | `number` | `1000` | Timeout for the `checkHealth()` PING (ms) |

`checkHealth()` is an explicit probe for readiness endpoints. Regular commands never pay for a PING: their gate is a local check of the driver's own status.

### Advanced (ioredis passthrough)

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `commandTimeout` | `number` | — | Per-command timeout (ms). No default on purpose: it would break blocking reads |
| `maxRetriesPerRequest` | `number \| null` | `null` | Retries per command |
| `enableReadyCheck` | `boolean` | `true` | Wait for the server to be truly ready |
| `autoResubscribe` | `boolean` | `true` | Resubscribe channels after reconnection |
| `autoResendUnfulfilledCommands` | `boolean` | `true` | Resend in-flight commands after reconnection |
| `lazyConnect` | `boolean` | `true` | Do not connect on instantiation |
| `logger` | `object` | built-in | See [Logging](#logging) |

## Events

| Event | Payload | Fires when |
| --- | --- | --- |
| `ready` | — | The connection is established and the server accepted commands (also after every successful reconnection) |
| `close` | — | The connection dropped |
| `reconnecting` | `delay?: number` | The driver scheduled a reconnection attempt |
| `end` | — | The client is done: after `disconnect()`, or when `maxRetryAttempts` is exhausted |
| `connectionError` | `Error` | The driver reported a connection-level error (never emitted as `'error'`, so an unsubscribed process is never crashed) |
| `message` | `channel, message` | A subscribed channel received a message |
| `pmessage` | `pattern, channel, message` | A pattern subscription received a message |

```javascript
redis.once('ready', () => console.log('connected'))
redis.on('connectionError', (err) => metrics.increment('redis.errors'))
```

## Error handling

All library errors are `RedisClientError` instances carrying `operation` and a stable `code` — branch on the code, never on message text:

| Code | Meaning |
| --- | --- |
| `REDIS_UNAVAILABLE` | The command was rejected because the connection is not ready. Nothing was sent |
| `UNSUPPORTED_OPERATION` | The method cannot work safely on the shared connection (`watch`/`unwatch`) |
| `LOCK_NOT_ACQUIRED` | `acquireLock`/`withLock` could not obtain the lock within the configured retries |
| `REDIS_CLIENT_ERROR` | Generic library error |

```javascript
import { RedisClientError } from '@pinceladasdaweb/redis'

try {
  await redis.set('key', 'value')
} catch (err) {
  if (err instanceof RedisClientError && err.code === 'REDIS_UNAVAILABLE') {
    // Redis is down and reconnecting — degrade gracefully
  } else {
    throw err
  }
}
```

Command errors coming from the server (e.g. `WRONGTYPE`) are ioredis errors and propagate as-is.

## Logging

The library logs through whatever you inject — any object with `error`, `warn`, `info` (and optionally `debug`) methods works, so a pino/winston/bunyan instance plugs in directly:

```javascript
import pino from 'pino'

const redis = new RedisClient({ host: '127.0.0.1', port: 6379, logger: pino() })
```

Without injection you get a dependency-free leveled console logger (default level `info`, configurable via `LOG_LEVEL`). It is exported for reuse:

```javascript
import { createLogger } from '@pinceladasdaweb/redis'

const logger = createLogger('debug')
```

Hot paths log at `debug` level only.

## Transactions and dedicated connections

`multi()` returns an ioredis pipeline for atomic batches on the shared connection:

```javascript
const results = await (await redis.multi()).incr('counter').expire('counter', 60).exec()
```

`WATCH` state is per-connection, so `watch()`/`unwatch()` on the shared connection would be silently broken under concurrency — they reject with `UNSUPPORTED_OPERATION`. For real optimistic locking, use `withDedicatedConnection()`: it hands `fn` a short-lived isolated client (full configuration inherited) and always releases it:

```javascript
const committed = await redis.withDedicatedConnection(async (conn) => {
  await conn.watch('balance')
  const balance = Number(await conn.get('balance'))

  return conn.multi().set('balance', String(balance - 100)).exec()
  // resolves null if 'balance' changed since watch() — retry your logic
})
```

## Pub/Sub

A Redis connection in subscriber mode cannot run regular commands, so subscriptions live on a **dedicated connection** managed by the library (created on the first `subscribe`, released on `disconnect()`). Subscriptions survive reconnections automatically — the integration suite kills the subscriber server-side and proves delivery resumes.

```javascript
await redis.subscribe('news', (message, channel) => {
  console.log(`${channel}: ${message}`)
})

await redis.psubscribe('logs.*', (message, channel, pattern) => {
  console.log(`${pattern} matched ${channel}`)
})

await redis.publish('news', 'hello')
await redis.publishJson('logs.app', { level: 'info' })

await redis.unsubscribe('news')
await redis.punsubscribe('logs.*')
```

Messages also arrive as facade events (`message`, `pmessage`) if you prefer a single listener. Handler rejections are caught and logged — they never crash the process. Note: channels are not keys, so `keyPrefix` does not apply to them.

## Distributed locking

Single-instance locking: acquisition via `SET NX PX`, release and extension via Lua scripts that check the holder token — you can never release or extend a lock you no longer hold. This is a best-effort mutex against one Redis instance, **not** Redlock: no multi-node quorum claims.

```javascript
// Managed: acquire, run, always release
await redis.withLock('reports:daily', { ttl: 60000, retries: 5, retryDelay: 200 }, async () => {
  await generateDailyReport()
})

// Manual control
const lock = await redis.acquireLock('reports:daily', { ttl: 60000 })
try {
  await generateDailyReport()
  await lock.extend(60000) // still holding? reset the ttl
} finally {
  await lock.release() // false if the lock had already expired
}
```

Failing to acquire rejects with `code: 'LOCK_NOT_ACQUIRED'`. Locks are stored as `lock:<name>` (your `keyPrefix` applies). Scripts are cached and transparently reloaded after a server restart (`NOSCRIPT`). Keep the critical section shorter than the `ttl` — the ttl is the safety net that prevents dead holders from blocking everyone forever.

## Streams

All stream commands are available (`xadd`, `xread`, `xreadgroup`, `xgroup`, `xlen`, `xinfo`, `xrange`, `xrevrange`, `xdel`, `xtrim`, `xpending`, `xclaim`). Two behaviors worth knowing:

- **Blocking reads run on a dedicated connection.** `xread`/`xreadgroup` with `block` (including `block: 0`, which blocks forever) never stall other commands.
- **`xgroup` respects each subcommand's arity** — `CREATE` (with optional `MKSTREAM`), `DESTROY`, `SETID`, `CREATECONSUMER`, `DELCONSUMER`.

```javascript
await redis.xadd('events', '*', 'type', 'signup')
await redis.xgroup('CREATE', 'events', 'workers', '$', true) // MKSTREAM

const entries = await redis.xreadgroup('workers', 'worker-1', { count: 10, block: 5000 }, ['events', '>'])
```

## Keyspace scan

`getAllStream(pattern)` returns `[{ key: value }, ...]` for every **string** key matching the pattern *within your `keyPrefix`* (raw `SCAN` ignores ioredis prefixes; this method compensates). Non-string keys are skipped, reads are pipelined per SCAN batch, and returned keys come unprefixed:

```javascript
const redis = new RedisClient({ host, port, keyPrefix: 'myapp:' })
await redis.getAllStream('user:*') // [{ 'user:1': '...' }, { 'user:2': '...' }]
```

## Full method reference

**Connection**: `connect()`, `disconnect()`, `checkHealth()`, `withDedicatedConnection(fn)`
**Strings**: `get`, `set`, `setex`, `incr`, `decr`, `mset`, `mget`
**JSON**: `setJson`, `getJson`, `setexJson`
**Hashes**: `hset` (pairs or object), `hget`, `hgetall`, `hmset` (delegates to `HSET`; returns the number of new fields), `hmget`, `hincrby`, `hexists`, `hdel`
**Lists**: `lpush`, `rpop`, `lrange`, `llen`, `lrem`, `lpushx`, `rpushx`
**Sets**: `sadd`, `smembers`, `sismember`, `scard`, `spop` (single member without `count`, array with it), `srem`
**Keys**: `del`, `exists`, `type`, `rename`, `renamenx`, `persist`, `expire`, `ttl`, `sort`
**Transactions**: `multi()` (`watch`/`unwatch` reject — see above)
**Pub/Sub**: `publish`, `publishJson`, `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe`
**Locking**: `acquireLock`, `withLock`
**Streams**: see [Streams](#streams)
**Scan**: `getAllStream(pattern)`

Anything not wrapped is reachable through `redis.client` (the raw ioredis instance) or `executeCommand(command, ...args)`.

## Notes on semantics

- Values are sent as-is: no implicit JSON serialization anywhere (`mset` included). Use the `*Json` helpers.
- `getJson` returns `null` for missing keys and throws `SyntaxError` on non-JSON payloads.
- A command issued while disconnected rejects with `REDIS_UNAVAILABLE` — it is **not** queued (the tiny race window that slips into the driver's offline queue is resent on reconnection; bound it with `commandTimeout` if needed).

## Development

```bash
docker compose up -d       # pinned redis:7.4-alpine
npm test                   # unit tests (no server required)
npm run test:integration   # full suite against the real Redis, including CLIENT KILL recovery
npm run check:types        # tsc --strict on index.d.ts
```

The integration suite is destructive (it kills connections server-side) — never point it at a shared Redis.

## Contributing

- For a small change, just send a PR.
- For bigger changes open an issue for discussion before sending a PR.
- PRs should come with tests — reliability claims here are only as good as the suite that enforces them.

## Author

Pedro Rogério — [GitHub](https://github.com/pinceladasdaweb)

## License

[MIT](LICENSE)

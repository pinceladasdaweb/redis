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
- **Cache-aside with stampede protection**: `getOrSet`/`getOrSetJson` return the cached value or produce-and-store it — and with `{ lock: true }`, concurrent misses collapse into a single producer call.
- **Bulk deletion done right**: `deleteByPattern` uses `SCAN` + `UNLINK` in batches (non-blocking, prefix-aware) instead of `KEYS`.
- **Sentinel support**: pass `sentinels` + `name` and the client rides ioredis' native high-availability failover.
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

### High availability (Sentinel)

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `sentinels` | `Array<{host, port}>` | — | Sentinel nodes; providing this enables sentinel mode |
| `name` | `string` | — | Master group name to resolve (e.g. `'mymaster'`) |
| `sentinelPassword` | `string` | — | Password for the sentinel nodes themselves |
| `role` | `'master' \| 'slave'` | `'master'` | Which role to connect to |

```javascript
const redis = new RedisClient({
  sentinels: [{ host: 'sentinel-1', port: 26379 }, { host: 'sentinel-2', port: 26379 }],
  name: 'mymaster'
})
```

Failover is handled by ioredis: on `READONLY` replies the client reconnects to the new master and resends the failed command.

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
| `INVALID_ARGUMENT` | A required argument is missing or malformed (e.g. `xtrim` without a count) |
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

## Caching (cache-aside)

`getOrSet`/`getOrSetJson` implement the read-through pattern: return the cached value, or run the producer, store its result with the ttl and return it.

```javascript
const user = await redis.getOrSetJson(`user:${id}`, 300, () => db.loadUser(id))
```

Under load, an expired hot key means N concurrent misses running N producers (the dogpile/stampede effect). Enable the built-in protection and they collapse into **one** producer call — the winner fills the cache while the others wait on a lock and re-read:

```javascript
const report = await redis.getOrSetJson('report:daily', 3600, buildExpensiveReport, { lock: true })
// lock accepts LockOptions too: { lock: { ttl: 30000, retries: 200 } }
```

Guarantees worth knowing:

- The cache lock auto-extends by default, so a producer slower than the lock ttl does not reopen the stampede.
- A cache call never surfaces lock errors: a waiter that exhausts its retry budget re-reads the cache (the winner has usually filled it by then) and, as a last resort, runs the producer without protection.
- The producer's value must be cacheable — `getOrSet` accepts strings and numbers, `getOrSetJson` anything JSON-serializable. Anything else (including `undefined`) rejects with `INVALID_ARGUMENT` **without writing to the cache**.

To invalidate, delete by pattern — `SCAN` + `UNLINK` in batches (non-blocking, never `KEYS`), confined to your `keyPrefix`:

```javascript
const removed = await redis.deleteByPattern('user:*')
// The pattern is required: deleteByPattern('*') wipes the whole prefixed keyspace, so say it explicitly.
```

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

Each channel/pattern holds **one handler** — subscribing again replaces it (last one wins). Use the `message`/`pmessage` events when you need fan-out to multiple listeners. If the subscriber connection permanently gives up (finite `maxRetryAttempts` exhausted), a warning is logged and the next `subscribe()` starts a fresh connection — resubscribe to restore delivery.

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

Failing to acquire rejects with `code: 'LOCK_NOT_ACQUIRED'`. Locks are stored as `lock:<name>` (your `keyPrefix` applies). Scripts are cached and transparently reloaded after a server restart (`NOSCRIPT`).

Two options worth knowing:

- **`retryJitter`** adds a random extra delay (0..n ms) per acquisition attempt — under contention, fixed delays make every waiter retry in lockstep.
- **`autoExtend: true`** (in `withLock` only) starts a watchdog that keeps extending the lock at half-ttl intervals while your callback runs — for critical sections that may outlive the ttl. If the lock is definitively lost, the watchdog stops and logs a warning.

```javascript
await redis.withLock('report:build', { ttl: 30000, autoExtend: true, retries: 10, retryJitter: 100 }, async () => {
  await possiblyVerySlowJob()
})
```

Without `autoExtend`, keep the critical section shorter than the `ttl` — the ttl is the safety net that prevents dead holders from blocking everyone forever.

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
**Cache**: `getOrSet`, `getOrSetJson`, `deleteByPattern`
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
npm run hooks              # once per clone: enable the repo's git hooks (lint, commitlint)
docker compose up -d       # pinned redis:7.4-alpine
npm test                   # unit tests (no server required)
npm run test:integration   # full suite against the real Redis, including CLIENT KILL recovery
npm run check:types        # tsc --strict on index.d.ts
```

The published package declares **zero lifecycle scripts**, so it installs without any `allow-scripts` approval friction.

The integration suite is destructive (it kills connections server-side) — never point it at a shared Redis.

## Contributing

- For a small change, just send a PR.
- For bigger changes open an issue for discussion before sending a PR.
- PRs should come with tests — reliability claims here are only as good as the suite that enforces them.

## Author

Pedro Rogério — [GitHub](https://github.com/pinceladasdaweb)

## License

[MIT](LICENSE)

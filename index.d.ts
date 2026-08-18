/// <reference types="node" />

import { EventEmitter } from 'node:events'
import { Redis, Cluster, ChainableCommander, RedisOptions, ClusterOptions } from 'ioredis'

export interface Logger {
  error: (message: string, ...args: unknown[]) => void
  warn: (message: string, ...args: unknown[]) => void
  info: (message: string, ...args: unknown[]) => void
  debug?: (message: string, ...args: unknown[]) => void
}

export declare function createLogger (level?: string): Logger

/**
 * Everything ioredis accepts — host/port/password, tls, connectTimeout,
 * keepAlive, family, path, natMap, enableOfflineQueue, the sentinel options —
 * forwarded to the driver untouched, plus this library's own options below.
 *
 * `retryStrategy` and `reconnectOnError` are deliberately absent: the library
 * owns them, and passing either rejects at construction with INVALID_OPTION.
 * Shape the backoff with maxRetryAttempts / baseRetryDelay / maxRetryDelay.
 */
export interface RedisClientOptions extends Omit<RedisOptions, 'retryStrategy' | 'reconnectOnError'>, Omit<ClusterOptions, 'redisOptions' | 'clusterRetryStrategy'> {
  /**
   * Startup nodes of a Redis Cluster. Providing them switches the client to
   * cluster mode: slots are discovered, MOVED/ASK redirections are followed,
   * and multi-key commands require a shared hash slot.
   */
  nodes?: Array<{ host: string, port: number }>
  /** Maximum reconnection attempts before the driver gives up. 0 means never retry. Default: Infinity. */
  maxRetryAttempts?: number
  /** Base delay in ms for the exponential reconnection backoff. Default: 1000. */
  baseRetryDelay?: number
  /** Cap in ms for the exponential reconnection backoff. Default: 30000. */
  maxRetryDelay?: number
  /** Minimum interval in ms between real PINGs issued by checkHealth(). Default: 5000. */
  healthCheckInterval?: number
  /** Timeout in ms for the checkHealth() PING. Default: 1000. */
  healthCheckTimeout?: number
  /** Any pino/winston/bunyan-compatible instance. Default: built-in leveled console logger. */
  logger?: Logger
}

export type RedisClientErrorCode = 'REDIS_UNAVAILABLE' | 'UNSUPPORTED_OPERATION' | 'LOCK_NOT_ACQUIRED' | 'INVALID_ARGUMENT' | 'INVALID_OPTION' | 'KEYSPACE_NOTIFICATIONS_DISABLED' | 'OPERATION_TIMEOUT' | 'REDIS_CLIENT_ERROR'

export declare class RedisClientError extends Error {
  name: 'RedisClientError'
  operation: string
  code: RedisClientErrorCode | string
  /**
   * On `LOCK_NOT_ACQUIRED` only: which lock refused. `code` and `operation`
   * are the same for every lock, so this is how a caller holding locks inside
   * a `getOrSet` producer tells its own failure from the cache's.
   */
  lockName?: string
  constructor (message: string, operation: string, code?: string)
}

/** A sorted-set member with its score already parsed (infinities included). */
export interface ScoredMember {
  member: string
  score: number
}

export interface SortedSetRangeOptions {
  /** Return { member, score } pairs instead of bare members. */
  withScores?: boolean
  /** Interpret start/stop as scores (ZRANGE only). */
  byScore?: boolean
  /** Interpret start/stop as lexicographic ranges (ZRANGE only). */
  byLex?: boolean
  /** Reverse the ordering (ZRANGE only). */
  rev?: boolean
  /** Requires byScore or byLex on ZRANGE; always available on ZRANGEBYSCORE. */
  limit?: { offset: number, count: number }
}

export interface SortOptions {
  by?: string
  limit?: { offset: number, count: number }
  get?: string
  direction?: 'ASC' | 'DESC'
  alpha?: boolean
}

export interface StreamReadOptions {
  count?: number
  /** Milliseconds to block; 0 blocks forever. Blocking reads run on a dedicated connection. */
  block?: number
}

export interface StreamGroupReadOptions extends StreamReadOptions {
  noack?: boolean
}

export interface StreamRangeOptions {
  count?: number
}

/**
 * The extended form of XPENDING. `start`, `end` and `count` travel together:
 * a partial range is rejected with `INVALID_ARGUMENT` rather than silently
 * answering the other question (the group summary, in a different shape).
 */
export interface StreamPendingOptions {
  start: string
  end: string
  count: number
  consumer?: string
}

export interface StreamAutoClaimOptions {
  count?: number
  /** Return only the ids, without the field/value payloads. */
  justId?: boolean
}

export interface AutoClaimResult {
  /** Where to resume the sweep; '0-0' means the pending list was covered. */
  cursor: string
  entries: StreamEntry[]
  /** Ids that were pending for entries no longer in the stream. */
  deleted: string[]
}

export type StreamEntry = [id: string, fields: string[]]
export type StreamReadResult = Array<[key: string, entries: StreamEntry[]]> | null

export type RedisClientEvent = 'ready' | 'close' | 'reconnecting' | 'end' | 'connectionError' | 'message' | 'pmessage'

/** Handler for subscribe()/psubscribe(). `pattern` is set for pattern subscriptions only. */
export type PubSubHandler = (message: string, channel: string, pattern?: string) => void | Promise<void>

/**
 * A Lua script registered once and called by name from then on. The driver
 * sends the SHA rather than the body, reloads it on `NOSCRIPT` after a restart
 * or failover, and reinstalls it on the new client after a reconnection.
 */
export interface ScriptDefinition {
  /** The Lua source. */
  lua: string
  /**
   * How many of the arguments are KEYS. Required: it is what makes the script
   * routable in a cluster and prefixable at all, and `runScript` checks the
   * count on every call.
   */
  numberOfKeys: number
  /** Marks the script as a reader, so `scaleReads` may route it to a replica. */
  readOnly?: boolean
}

export interface LockOptions {
  /** Lock lifetime in ms. The critical section must finish within it (or use autoExtend). Default: 30000. */
  ttl?: number
  /** Extra acquisition attempts when the lock is taken. Default: 0. */
  retries?: number
  /** Delay between acquisition attempts (ms). Default: 100. */
  retryDelay?: number
  /** Random extra delay (0..n ms) added per attempt to avoid retry lockstep under contention. Default: 0. */
  retryJitter?: number
  /** withLock() only: keep extending the lock at half-ttl intervals while fn runs. Default: false. */
  autoExtend?: boolean
}

export interface CacheAsideOptions {
  /**
   * Stampede protection: true (defaults: ttl 10s auto-extended, 100 retries
   * of 50ms+jitter) or LockOptions to tune. Concurrent misses collapse into
   * one producer call. Lock errors never surface from cache calls: an
   * exhausted retry budget falls back to re-read, then unprotected produce.
   */
  lock?: boolean | LockOptions
}

export interface Lock {
  name: string
  token: string
  /** Deletes the lock only if still held by this token. False means it was already gone. */
  release (): Promise<boolean>
  /** Resets the ttl only if still held by this token. */
  extend (ttlMs?: number): Promise<boolean>
}

export declare class RedisClient extends EventEmitter {
  constructor (options?: RedisClientOptions)

  /** The underlying ioredis instance (a Cluster when `nodes` was given), or null while disconnected. */
  readonly client: Redis | Cluster | null
  readonly isConnected: boolean
  logger: Logger
  keyPrefix: string

  on (event: 'ready' | 'close' | 'end', listener: () => void): this
  on (event: 'reconnecting', listener: (delay?: number) => void): this
  on (event: 'connectionError', listener: (err: Error) => void): this
  on (event: 'message', listener: (channel: string, message: string) => void): this
  on (event: 'pmessage', listener: (pattern: string, channel: string, message: string) => void): this
  on (event: string | symbol, listener: (...args: unknown[]) => void): this

  once (event: 'ready' | 'close' | 'end', listener: () => void): this
  once (event: 'reconnecting', listener: (delay?: number) => void): this
  once (event: 'connectionError', listener: (err: Error) => void): this
  once (event: 'message', listener: (channel: string, message: string) => void): this
  once (event: 'pmessage', listener: (pattern: string, channel: string, message: string) => void): this
  once (event: string | symbol, listener: (...args: unknown[]) => void): this

  connect (): Promise<void>
  disconnect (): Promise<void>
  /** Explicit PING probe with a timeout; results are shared within healthCheckInterval. */
  checkHealth (): Promise<boolean>

  /**
   * Runs fn with a short-lived dedicated connection (full configuration
   * inherited, always released). Required for WATCH/MULTI/EXEC and anything
   * else that must not share the main connection.
   *
   * If disconnect() runs while fn is still waiting, the connection is
   * reclaimed and the call rejects with REDIS_UNAVAILABLE.
   */
  withDedicatedConnection<T> (fn: (client: Redis) => T | Promise<T>): Promise<T>

  executeCommand (command: string, ...args: unknown[]): Promise<unknown>
  executeBlockingCommand (command: string, args: unknown[]): Promise<unknown>

  get (key: string): Promise<string | null>
  set (key: string, value: string | number | Buffer): Promise<'OK'>
  setex (key: string, seconds: number, value: string | number | Buffer): Promise<'OK'>
  del (...keys: string[]): Promise<number>
  incr (key: string): Promise<number>
  decr (key: string): Promise<number>
  exists (key: string): Promise<number>
  type (key: string): Promise<string>
  rename (key: string, newkey: string): Promise<'OK'>
  renamenx (key: string, newkey: string): Promise<number>
  persist (key: string): Promise<number>
  expire (key: string, seconds: number): Promise<number>
  ttl (key: string): Promise<number>

  setJson (key: string, value: unknown): Promise<'OK'>
  getJson<T = unknown> (key: string): Promise<T | null>
  setexJson (key: string, seconds: number, value: unknown): Promise<'OK'>

  /** Cache-aside: cached value, or produce + SETEX + return. String values only — see getOrSetJson. */
  getOrSet (key: string, ttlSeconds: number, producer: () => string | number | Promise<string | number>, options?: CacheAsideOptions): Promise<string>
  /** Cache-aside with JSON serialization. */
  getOrSetJson<T = unknown> (key: string, ttlSeconds: number, producer: () => T | Promise<T>, options?: CacheAsideOptions): Promise<T>
  /** SCAN + UNLINK batches inside the prefixed keyspace. Returns the number of keys removed. */
  deleteByPattern (pattern: string): Promise<number>

  hset (key: string, field: string, value: string | number): Promise<number>
  hset (key: string, obj: Record<string, string | number>): Promise<number>
  hset (key: string, ...fieldValuePairs: Array<string | number>): Promise<number>
  hget (key: string, field: string): Promise<string | null>
  hgetall (key: string): Promise<Record<string, string>>
  /** Delegates to variadic HSET (HMSET is deprecated); returns the number of new fields. */
  hmset (key: string, obj: Record<string, string | number>): Promise<number>
  hmget (key: string, ...fields: string[]): Promise<Array<string | null>>
  hincrby (key: string, field: string, increment: number): Promise<number>
  hexists (key: string, field: string): Promise<number>
  hdel (key: string, ...fields: string[]): Promise<number>

  lpush (key: string, ...values: Array<string | number>): Promise<number>
  rpop (key: string): Promise<string | null>
  lrange (key: string, start: number, stop: number): Promise<string[]>
  llen (key: string): Promise<number>
  lrem (key: string, count: number, value: string): Promise<number>
  lpushx (key: string, value: string | number): Promise<number>
  rpushx (key: string, value: string | number): Promise<number>

  sadd (key: string, ...members: Array<string | number>): Promise<number>
  smembers (key: string): Promise<string[]>
  sismember (key: string, member: string): Promise<number>
  scard (key: string): Promise<number>
  /** Without a count Redis returns a single member; with one it returns an array. */
  spop (key: string): Promise<string | null>
  spop (key: string, count: number): Promise<string[]>
  srem (key: string, ...members: string[]): Promise<number>

  /** Adds members as { member: score }, or passes ioredis arguments through (score first, plus flags). */
  zadd (key: string, members: Record<string, number | string>): Promise<number>
  zadd (key: string, ...args: Array<string | number>): Promise<number | string>
  /** The member's score as a number, or null when it is not in the set. */
  zscore (key: string, member: string): Promise<number | null>
  /** The new score after the increment. */
  zincrby (key: string, increment: number, member: string): Promise<number>
  zcard (key: string): Promise<number>
  zcount (key: string, min: number | string, max: number | string): Promise<number>
  /** Zero-based position, or null when the member is absent. */
  zrank (key: string, member: string): Promise<number | null>
  zrevrank (key: string, member: string): Promise<number | null>
  zrem (key: string, ...members: string[]): Promise<number>
  zrange (key: string, start: number | string, stop: number | string, options: SortedSetRangeOptions & { withScores: true }): Promise<ScoredMember[]>
  zrange (key: string, start: number | string, stop: number | string, options?: SortedSetRangeOptions): Promise<string[]>
  zrevrange (key: string, start: number, stop: number, options: SortedSetRangeOptions & { withScores: true }): Promise<ScoredMember[]>
  zrevrange (key: string, start: number, stop: number, options?: SortedSetRangeOptions): Promise<string[]>
  zrangebyscore (key: string, min: number | string, max: number | string, options: SortedSetRangeOptions & { withScores: true }): Promise<ScoredMember[]>
  zrangebyscore (key: string, min: number | string, max: number | string, options?: SortedSetRangeOptions): Promise<string[]>
  zremrangebyrank (key: string, start: number, stop: number): Promise<number>
  zremrangebyscore (key: string, min: number | string, max: number | string): Promise<number>
  /** Without a count: the single lowest-scored member, or null on an empty set. */
  zpopmin (key: string): Promise<ScoredMember | null>
  zpopmin (key: string, count: number): Promise<ScoredMember[]>
  zpopmax (key: string): Promise<ScoredMember | null>
  zpopmax (key: string, count: number): Promise<ScoredMember[]>

  sort (key: string, options?: SortOptions): Promise<string[]>

  /** Values are sent as-is — no JSON serialization. Use the *Json helpers for objects. */
  mset (obj: Record<string, string | number>): Promise<'OK'>
  mget (...keys: string[]): Promise<Array<string | null>>

  multi (): Promise<ChainableCommander>
  /** Always rejects with UNSUPPORTED_OPERATION — use withDedicatedConnection(). */
  watch (...keys: string[]): Promise<never>
  /** Always rejects with UNSUPPORTED_OPERATION — use withDedicatedConnection(). */
  unwatch (): Promise<never>

  /** Channels are not keys: keyPrefix does not apply to pub/sub. */
  publish (channel: string, message: string | number | Buffer): Promise<number>
  publishJson (channel: string, value: unknown): Promise<number>
  /**
   * Subscriptions live on a dedicated connection and survive reconnections.
   * One handler per channel — a re-subscribe replaces it (last one wins);
   * use the 'message' event for fan-out.
   */
  subscribe (channel: string, handler?: PubSubHandler): Promise<unknown>
  unsubscribe (channel: string): Promise<unknown>
  psubscribe (pattern: string, handler?: PubSubHandler): Promise<unknown>
  punsubscribe (pattern: string): Promise<unknown>

  /** The server's current `notify-keyspace-events` flags (empty when disabled). */
  keyspaceNotifications (): Promise<string>
  /**
   * Subscribes to `__keyevent@<db>__:<event>` after checking the server is
   * configured to emit it — otherwise a silent channel would be
   * indistinguishable from a working one. Rejects with
   * KEYSPACE_NOTIFICATIONS_DISABLED, naming the CONFIG SET that fixes it.
   */
  subscribeToKeyEvents (event: string, handler?: PubSubHandler, options?: { db?: number }): Promise<unknown>

  /** Single-instance lock (SET NX PX + token-checked Lua release). Not Redlock. */
  /**
   * Registers Lua under a name. Prefer this over `executeCommand('eval', …)`
   * for anything on a hot path: `EVAL` ships the whole program on every call,
   * a registered script ships its SHA. Registration is lazy — no connection
   * needed — and survives reconnection cycles.
   *
   * Throws `INVALID_ARGUMENT` on a malformed definition, at registration
   * rather than at first use.
   */
  defineScript (name: string, definition: ScriptDefinition): void
  /**
   * Runs a registered script. Keys and arguments travel as two arrays, and the
   * key count declared at registration is enforced here: a misplaced key would
   * otherwise read something nobody named, on a node nobody meant to reach.
   *
   * Rejects with `INVALID_ARGUMENT` for an unknown name or the wrong number of
   * keys; a server-side script error propagates as the driver's own error.
   */
  runScript (name: string, keys?: Array<string | number>, args?: Array<string | number>): Promise<unknown>

  acquireLock (name: string, options?: LockOptions): Promise<Lock>
  withLock<T> (name: string, fn: (lock: Lock) => T | Promise<T>): Promise<T>
  withLock<T> (name: string, options: LockOptions, fn: (lock: Lock) => T | Promise<T>): Promise<T>

  xadd (key: string, id: string, ...args: Array<string | number>): Promise<string>
  xread (options: StreamReadOptions, streams: string[]): Promise<StreamReadResult>
  xreadgroup (groupName: string, consumerName: string, options: StreamGroupReadOptions, streams: string[]): Promise<StreamReadResult>
  xgroup (command: 'CREATE' | 'DESTROY' | 'SETID' | 'CREATECONSUMER' | 'DELCONSUMER' | string, key: string, groupName: string, ...rest: unknown[]): Promise<unknown>
  xlen (key: string): Promise<number>
  xinfo (subcommand: string, key: string, ...args: unknown[]): Promise<unknown>
  xrange (key: string, start: string, end: string, options?: StreamRangeOptions): Promise<StreamEntry[]>
  xrevrange (key: string, end: string, start: string, options?: StreamRangeOptions): Promise<StreamEntry[]>
  xdel (key: string, ...ids: string[]): Promise<number>
  /** `count` is required at runtime — omitting it rejects with INVALID_ARGUMENT. */
  xtrim (key: string, strategy: string, approx: boolean, count: number): Promise<number>
  /** No options: the group summary, `[total, minId, maxId, consumers]`. */
  xpending (key: string, group: string): Promise<unknown>
  /** With a full range: the pending entries themselves. */
  xpending (key: string, group: string, options: StreamPendingOptions): Promise<unknown>
  /** Acknowledges entries so they leave the consumer group's pending list. */
  xack (key: string, group: string, ...ids: string[]): Promise<number>
  /**
   * Sweeps the group's pending list for entries idle longer than
   * `minIdleTime` and hands them to `consumer` — the recovery path for a
   * consumer that died holding deliveries. Returned as named fields instead
   * of the positional reply.
   */
  xautoclaim (key: string, group: string, consumer: string, minIdleTime: number, start?: string, options?: StreamAutoClaimOptions): Promise<AutoClaimResult>
  xclaim (key: string, group: string, consumer: string, minIdleTime: number, ...ids: string[]): Promise<StreamEntry[]>

  /** SCAN-based dump of the (prefixed) keyspace: [{ key: value }, ...]. Skips non-string keys. */
  getAllStream (pattern?: string): Promise<Array<Record<string, string>>>

  omitPrefix (key: string): string
}

export default RedisClient

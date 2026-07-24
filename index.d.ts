/// <reference types="node" />

import { EventEmitter } from 'node:events'
import { Redis, ChainableCommander } from 'ioredis'

export interface Logger {
  error: (message: string, ...args: unknown[]) => void
  warn: (message: string, ...args: unknown[]) => void
  info: (message: string, ...args: unknown[]) => void
  debug?: (message: string, ...args: unknown[]) => void
}

export declare function createLogger (level?: string): Logger

export interface RedisClientOptions {
  /** Redis server hostname. */
  host?: string
  /** Redis server port. */
  port?: number
  username?: string
  password?: string
  /** Database number. */
  db?: number
  /** Prefix automatically applied to every key (including SCAN in getAllStream). */
  keyPrefix?: string
  /** CLIENT SETNAME value — makes this client identifiable in CLIENT LIST. */
  connectionName?: string
  /** Per-command timeout in ms (ioredis passthrough). No default: blocking reads would break. */
  commandTimeout?: number
  /** Maximum reconnection attempts before the driver gives up. 0 means never retry. Default: Infinity. */
  maxRetryAttempts?: number
  /** Base delay in ms for the exponential reconnection backoff. Default: 1000. */
  baseRetryDelay?: number
  /** Cap in ms for the exponential reconnection backoff. Default: 30000. */
  maxRetryDelay?: number
  /** ioredis passthrough. Default: null (unlimited). */
  maxRetriesPerRequest?: number | null
  /** ioredis passthrough. Default: true. */
  enableReadyCheck?: boolean
  /** ioredis passthrough. Default: true. */
  autoResubscribe?: boolean
  /** ioredis passthrough. Default: true. */
  autoResendUnfulfilledCommands?: boolean
  /** ioredis passthrough. Default: true. */
  lazyConnect?: boolean
  /** Minimum interval in ms between real PINGs issued by checkHealth(). Default: 5000. */
  healthCheckInterval?: number
  /** Timeout in ms for the checkHealth() PING. Default: 1000. */
  healthCheckTimeout?: number
  /** Any pino/winston/bunyan-compatible instance. Default: built-in leveled console logger. */
  logger?: Logger
}

export type RedisClientErrorCode = 'REDIS_UNAVAILABLE' | 'UNSUPPORTED_OPERATION' | 'LOCK_NOT_ACQUIRED' | 'INVALID_ARGUMENT' | 'REDIS_CLIENT_ERROR'

export declare class RedisClientError extends Error {
  name: 'RedisClientError'
  operation: string
  code: RedisClientErrorCode | string
  constructor (message: string, operation: string, code?: string)
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

export interface StreamPendingOptions {
  start?: string
  end?: string
  count?: number
  consumer?: string
}

export type StreamEntry = [id: string, fields: string[]]
export type StreamReadResult = Array<[key: string, entries: StreamEntry[]]> | null

export type RedisClientEvent = 'ready' | 'close' | 'reconnecting' | 'end' | 'connectionError' | 'message' | 'pmessage'

/** Handler for subscribe()/psubscribe(). `pattern` is set for pattern subscriptions only. */
export type PubSubHandler = (message: string, channel: string, pattern?: string) => void | Promise<void>

export interface LockOptions {
  /** Lock lifetime in ms. The critical section must finish within it. Default: 30000. */
  ttl?: number
  /** Extra acquisition attempts when the lock is taken. Default: 0. */
  retries?: number
  /** Delay between acquisition attempts (ms). Default: 100. */
  retryDelay?: number
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

  /** The underlying ioredis instance, or null while disconnected. */
  readonly client: Redis | null
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

  /** Single-instance lock (SET NX PX + token-checked Lua release). Not Redlock. */
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
  xpending (key: string, group: string, options?: StreamPendingOptions): Promise<unknown>
  xclaim (key: string, group: string, consumer: string, minIdleTime: number, ...ids: string[]): Promise<StreamEntry[]>

  /** SCAN-based dump of the (prefixed) keyspace: [{ key: value }, ...]. Skips non-string keys. */
  getAllStream (pattern?: string): Promise<Array<Record<string, string>>>

  omitPrefix (key: string): string
}

export default RedisClient

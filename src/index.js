import { EventEmitter } from 'node:events'
import { createClock } from './utils/clock.js'
import LockManager from './resilience/lock.js'
import withDeadline from './utils/deadline.js'
import RedisConfig from './connection/config.js'
import RedisClientError from './utils/errors.js'
import HealthChecker from './connection/health.js'
import ScriptRegistry from './scripting/scripts.js'
import SubscriptionManager from './messaging/pubsub.js'
import ConnectionManager from './connection/manager.js'
import Logger, { createLogger } from './utils/logger.js'
import { isCluster, masters, nodeKey } from './utils/cluster.js'
import { parseScore, parseScoredMembers } from './utils/scores.js'
import scanKeyspace, { deletePattern, omitPrefixWith } from './keyspace/scanner.js'

// Which notify-keyspace-events class each event needs — the full relation
// from redis.conf, not a sample of it. The probe's whole purpose is turning a
// silent subscription into an error, and it can only do that for an event
// whose class it knows; for years it knew fourteen and quietly checked only
// the 'E' flag for everything else, so `subscribeToKeyEvents('hdel')` against
// a server configured "Ex" installed a channel that would never speak. An
// event absent from this table is now reported as unverifiable rather than
// waved through.
const KEY_EVENT_CLASSES = {
  // g — generic commands
  del: 'g',
  rename_from: 'g',
  rename_to: 'g',
  move_from: 'g',
  move_to: 'g',
  copy_to: 'g',
  restore: 'g',
  expire: 'g',
  persist: 'g',
  sortstore: 'g',
  // $ — strings
  set: '$',
  setrange: '$',
  incrby: '$',
  incrbyfloat: '$',
  append: '$',
  // l — lists
  lpush: 'l',
  rpush: 'l',
  lpop: 'l',
  rpop: 'l',
  linsert: 'l',
  lset: 'l',
  lrem: 'l',
  ltrim: 'l',
  // s — sets
  sadd: 's',
  srem: 's',
  spop: 's',
  sinterstore: 's',
  sunionstore: 's',
  sdiffstore: 's',
  // h — hashes
  hset: 'h',
  hincrby: 'h',
  hincrbyfloat: 'h',
  hdel: 'h',
  hexpire: 'h',
  hexpired: 'h',
  hpersist: 'h',
  // z — sorted sets
  zadd: 'z',
  zincr: 'z',
  zrem: 'z',
  zrembyscore: 'z',
  zrembyrank: 'z',
  zrembylex: 'z',
  zdiffstore: 'z',
  zinterstore: 'z',
  zunionstore: 'z',
  zpopmin: 'z',
  zpopmax: 'z',
  // t — streams
  xadd: 't',
  xdel: 't',
  'xgroup-create': 't',
  'xgroup-createconsumer': 't',
  'xgroup-delconsumer': 't',
  'xgroup-destroy': 't',
  'xgroup-setid': 't',
  xsetid: 't',
  xtrim: 't',
  // x, e, n, m — one event each
  expired: 'x',
  evicted: 'e',
  new: 'n',
  keymiss: 'm'
}

// The JSON helpers share one encoder so they agree on what cannot be stored.
// JSON.stringify returns undefined for undefined/functions/symbols; ioredis
// serializes that argument as '' — a key that exists, reads as a miss through
// getJson, and makes every getOrSetJson on it throw a raw SyntaxError until it
// expires. getOrSetJson refused that at its producer; setJson wrote it.
const encodeJson = (value, operation) => {
  const encoded = JSON.stringify(value)

  if (typeof encoded !== 'string') {
    throw new RedisClientError(
      `${operation} requires a JSON-serializable value (got undefined, a function or a symbol).`,
      operation,
      'INVALID_ARGUMENT'
    )
  }

  return encoded
}

// How many idle connections the blocking-read pool keeps around. A consumer
// loop reuses one connection instead of paying a full handshake per iteration
// (a whole cluster pool per iteration, in cluster mode); the cap stops a
// concurrency spike from leaving sockets parked forever.
const MAX_IDLE_BLOCKING_CONNECTIONS = 4

// How long the keyspace CONFIG probe may wait for an answer. Providers that
// restrict CONFIG usually error immediately; this bounds the ones that hang.
const CONFIG_PROBE_DEADLINE_MS = 2000

// Thin facade: wires the collaborators together through a small context
// (logger, config, emit) and exposes the command surface. Mutable state is
// always reached through getters — never captured references.
class RedisClient extends EventEmitter {
  // Dedicated connections currently in use, so shutdown can reclaim them.
  #dedicated = new Set()
  // Connections a blocking read finished with, ready for the next one.
  #idleBlocking = []
  // The disconnect() in flight, if any. connect() waits for it: the facade has
  // steps to run before the driver is told (subscribers, dedicated
  // connections), and a connect() landing during those found a manager that
  // knew nothing of the teardown and "reused" the client about to be quit —
  // resolving connected, with nothing behind it a moment later.
  #shutdown = null

  constructor (options = {}) {
    super()

    const retryConfig = {
      maxRetryAttempts: options.maxRetryAttempts ?? Infinity,
      baseRetryDelay: options.baseRetryDelay ?? 1000,
      maxRetryDelay: options.maxRetryDelay ?? 30000
    }

    this.logger = options.logger || Logger

    this.redisConfig = new RedisConfig({
      ...options,
      ...retryConfig,
      logger: this.logger
    })

    this.config = this.redisConfig.getOptions()
    // Read from the config object, not from the driver options: under cluster
    // these live inside redisOptions.
    this.keyPrefix = this.redisConfig.keyPrefix

    // One clock for the whole facade: every timer and every reading of "now"
    // in the collaborators goes through it, so time is drivable in tests.
    this.clock = options.clock ?? createClock()

    this.connection = new ConnectionManager({
      redisConfig: this.redisConfig,
      logger: this.logger,
      clock: this.clock,
      emit: (event, ...args) => this.emit(event, ...args)
    })

    this.health = new HealthChecker({
      getClient: () => this.connection.client,
      logger: this.logger,
      clock: this.clock,
      interval: options.healthCheckInterval ?? 5000,
      timeout: options.healthCheckTimeout ?? 1000
    })

    this.subscriptions = new SubscriptionManager({
      connection: this.connection,
      logger: this.logger,
      clock: this.clock,
      emit: (event, ...args) => this.emit(event, ...args),
      // The library's own backoff, for the per-node keyspace subscribers only.
      // Cluster node connections carry the driver's `retryStrategy: null` and
      // a duplicate() inherits it; without this a subscriber died on the first
      // socket blip. It must reach the SUBSCRIBER sockets and nothing else —
      // giving the pool's nodes a retry policy (via clusterNodeRetryStrategy)
      // defeats the driver's failover detection, which relies on a dead node
      // ENDING to notice it is gone.
      retryStrategy: this.redisConfig.retryStrategy.bind(this.redisConfig)
    })

    this.locks = new LockManager({
      connection: this.connection,
      logger: this.logger,
      clock: this.clock
    })

    this.scripts = new ScriptRegistry({
      connection: this.connection,
      logger: this.logger
    })

    // The pool holds duplicate()s of the CURRENT client. When that cycle ends
    // — quit or the driver giving up — those duplicates would otherwise keep
    // their own infinite retry loops alive, invisible to the facade (their
    // errors only reach logger.debug). The cycle's end is the pool's end.
    this.on('end', () => this.#releaseDedicatedConnections())
  }

  get client () {
    return this.connection.client
  }

  get isConnected () {
    return this.connection.isConnected
  }

  async connect () {
    // A shutdown still running owns the client. Waiting here — rather than in
    // the manager, which has not been told yet — is what lets "connect after
    // disconnect" mean a fresh cycle instead of a handshake with a corpse.
    if (this.#shutdown) {
      await this.#shutdown
    }

    return this.connection.connect()
  }

  async disconnect () {
    if (this.#shutdown) {
      return this.#shutdown
    }

    // The gate closes FIRST, before any of the steps below: quit() does not
    // flip the driver's status synchronously, so without this work arriving
    // during the teardown would still pass assertReady and create connections
    // after their reapers ran — sockets nobody cancels, parked blocking reads
    // that keep the process alive.
    this.connection.beginShutdown()

    const shutdown = (async () => {
      try {
        await this.subscriptions.close()
        this.#releaseDedicatedConnections()
        await this.connection.disconnect()
      } finally {
        // An in-flight health probe holds a deliberately ref'd timer (it is
        // awaited); on a wedged server nothing would ever settle it, and the
        // loop would stay open for up to healthCheckTimeout after this method
        // resolved. Cancelling is the shutdown's job, not the probe's.
        this.health.stop()
        // Second sweeps, for whatever the awaits above let through: a lease
        // or a subscription that was mid-flight when the gate closed and
        // finished creating its connection after the first sweep ran.
        this.#releaseDedicatedConnections()
        await this.subscriptions.close()
      }
    })().finally(() => {
      this.#shutdown = null
    })

    this.#shutdown = shutdown

    return shutdown
  }

  // A blocking read parked on a dedicated connection would otherwise outlive
  // the client: its promise never settles and its socket keeps the process
  // alive, so a graceful shutdown never finishes. Idle pooled connections go
  // too — nothing may hold the loop open past disconnect().
  //
  // The cancellation is OURS, not the driver's. Probed against ioredis 6: on
  // a connection that is 'reconnecting' (server down, socket already gone),
  // disconnect() flushes nothing — the parked XREAD sits in a queue only a
  // SUCCESSFUL reconnect ever drains, and no public driver API forces the
  // rejection. The caller's await would hang forever after disconnect()
  // resolved. So every lease carries a promise this sweep rejects directly.
  #releaseDedicatedConnections () {
    for (const held of this.#dedicated) {
      held.cancel(new RedisClientError(
        `disconnect() closed the connection while '${held.operation}' was still waiting.`,
        held.operation,
        'REDIS_UNAVAILABLE'
      ))
      held.client.disconnect()
    }

    for (const client of this.#idleBlocking.splice(0)) {
      client.disconnect()
    }
  }

  async checkHealth () {
    return this.health.check()
  }

  // Escape hatch for anything that must not share the main connection:
  // WATCH/MULTI/EXEC transactions, blocking reads, SUBSCRIBE experiments.
  // The dedicated client inherits the full configuration (prefix, retries)
  // and is always released, whatever fn does.
  async withDedicatedConnection (fn) {
    return this.#withDedicatedConnection('withDedicatedConnection', fn)
  }

  async #withDedicatedConnection (operation, fn, { reuse = false } = {}) {
    const client = this.connection.assertReady(operation)
    const held = { client: this.#lease(client, reuse), operation, cancelled: false, reuse }

    // Settled only by #releaseDedicatedConnections: shutdown closed this
    // connection under a command that was still waiting. That is a
    // cancellation, not a failure of its own, and the caller needs a code it
    // can branch on to leave its loop — delivered by us, because the driver
    // cannot be relied on to reject anything once the socket is gone.
    const cancellation = new Promise((_resolve, reject) => {
      held.cancel = (err) => {
        held.cancelled = true
        reject(err)
      }
    })
    // Cancellation is one of two ways to lose the race; when fn settles first
    // this rejection has no listener left and must not become "unhandled".
    cancellation.catch(() => {})

    this.#dedicated.add(held)

    try {
      return await Promise.race([fn(held.client), cancellation])
    } finally {
      this.#dedicated.delete(held)
      this.#return(held)
    }
  }

  // A pooled connection when the caller can share one, a fresh one otherwise.
  #lease (client, reuse) {
    if (reuse) {
      const pooled = this.#idleBlocking.pop()

      if (pooled?.status === 'ready') {
        return pooled
      }

      // Recycled from a cycle that already ended: not worth reviving.
      pooled?.disconnect()
    }

    // A blocking read documents `block: 0` as "block forever", and the driver
    // applies commandTimeout to blocking commands like any other — so a
    // consumer loop with commandTimeout: 5000 saw its forever-read rejected
    // "Command timed out" after 5s on an idle stream. The read's own BLOCK is
    // its bound; the pooled connection drops the inherited timeout. The two
    // driver classes take the override in different positions.
    const fresh = reuse
      ? (isCluster(client)
          ? client.duplicate([], { redisOptions: { commandTimeout: undefined } })
          : client.duplicate({ commandTimeout: undefined }))
      : client.duplicate()

    fresh.on('error', (err) => {
      this.logger.debug?.(`Dedicated connection error: ${err.message}`)
    })

    return fresh
  }

  #return ({ client, cancelled, reuse }) {
    // One-shot connections may carry per-connection state the caller left
    // behind (WATCH, MULTI, SUBSCRIBE), and a cancelled or unhealthy socket is
    // never worth recycling.
    const recyclable = reuse &&
      !cancelled &&
      client.status === 'ready' &&
      this.#idleBlocking.length < MAX_IDLE_BLOCKING_CONNECTIONS

    if (recyclable) {
      this.#idleBlocking.push(client)

      return
    }

    client.disconnect()
  }

  // Blocking commands (XREAD/XREADGROUP with BLOCK) run on a dedicated
  // connection: on the shared one they would stall every other command of the
  // application until the block resolves. The connection is pooled afterwards
  // — a consumer loop calls this on every iteration, and a handshake per
  // iteration is a cost nobody asked for.
  async executeBlockingCommand (command, args) {
    try {
      return await this.#withDedicatedConnection(
        command,
        (client) => client[command](...args),
        { reuse: true }
      )
    } catch (err) {
      this.logError(err, command)

      throw err
    }
  }

  async executeCommand (command, ...args) {
    return this.#command(command, command, ...args)
  }

  // Runs a driver command under the PUBLIC operation the caller invoked. The
  // gate and the log both report `operation`, so a getOrSetJson() that has to
  // GET and SETEX on the way says 'getOrSetJson' when either refuses — never
  // the name of the wire command it happened to be issuing at the time.
  async #command (operation, command, ...args) {
    const client = this.connection.assertReady(operation)

    try {
      if (command === 'getAllStream') {
        return await this._getAllStream(...args)
      }

      return await client[command](...args)
    } catch (err) {
      this.logError(err, operation)

      throw err
    }
  }

  async get (key) {
    return this.executeCommand('get', key)
  }

  async getAllStream (pattern = '*') {
    return this.executeCommand('getAllStream', pattern)
  }

  // Variadic, like the command: SET takes NX|XX, GET, EX|PX|EXAT|PXAT|KEEPTTL
  // after the value. A two-argument wrapper used to swallow those silently —
  // `set('session', token, 'EX', 900)` answered 'OK' and the key never
  // expired. Same for expire's NX|XX|GT|LT, exists' extra keys, rpop's count.
  async set (key, value, ...options) {
    return this.executeCommand('set', key, value, ...options)
  }

  async setex (key, seconds, value) {
    return this.executeCommand('setex', key, seconds, value)
  }

  async del (...keys) {
    return this.executeCommand('del', ...keys)
  }

  async incr (key) {
    return this.executeCommand('incr', key)
  }

  async decr (key) {
    return this.executeCommand('decr', key)
  }

  async hset (key, ...args) {
    return this.executeCommand('hset', key, ...args)
  }

  async hget (key, field) {
    return this.executeCommand('hget', key, field)
  }

  async hgetall (key) {
    return this.executeCommand('hgetall', key)
  }

  async lpush (key, ...values) {
    return this.executeCommand('lpush', key, ...values)
  }

  // Without a count Redis pops a single element; with one it pops up to count
  // as an array — forward it, or a caller iterating the result walks the
  // characters of one string.
  async rpop (key, ...count) {
    return this.executeCommand('rpop', key, ...count)
  }

  async sadd (key, ...members) {
    return this.executeCommand('sadd', key, ...members)
  }

  async smembers (key) {
    return this.executeCommand('smembers', key)
  }

  async expire (key, seconds, ...options) {
    return this.executeCommand('expire', key, seconds, ...options)
  }

  async ttl (key) {
    return this.executeCommand('ttl', key)
  }

  async setJson (key, value) {
    return this.#command('setJson', 'set', key, encodeJson(value, 'setJson'))
  }

  async getJson (key) {
    const value = await this.#command('getJson', 'get', key)

    return value ? JSON.parse(value) : null
  }

  async setexJson (key, seconds, value) {
    return this.#command('setexJson', 'setex', key, seconds, encodeJson(value, 'setexJson'))
  }

  // Cache-aside: return the cached value, or produce it, store it (SETEX)
  // and return it. With `lock`, concurrent misses collapse into a single
  // producer call — the winner fills the cache while the others wait on the
  // library's own lock and re-read (dogpile/stampede protection).
  async getOrSet (key, ttlSeconds, producer, options = {}) {
    return this.#getOrSet(key, ttlSeconds, producer, options, {
      operation: 'getOrSet',
      encode: (value) => {
        if (typeof value !== 'string' && typeof value !== 'number') {
          throw new RedisClientError(
            'getOrSet caches strings and numbers only — use getOrSetJson for anything else.',
            'getOrSet',
            'INVALID_ARGUMENT'
          )
        }

        return String(value)
      },
      decode: (raw) => raw
    })
  }

  async getOrSetJson (key, ttlSeconds, producer, options = {}) {
    return this.#getOrSet(key, ttlSeconds, producer, options, {
      operation: 'getOrSetJson',
      encode: (value) => {
        const encoded = JSON.stringify(value)

        // JSON.stringify returns undefined for undefined/functions/symbols;
        // caching that would poison the key (an empty string that every
        // later read fails to parse until the ttl expires).
        if (typeof encoded !== 'string') {
          throw new RedisClientError(
            'getOrSetJson requires the producer to return a JSON-serializable value (got undefined, a function or a symbol).',
            'getOrSetJson',
            'INVALID_ARGUMENT'
          )
        }

        return encoded
      },
      decode: (raw) => JSON.parse(raw)
    })
  }

  // `operation` is the method the caller actually named. It travels down here
  // because `operation` is contract — a consumer routing errors by it must
  // match its own call site, and a getOrSetJson rejection reporting 'getOrSet'
  // matches nothing it ever wrote.
  async #getOrSet (key, ttlSeconds, producer, { lock } = {}, { operation, encode, decode }) {
    if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
      throw new RedisClientError(
        `${operation} requires a positive integer ttl in seconds (got ${ttlSeconds}).`,
        operation,
        'INVALID_ARGUMENT'
      )
    }

    if (typeof producer !== 'function') {
      throw new RedisClientError(
        `${operation} requires a producer function.`,
        operation,
        'INVALID_ARGUMENT'
      )
    }

    // Gated once, up front, under the caller's own name. Reaching the wire
    // through the public get()/setex() wrappers reported their names instead
    // — a disconnected getOrSetJson() rejected with operation 'get', and a
    // failing SETEX with 'setex' — and with `lock`, a gate failure inside
    // withLock said 'acquireLock'. The lock manager keeps its own operation
    // for its own errors (LOCK_NOT_ACQUIRED is genuinely the lock's); the
    // readiness check is this method's.
    this.connection.assertReady(operation)

    const cached = await this.#command(operation, 'get', key)

    if (cached !== null) {
      return decode(cached)
    }

    // Every caller gets the value in its cached form (a decode of what was
    // stored), so winner and waiters always see consistent types.
    const produceAndStore = async () => {
      const value = await producer()
      const encoded = encode(value)
      await this.#command(operation, 'setex', key, ttlSeconds, encoded)

      return decode(encoded)
    }

    if (!lock) {
      return produceAndStore()
    }

    const lockOptions = {
      ttl: 10000,
      retries: 100,
      retryDelay: 50,
      retryJitter: 50,
      // Producers slower than the lock ttl must not reopen the stampede.
      autoExtend: true,
      ...(typeof lock === 'object' ? lock : {})
    }

    try {
      return await this.locks.withLock(`cache:${key}`, lockOptions, async () => {
        // Double-check: the winner may have filled the cache while we waited.
        const refreshed = await this.#command(operation, 'get', key)

        if (refreshed !== null) {
          return decode(refreshed)
        }

        return produceAndStore()
      })
    } catch (err) {
      // Only THIS cache entry's lock failing to be acquired means "fall
      // back". The code alone cannot say which lock threw: a producer that
      // uses withLock internally surfaces the same LOCK_NOT_ACQUIRED, and
      // treating it as ours would rerun the producer unprotected — doubling
      // its side effects precisely under the contention that made the inner
      // lock fail. The error carries the lock's name for exactly this check.
      if (err?.code !== 'LOCK_NOT_ACQUIRED' || err?.lockName !== `cache:${key}`) {
        throw err
      }

      // A cache call must not surface its own lock errors. Waiters land here
      // after waiting out their whole retry budget, so the winner has probably
      // filled the cache by now — re-read, and only produce unprotected as
      // the last resort (availability beats perfect stampede protection).
      this.logger.debug?.(`Cache lock for '${key}' not acquired within the retry budget — falling back.`)

      const fallback = await this.#command(operation, 'get', key)

      if (fallback !== null) {
        return decode(fallback)
      }

      return produceAndStore()
    }
  }

  // Non-blocking bulk deletion (SCAN + UNLINK batches) confined to the
  // prefixed keyspace, same semantics as getAllStream. Returns the number
  // of keys removed. The pattern is required — '*' wipes the whole
  // (prefixed) keyspace and must be an explicit choice.
  async deleteByPattern (pattern) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
      throw new RedisClientError(
        "deleteByPattern requires a non-empty pattern (use '*' explicitly to wipe the prefixed keyspace).",
        'deleteByPattern',
        'INVALID_ARGUMENT'
      )
    }

    const client = this.connection.assertReady('deleteByPattern')

    return deletePattern({
      client,
      keyPrefix: this.keyPrefix,
      logger: this.logger,
      pattern
    })
  }

  // HMSET is deprecated since Redis 4.0: delegate to variadic HSET.
  // Note: returns the number of newly created fields, not 'OK'.
  async hmset (key, obj) {
    return this.executeCommand('hset', key, obj)
  }

  async hmget (key, ...fields) {
    return this.executeCommand('hmget', key, ...fields)
  }

  async hincrby (key, field, increment) {
    return this.executeCommand('hincrby', key, field, increment)
  }

  async hexists (key, field) {
    return this.executeCommand('hexists', key, field)
  }

  async hdel (key, ...fields) {
    return this.executeCommand('hdel', key, ...fields)
  }

  async lrange (key, start, stop) {
    return this.executeCommand('lrange', key, start, stop)
  }

  async llen (key) {
    return this.executeCommand('llen', key)
  }

  async lrem (key, count, value) {
    return this.executeCommand('lrem', key, count, value)
  }

  async lpushx (key, value) {
    return this.executeCommand('lpushx', key, value)
  }

  async rpushx (key, value) {
    return this.executeCommand('rpushx', key, value)
  }

  async sismember (key, member) {
    return this.executeCommand('sismember', key, member)
  }

  async scard (key) {
    return this.executeCommand('scard', key)
  }

  // Without a count Redis returns a single member; with one it returns an
  // array — never force a count, or the return type silently changes.
  async spop (key, count) {
    return count === undefined
      ? this.executeCommand('spop', key)
      : this.executeCommand('spop', key, count)
  }

  async srem (key, ...members) {
    return this.executeCommand('srem', key, ...members)
  }

  // Sorted sets. Scores come back as numbers (Redis speaks strings) and
  // WITHSCORES replies as { member, score } pairs instead of a flat array.
  async zadd (key, ...args) {
    const [first] = args

    // The common case reads better as { member: score }; arrays and anything
    // else are passed straight through, so flags like NX/GT/CH stay available.
    const isMemberMap = args.length === 1 &&
      typeof first === 'object' &&
      first !== null &&
      !Array.isArray(first)

    const commandArgs = isMemberMap
      ? Object.entries(first).flatMap(([member, score]) => [score, member])
      : args

    if (commandArgs.length === 0) {
      throw new RedisClientError(
        'zadd requires at least one member to add.',
        'zadd',
        'INVALID_ARGUMENT'
      )
    }

    // Parsed unconditionally: a count comes back as an integer, which
    // parseScore leaves untouched, and with INCR the reply is the new score,
    // which Redis speaks as a string ('5', 'inf') — every other sorted-set
    // method parses those, and this one used to hand the raw string back.
    return parseScore(await this.executeCommand('zadd', key, ...commandArgs))
  }

  async zscore (key, member) {
    return parseScore(await this.executeCommand('zscore', key, member))
  }

  async zincrby (key, increment, member) {
    return parseScore(await this.executeCommand('zincrby', key, increment, member))
  }

  async zcard (key) {
    return this.executeCommand('zcard', key)
  }

  async zcount (key, min, max) {
    return this.executeCommand('zcount', key, min, max)
  }

  async zrank (key, member) {
    return this.executeCommand('zrank', key, member)
  }

  async zrevrank (key, member) {
    return this.executeCommand('zrevrank', key, member)
  }

  async zrem (key, ...members) {
    return this.executeCommand('zrem', key, ...members)
  }

  async zrange (key, start, stop, options = {}) {
    // Redis refuses these combinations with a generic "syntax error" that
    // names no option; the caller gets told which one here, first.
    if (options.byScore && options.byLex) {
      throw new RedisClientError('zrange takes byScore or byLex, not both.', 'zrange', 'INVALID_ARGUMENT')
    }

    if (options.limit && !options.byScore && !options.byLex) {
      throw new RedisClientError('zrange only supports limit together with byScore or byLex.', 'zrange', 'INVALID_ARGUMENT')
    }

    if (options.byLex && options.withScores) {
      throw new RedisClientError('zrange cannot return withScores for a byLex range (lexicographic ranges have no scores to report).', 'zrange', 'INVALID_ARGUMENT')
    }

    const args = [key, start, stop]

    if (options.byScore) args.push('BYSCORE')
    if (options.byLex) args.push('BYLEX')
    if (options.rev) args.push('REV')
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)
    if (options.withScores) args.push('WITHSCORES')

    const reply = await this.executeCommand('zrange', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zrevrange (key, start, stop, options = {}) {
    const args = [key, start, stop]

    if (options.withScores) args.push('WITHSCORES')

    const reply = await this.executeCommand('zrevrange', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zrangebyscore (key, min, max, options = {}) {
    const args = [key, min, max]

    if (options.withScores) args.push('WITHSCORES')
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)

    const reply = await this.executeCommand('zrangebyscore', ...args)

    return options.withScores ? parseScoredMembers(reply) : reply
  }

  async zremrangebyrank (key, start, stop) {
    return this.executeCommand('zremrangebyrank', key, start, stop)
  }

  async zremrangebyscore (key, min, max) {
    return this.executeCommand('zremrangebyscore', key, min, max)
  }

  // Without a count Redis pops a single member; with one it pops up to count.
  async zpopmin (key, count) {
    return this.#popScored('zpopmin', key, count)
  }

  async zpopmax (key, count) {
    return this.#popScored('zpopmax', key, count)
  }

  async #popScored (command, key, count) {
    const reply = count === undefined
      ? await this.executeCommand(command, key)
      : await this.executeCommand(command, key, count)

    const entries = parseScoredMembers(reply)

    return count === undefined ? entries[0] ?? null : entries
  }

  // `by` and `get` patterns are KEYS to the driver: @ioredis/commands marks
  // the BY pattern, every GET pattern except '#', and the STORE destination as
  // key positions, and ioredis prefixes them with keyPrefix like any other.
  // Pass them WITHOUT the prefix — the README once said the opposite, and a
  // caller who followed it got `BY app:app:weight_*`: every weight 0, an
  // arbitrary order, and no error. SORT accepts many GET patterns; `get` may
  // be one string or an array of them.
  async sort (key, options = {}) {
    const args = [key]

    if (options.by) args.push('BY', options.by)
    if (options.limit) args.push('LIMIT', options.limit.offset, options.limit.count)
    for (const pattern of [options.get].flat().filter((p) => p != null)) args.push('GET', pattern)
    if (options.direction) args.push(options.direction)
    if (options.alpha) args.push('ALPHA')
    if (options.store) args.push('STORE', options.store)

    return this.executeCommand('sort', ...args)
  }

  // Values are sent as-is (ioredis accepts the object form directly). No
  // magic JSON serialization: mget/get would hand back raw strings anyway —
  // use the *Json helpers for objects.
  async mset (obj) {
    return this.executeCommand('mset', obj)
  }

  async mget (...keys) {
    return this.executeCommand('mget', ...keys)
  }

  async exists (...keys) {
    return this.executeCommand('exists', ...keys)
  }

  async type (key) {
    return this.executeCommand('type', key)
  }

  async rename (key, newkey) {
    return this.executeCommand('rename', key, newkey)
  }

  async renamenx (key, newkey) {
    return this.executeCommand('renamenx', key, newkey)
  }

  async persist (key) {
    return this.executeCommand('persist', key)
  }

  async multi () {
    return this.connection.assertReady('multi').multi()
  }

  // WATCH state is per-connection: on the shared connection, concurrent
  // flows watching keys poison each other (any EXEC/UNWATCH clears ALL
  // watches). Refusing loudly beats silently-wrong optimistic locking —
  // use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.
  async watch () {
    throw new RedisClientError(
      'watch() is not supported on the shared connection. Use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.',
      'watch',
      'UNSUPPORTED_OPERATION'
    )
  }

  async unwatch () {
    throw new RedisClientError(
      'unwatch() is not supported on the shared connection. Use withDedicatedConnection() for isolated WATCH/MULTI/EXEC.',
      'unwatch',
      'UNSUPPORTED_OPERATION'
    )
  }

  // Channels are not keys: keyPrefix does not apply to pub/sub.
  async publish (channel, message) {
    return this.executeCommand('publish', channel, message)
  }

  async publishJson (channel, value) {
    return this.#command('publishJson', 'publish', channel, encodeJson(value, 'publishJson'))
  }

  // The readiness gate lives in the connection manager and already knows a
  // shutdown is in progress; each method passes its own name down so the
  // refusal — whichever reason — is reported under it.
  async subscribe (channel, handler) {
    return this.subscriptions.subscribe(channel, handler, 'subscribe')
  }

  async unsubscribe (channel) {
    return this.subscriptions.unsubscribe(channel, 'unsubscribe')
  }

  async psubscribe (pattern, handler) {
    return this.subscriptions.psubscribe(pattern, handler, 'psubscribe')
  }

  async punsubscribe (pattern) {
    return this.subscriptions.punsubscribe(pattern)
  }

  /**
   * The server's current `notify-keyspace-events` flags (empty when disabled).
   *
   * In a cluster every master is configured on its own, so this answers with
   * the flags they ALL agree on — and an empty string the moment they differ,
   * with a warning naming the readings. Sampling the first master would report
   * "AKE" while another shard sits silent, which is precisely the failure
   * subscribeToKeyEvents refuses to let through; the operational check that
   * exists to catch it must not be the one that hides it.
   *
   * Use keyspaceNotificationsByNode() for the per-master breakdown.
   */
  async keyspaceNotifications () {
    const readings = await this.#keyspaceFlagsByNode('keyspaceNotifications')
    // Mid-failover a cluster can report no masters at all: no reading is the
    // same answer as no notifications, and the default keeps the comparison
    // below from having to care which case it is.
    const [first = { flags: '' }] = readings

    if (readings.some((reading) => reading.flags !== first.flags)) {
      this.logger.warn(`Masters disagree on notify-keyspace-events: ${readings.map(({ node, flags }) => `${node} "${flags}"`).join(', ')}. Reporting none.`)

      return ''
    }

    return first.flags
  }

  /**
   * The `notify-keyspace-events` flags of every master, as
   * `[{ node, flags }]` — `node` is null outside a cluster.
   */
  async keyspaceNotificationsByNode () {
    return this.#keyspaceFlagsByNode('keyspaceNotificationsByNode')
  }

  // CONFIG has no key to route on, so a cluster has to be asked node by node —
  // and every master must answer, because each one is configured on its own and
  // each one emits only its own slots' events.
  //
  // The probe is deadlined because its whole purpose is graceful degradation
  // on managed providers that restrict CONFIG. A provider that HANGS on it
  // instead of erroring would otherwise park this await forever — defeating
  // the very fallback in #assertKeyspaceNotifications that was written for
  // that class of provider.
  async #keyspaceFlagsByNode (operation) {
    const client = this.connection.assertReady(operation)
    const cluster = isCluster(client)

    return Promise.all(masters(client).map(async (target) => {
      const [, flags] = await withDeadline(
        target.config('GET', 'notify-keyspace-events'),
        { clock: this.clock, ms: CONFIG_PROBE_DEADLINE_MS, operation }
      )

      return {
        node: cluster ? nodeKey(target) : null,
        flags: flags ?? ''
      }
    }))
  }

  // Keyspace events only exist if the server was configured to emit them, and
  // a subscription to a silent channel looks exactly like one that works.
  // Probing turns that silence into an error that says what to enable.
  async subscribeToKeyEvents (event, handler, options = {}) {
    await this.#assertKeyspaceNotifications(event)

    const db = options.db ?? this.redisConfig.db

    // Not subscribe(): in a cluster these events are node-local, so they need
    // one subscriber per master (see SubscriptionManager). The probe above
    // may have waited up to its deadline; subscribeEverywhere gates again
    // under this operation's name, so a disconnect() that started during the
    // probe refuses the subscription instead of finding a dying client.
    return this.subscriptions.subscribeEverywhere(`__keyevent@${db}__:${event}`, handler, 'subscribeToKeyEvents')
  }

  async #assertKeyspaceNotifications (event) {
    let readings

    try {
      readings = await this.#keyspaceFlagsByNode('subscribeToKeyEvents')
    } catch (err) {
      // "Not connected" is the gate's verdict, not the provider's. Swallowing
      // it here logged a bogus "CONFIG restricted" warning and let the call
      // fail one step later under the wrong operation.
      if (err instanceof RedisClientError && err.code === 'REDIS_UNAVAILABLE') {
        throw err
      }

      // Managed providers commonly block CONFIG. Refusing to subscribe would
      // be worse than subscribing without the guarantee.
      this.logger.warn(`Could not read notify-keyspace-events (${err.message}). Subscribing without verifying it.`)

      return
    }

    const required = Object.hasOwn(KEY_EVENT_CLASSES, event) ? KEY_EVENT_CLASSES[event] : undefined

    // An event this table does not know cannot be verified beyond the 'E'
    // flag. Say so, rather than passing it in silence as if it had been.
    if (required === undefined) {
      this.logger.warn(`Key event '${event}' has no known notify-keyspace-events class: only the 'E' flag can be verified for it.`)
    }

    // One misconfigured master is enough to lose that shard's events silently,
    // so the weakest node decides the verdict — not the first one asked.
    for (const { node, flags } of readings) {
      const missing = []

      if (!flags.includes('E')) missing.push('E')

      // 'A' is NOT "everything": redis.conf defines it as g$lshzxetd, which
      // deliberately excludes 'n' (new-key) and 'm' (key-miss). Accepting 'A'
      // for those would wave through the canonical "AKE" config and hand the
      // caller a subscription that never speaks — the exact silence this
      // probe exists to turn into an error.
      const coveredByAlias = flags.includes('A') && !'nm'.includes(required)

      if (required && !coveredByAlias && !flags.includes(required)) missing.push(required)

      if (missing.length > 0) {
        const where = node ? ` on cluster node ${node}` : ''

        throw new RedisClientError(
          `Keyspace notifications are not enabled for '${event}'${where}: notify-keyspace-events is "${flags}", missing "${missing.join('')}". Enable it with CONFIG SET notify-keyspace-events "${flags}${missing.join('')}".`,
          'subscribeToKeyEvents',
          'KEYSPACE_NOTIFICATIONS_DISABLED'
        )
      }
    }
  }

  // Single-instance distributed lock (SET NX PX + token-checked Lua release).
  async acquireLock (name, options) {
    return this.locks.acquire(name, options)
  }

  async withLock (name, options, fn) {
    return this.locks.withLock(name, options, fn)
  }

  // Registers Lua once and calls it by name from then on. The driver sends the
  // SHA rather than the script body, reloads it by itself on NOSCRIPT, and
  // reinstalls it on the new client after a reconnection cycle. Use this over
  // executeCommand('eval', …) for anything on a hot path — a compare-and-set
  // that runs per request should not ship a program per request.
  defineScript (name, definition) {
    return this.scripts.define(name, definition)
  }

  // Keys and arguments travel as two arrays, not one flat list: KEYS is what
  // makes a script routable in a cluster and prefixable at all, and a boundary
  // the caller can get wrong silently is one this library refuses to have.
  // The count declared at registration is checked here.
  async runScript (name, keys = [], args = []) {
    return this.scripts.run(name, keys, args)
  }

  async xadd (key, id, ...args) {
    return this.executeCommand('xadd', key, id, ...args)
  }

  // block: 0 is a legitimate value (block forever) — test against null,
  // never truthiness. Blocking reads run on a dedicated connection.
  async xread (options = {}, streams) {
    return this.#xreadCommand('xread', [], options, streams)
  }

  async xreadgroup (groupName, consumerName, options = {}, streams) {
    return this.#xreadCommand('xreadgroup', ['GROUP', groupName, consumerName], options, streams)
  }

  // One body for both reads: they differed only in a leading GROUP clause and
  // NOACK, and the duplicated block-routing ternary had already cost a
  // mutation review (the report blamed one method's line while the live
  // mutant sat on the other's). `streams` is the keys followed by the ids,
  // positionally; a string would be spread into its characters and reach the
  // server as `STREAMS e v e n t s`, and a missing argument threw a raw
  // TypeError outside the logging path.
  async #xreadCommand (command, leading, options, streams) {
    if (!Array.isArray(streams) || streams.length === 0 || streams.length % 2 !== 0) {
      throw new RedisClientError(
        `${command} takes the streams as one array of keys followed by their ids, e.g. ['events', '$'] (got ${JSON.stringify(streams)}).`,
        command,
        'INVALID_ARGUMENT'
      )
    }

    const args = [...leading]

    if (options.count != null) args.push('COUNT', options.count)
    if (options.block != null) args.push('BLOCK', options.block)
    if (options.noack) args.push('NOACK')

    args.push('STREAMS', ...streams)

    return options.block != null
      ? this.executeBlockingCommand(command, args)
      : this.executeCommand(command, ...args)
  }

  // The key of XGROUP/XINFO sits *after* a subcommand. Through ioredis 5 the
  // driver did not recognize that position, so this facade prefixed those two
  // by hand — otherwise a prefixed client created consumer groups on
  // unprefixed keys while XADD/XREADGROUP used the prefixed ones.
  //
  // @ioredis/commands 2.0.0 (shipped with ioredis 6) declares the position,
  // so the driver prefixes them like any other key. Doing it here as well
  // produced `app:app:stream` — the group created on one key and read from
  // another, which is why this library requires ioredis 6 and cannot support
  // both majors without branching on the driver's version.
  async xgroup (command, key, groupName, ...rest) {
    const subcommand = String(command).toUpperCase()
    const args = [subcommand, key, groupName]

    switch (subcommand) {
      case 'CREATE': {
        // `mkstream` is the boolean this facade has always taken; anything
        // after it (ENTRIESREAD n, Redis 7) travels as-is — it used to be
        // dropped, and XINFO reported the group's lag as null forever.
        const [id = '$', mkstream, ...extra] = rest
        args.push(id)

        if (mkstream) {
          args.push('MKSTREAM')
        }

        args.push(...extra)

        break
      }
      case 'SETID': {
        const [id = '$', ...extra] = rest
        args.push(id, ...extra)

        break
      }
      case 'CREATECONSUMER':
      case 'DELCONSUMER': {
        args.push(rest[0])

        break
      }
      // DESTROY takes no extra arguments.
    }

    return this.executeCommand('xgroup', ...args)
  }

  async xlen (key) {
    return this.executeCommand('xlen', key)
  }

  async xinfo (subcommand, key, ...args) {
    return this.executeCommand('xinfo', subcommand, key, ...args)
  }

  async xrange (key, start, end, options = {}) {
    const args = [key, start, end]

    if (options.count != null) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrange', ...args)
  }

  async xrevrange (key, end, start, options = {}) {
    const args = [key, end, start]

    if (options.count != null) {
      args.push('COUNT', options.count)
    }

    return this.executeCommand('xrevrange', ...args)
  }

  async xdel (key, ...ids) {
    return this.executeCommand('xdel', key, ...ids)
  }

  async xtrim (key, strategy, approx = false, count) {
    if (count == null) {
      throw new RedisClientError(
        'xtrim requires a count/threshold value (e.g. xtrim(key, \'MAXLEN\', false, 1000)).',
        'xtrim',
        'INVALID_ARGUMENT'
      )
    }

    const args = [key, strategy]

    if (approx) {
      args.push('~')
    }

    args.push(count)

    return this.executeCommand('xtrim', ...args)
  }

  // Two shapes in one command: without a range it returns the group summary
  // ([total, minId, maxId, consumers]), with one it returns the pending
  // entries. Silently dropping a partial range would answer a different
  // question than the caller asked, in a different shape.
  async xpending (key, group, options = {}) {
    const { start, end, count, consumer } = options
    const wantsRange = start != null || end != null || count != null

    if (wantsRange && (start == null || end == null || count == null)) {
      throw new RedisClientError(
        "xpending needs start, end and count together to list entries (e.g. xpending(key, group, { start: '-', end: '+', count: 10 })). Pass no options for the group summary.",
        'xpending',
        'INVALID_ARGUMENT'
      )
    }

    if (!wantsRange && consumer != null) {
      throw new RedisClientError(
        'xpending can only filter by consumer together with start, end and count.',
        'xpending',
        'INVALID_ARGUMENT'
      )
    }

    const args = [key, group]

    if (wantsRange) {
      args.push(start, end, count)

      if (consumer) {
        args.push(consumer)
      }
    }

    return this.executeCommand('xpending', ...args)
  }

  // Settling a consumer-group entry: without it, every delivered entry stays
  // in the group's pending list forever.
  async xack (key, group, ...ids) {
    return this.executeCommand('xack', key, group, ...ids)
  }

  // Sweeps the group's pending list for entries idle longer than
  // minIdleTime and hands them to `consumer` — the recovery path for a
  // consumer that died holding deliveries. The reply is positional
  // ([cursor, entries, deleted]); it is returned as named fields so callers
  // do not index into it.
  async xautoclaim (key, group, consumer, minIdleTime, start = '0-0', options = {}) {
    const args = [key, group, consumer, minIdleTime, start]

    if (options.count != null) args.push('COUNT', options.count)
    if (options.justId) args.push('JUSTID')

    const [cursor, entries, deleted] = await this.executeCommand('xautoclaim', ...args)

    return { cursor, entries: entries ?? [], deleted: deleted ?? [] }
  }

  async xclaim (key, group, consumer, minIdleTime, ...ids) {
    const args = [key, group, consumer, minIdleTime, ...ids]

    return this.executeCommand('xclaim', ...args)
  }

  async _getAllStream (pattern = '*') {
    return scanKeyspace({
      client: this.connection.client,
      keyPrefix: this.keyPrefix,
      logger: this.logger,
      pattern
    })
  }

  omitPrefix (key) {
    return omitPrefixWith(this.keyPrefix)(key)
  }

  logError (err, operation) {
    if (err instanceof RedisClientError) {
      this.logger.error(`Redis operation '${err.operation}' failed: ${err.message}`)
    } else {
      this.logger.error(`Unexpected error in Redis '${operation}' operation: ${err.message}`)
    }
  }
}

// The class table is exported so callers (and the tests) can see exactly which
// events the keyspace probe can verify.
export { RedisClient, RedisClientError, createLogger, KEY_EVENT_CLASSES }
export default RedisClient

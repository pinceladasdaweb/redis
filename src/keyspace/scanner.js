import RedisClientError from '../utils/errors.js'
import { isCluster, masters } from '../utils/cluster.js'

// SCAN-based keyspace walks. ioredis does NOT apply keyPrefix to SCAN MATCH
// patterns: the pattern is prefixed here, so a walk is confined to this
// client's keyspace instead of sweeping the whole database (and other
// applications' keys).
//
// A cluster has no scanStream of its own — every master holds a slice of the
// keyspace, so a walk means walking each of them and merging the results.

const omitPrefixWith = (keyPrefix) => (key) =>
  keyPrefix && key.startsWith(keyPrefix) ? key.slice(keyPrefix.length) : key

// The prefix is a LITERAL, but it lands inside a glob: `*`, `?`, `[`, `]` and
// `\` in it would be read as pattern syntax. A keyPrefix of 'tenant[a]:' used
// to match 'tenanta:…' — nothing of this client's, and possibly someone
// else's, which the prefixing client then re-prefixed on GET and dropped.
const escapeGlob = (literal) => literal.replace(/[*?[\]\\]/g, '\\$&')

// A cluster's master roster is the driver's pool as of its LAST slots refresh.
// Between a failover and the next refresh a promoted replica is still filed
// under 'slave', and a walk that trusted the roster returned a result missing
// that shard's whole slice — with no error, the "truncated answer that looks
// complete" this file exists to refuse. One CLUSTER SLOTS round-trip before a
// full-keyspace walk is a price worth paying for an honest roster.
const refreshRoster = (client) => isCluster(client)
  ? new Promise((resolve, reject) => client.refreshSlotsCache((err) => (err ? reject(err) : resolve())))
  : Promise.resolve()

// Runs `onBatch` for every batch a single node reports, keeping the stream
// paused while the batch is handled so reads stay bounded.
//
// Once anything fails, NOTHING more may be issued. A paused Readable keeps
// prefetching into its buffer (probed: 17 SCAN pages queued while paused), and
// the in-flight batch's resume() used to release them all after the stream's
// own 'error' had already rejected the walk — so deleteByPattern went on
// deleting, invisibly, behind a promise that had reported failure.
const walkNode = (node, match, onBatch) => {
  const stream = node.scanStream({ match, count: 100 })
  let settled = false

  const done = new Promise((resolve, reject) => {
    const finish = (outcome, value) => {
      if (settled) return

      settled = true
      stream.destroy()
      outcome(value)
    }

    stream.on('data', (keys) => {
      if (settled || keys.length === 0) {
        return
      }

      stream.pause()

      onBatch(keys)
        .then(() => {
          if (!settled) stream.resume()
        })
        .catch((err) => finish(reject, err))
    })

    stream.on('end', () => finish(resolve))
    stream.on('error', (err) => finish(reject, err))

    // A sibling failed: stop scanning, and settle — quietly — so nothing
    // awaits a stream that will never end.
    stream.abort = () => finish(resolve)
  })

  return { done, abort: () => stream.abort() }
}

// Every master walked at once: the walks are independent and the merge is
// order-insensitive, so wall time is the slowest shard rather than the sum of
// all of them. The first failure aborts the rest — a partial result is not a
// result.
const walkAll = async (client, match, onBatch) => {
  await refreshRoster(client)

  const walks = masters(client).map((node) => walkNode(node, match, (keys) => onBatch(node, keys)))

  try {
    await Promise.all(walks.map((walk) => walk.done))
  } catch (err) {
    for (const walk of walks) walk.abort()

    throw err
  }
}

const assertPattern = (pattern, operation) => {
  // ioredis omits MATCH entirely for a falsy pattern, so '' with no prefix
  // walks the WHOLE database — every application's keys — while '' with a
  // prefix matches only a key literally named after the prefix. Two unrelated
  // outcomes for one input; refuse it.
  if (typeof pattern !== 'string' || pattern.length === 0) {
    throw new RedisClientError(
      `${operation} requires a non-empty pattern (use '*' explicitly to walk the whole prefixed keyspace).`,
      operation,
      'INVALID_ARGUMENT'
    )
  }
}

const scanKeyspace = async ({ client, keyPrefix = '', logger, pattern = '*' }) => {
  assertPattern(pattern, 'getAllStream')

  const omitPrefix = omitPrefixWith(keyPrefix)
  const data = []
  const seen = new Set()

  await walkAll(client, `${escapeGlob(keyPrefix)}${pattern}`, async (node, keys) => {
    const properties = keys.map(omitPrefix)

    // One pipelined round-trip per batch. Reads go to the node that
    // reported the keys, so they never cross a slot boundary.
    const results = await node.pipeline(properties.map((property) => ['get', property])).exec()

    for (const [index, [err, value]] of results.entries()) {
      const property = properties[index]

      if (err) {
        // The one error this walk may absorb: GET on a non-string key. The
        // README documents that skip. Anything else — MOVED/ASK mid-reshard
        // (these node-level pipelines never follow redirections), LOADING,
        // CLUSTERDOWN — means the result would be silently incomplete, and
        // a truncated answer that looks complete is worse than a failure.
        if (String(err.message).startsWith('WRONGTYPE')) {
          logger.debug?.(`getAllStream skipped non-string key '${property}'`)
          continue
        }

        throw err
      }

      // SCAN may return a key more than once; null means the key expired
      // or was deleted between SCAN and GET.
      if (value !== null && !seen.has(property)) {
        seen.add(property)
        data.push({ [property]: value })
      }
    }
  }).catch((err) => {
    logger.error(`Error in getAllStream: ${err.message}`)

    throw err
  })

  logger.debug?.(`Redis getAllStream is complete. Entries: ${data.length}`)

  return data
}

const deletePattern = async ({ client, keyPrefix = '', logger, pattern }) => {
  assertPattern(pattern, 'deleteByPattern')

  const omitPrefix = omitPrefixWith(keyPrefix)
  let deleted = 0

  await walkAll(client, `${escapeGlob(keyPrefix)}${pattern}`, async (node, keys) => {
    // One UNLINK per key rather than one variadic UNLINK: a multi-key
    // command needs every key in the same slot, which nothing guarantees
    // here. Pipelining keeps it to a single round-trip anyway.
    const results = await node.pipeline(keys.map((key) => ['unlink', omitPrefix(key)])).exec()

    for (const [err, count] of results) {
      if (err) {
        throw err
      }

      deleted += count
    }
  }).catch((err) => {
    logger.error(`Error in deleteByPattern: ${err.message}`)

    throw err
  })

  logger.debug?.(`deleteByPattern complete. Keys removed: ${deleted}`)

  return deleted
}

export { scanKeyspace, deletePattern, omitPrefixWith, escapeGlob }
export default scanKeyspace

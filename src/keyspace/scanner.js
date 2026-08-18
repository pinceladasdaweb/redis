// SCAN-based keyspace walks. ioredis does NOT apply keyPrefix to SCAN MATCH
// patterns: the pattern is prefixed here, so a walk is confined to this
// client's keyspace instead of sweeping the whole database (and other
// applications' keys).
//
// A cluster has no scanStream of its own — every master holds a slice of the
// keyspace, so a walk means walking each of them and merging the results.

const scanTargets = (client) =>
  typeof client.nodes === 'function' ? client.nodes('master') : [client]

const omitPrefixWith = (keyPrefix) => (key) =>
  keyPrefix && key.startsWith(keyPrefix) ? key.slice(keyPrefix.length) : key

// Runs `onBatch` for every batch a single node reports, keeping the stream
// paused while the batch is handled so reads stay bounded.
const walkNode = (node, match, onBatch) => new Promise((resolve, reject) => {
  const stream = node.scanStream({ match, count: 100 })

  stream.on('data', (keys) => {
    if (keys.length === 0) {
      return
    }

    stream.pause()

    onBatch(keys)
      .then(() => stream.resume())
      .catch((err) => {
        stream.destroy()
        reject(err)
      })
  })

  stream.on('end', resolve)
  stream.on('error', reject)
})

const scanKeyspace = async ({ client, keyPrefix = '', logger, pattern = '*' }) => {
  const omitPrefix = omitPrefixWith(keyPrefix)
  const data = []
  const seen = new Set()

  for (const node of scanTargets(client)) {
    await walkNode(node, `${keyPrefix}${pattern}`, async (keys) => {
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
  }

  logger.debug?.(`Redis getAllStream is complete. Entries: ${data.length}`)

  return data
}

const deletePattern = async ({ client, keyPrefix = '', logger, pattern }) => {
  const omitPrefix = omitPrefixWith(keyPrefix)
  let deleted = 0

  for (const node of scanTargets(client)) {
    await walkNode(node, `${keyPrefix}${pattern}`, async (keys) => {
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
  }

  logger.debug?.(`deleteByPattern complete. Keys removed: ${deleted}`)

  return deleted
}

export { scanKeyspace, deletePattern }
export default scanKeyspace

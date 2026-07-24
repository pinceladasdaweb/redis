// SCAN-based keyspace dump. ioredis does NOT apply keyPrefix to SCAN MATCH
// patterns: the pattern is prefixed here, so the scan is confined to this
// client's keyspace instead of sweeping the whole database (and other
// applications' keys).
const scanKeyspace = ({ client, keyPrefix = '', logger, pattern = '*' }) => {
  const omitPrefix = (key) =>
    keyPrefix && key.startsWith(keyPrefix) ? key.slice(keyPrefix.length) : key

  return new Promise((resolve, reject) => {
    const data = []
    const seen = new Set()

    const stream = client.scanStream({
      match: `${keyPrefix}${pattern}`,
      count: 100
    })

    stream.on('data', (keys) => {
      if (keys.length === 0) {
        return
      }

      // One pipelined round-trip per SCAN batch (bounded concurrency), and
      // per-key errors — e.g. WRONGTYPE for non-string keys — skip that key
      // instead of rejecting the whole scan.
      stream.pause()

      const properties = keys.map(omitPrefix)

      client.pipeline(properties.map((property) => ['get', property])).exec()
        .then((results) => {
          results.forEach(([err, value], index) => {
            const property = properties[index]

            if (err) {
              logger.debug?.(`getAllStream skipped key '${property}': ${err.message}`)
              return
            }

            // SCAN may return a key more than once; null means the key
            // expired or was deleted between SCAN and GET.
            if (value !== null && !seen.has(property)) {
              seen.add(property)
              data.push({ [property]: value })
            }
          })

          stream.resume()
        })
        .catch((err) => {
          logger.error(`Error in getAllStream: ${err.message}`)
          stream.destroy()
          reject(err)
        })
    })

    stream.on('end', () => {
      logger.debug?.(`Redis getAllStream is complete. Entries: ${data.length}`)
      resolve(data)
    })

    stream.on('error', (error) => {
      logger.error(`Error in getAllStream: ${error.message}`)
      reject(error)
    })
  })
}

// SCAN + UNLINK in batches: non-blocking deletion of every key matching the
// (prefixed) pattern. Same prefix semantics as scanKeyspace.
const deletePattern = ({ client, keyPrefix = '', logger, pattern }) => {
  const omitPrefix = (key) =>
    keyPrefix && key.startsWith(keyPrefix) ? key.slice(keyPrefix.length) : key

  return new Promise((resolve, reject) => {
    let deleted = 0

    const stream = client.scanStream({
      match: `${keyPrefix}${pattern}`,
      count: 100
    })

    stream.on('data', (keys) => {
      if (keys.length === 0) {
        return
      }

      stream.pause()

      client.unlink(...keys.map(omitPrefix))
        .then((count) => {
          deleted += count
          stream.resume()
        })
        .catch((err) => {
          logger.error(`Error in deleteByPattern: ${err.message}`)
          stream.destroy()
          reject(err)
        })
    })

    stream.on('end', () => {
      logger.debug?.(`deleteByPattern complete. Keys removed: ${deleted}`)
      resolve(deleted)
    })

    stream.on('error', (error) => {
      logger.error(`Error in deleteByPattern: ${error.message}`)
      reject(error)
    })
  })
}

export { scanKeyspace, deletePattern }
export default scanKeyspace

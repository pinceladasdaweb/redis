import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import scanKeyspace, { deletePattern } from '../src/keyspace/scanner.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

// Minimal stand-in for ioredis' scanStream: emits the given batches, honors
// pause/resume so the batching contract is actually exercised.
const createClient = ({ batches, pipelineResults, unlinkResults, fail }) => {
  const state = { patterns: [], pipelines: [], unlinked: [], destroyed: false }

  const client = {
    scanStream (options) {
      state.patterns.push(options.match)

      const handlers = {}
      let paused = false
      let index = 0

      const pump = () => {
        if (paused) return

        if (index >= batches.length) {
          handlers.end?.()
          return
        }

        const batch = batches[index++]
        handlers.data?.(batch)
        if (!paused) queueMicrotask(pump)
      }

      const stream = {
        on (event, handler) {
          handlers[event] = handler
          if (event === 'error' && fail === 'stream') queueMicrotask(() => handler(new Error('scan exploded')))
          return stream
        },
        pause () { paused = true },
        resume () { paused = false; queueMicrotask(pump) },
        destroy () { state.destroyed = true }
      }

      if (fail !== 'stream') queueMicrotask(pump)

      return stream
    },

    pipeline (commands) {
      state.pipelines.push(commands)

      if (commands[0]?.[0] === 'unlink') {
        state.unlinked.push(commands.map(([, key]) => key))
      }

      return {
        async exec () {
          if (fail === 'pipeline' && commands[0]?.[0] === 'get') throw new Error('pipeline exploded')
          if (fail === 'unlink' && commands[0]?.[0] === 'unlink') throw new Error('unlink exploded')

          if (commands[0]?.[0] !== 'unlink') {
            return pipelineResults.shift()
          }

          // A pipeline reports per-command failures inside the reply, not by
          // rejecting: an Error here stands for one key that refused to go.
          return commands.map(() => {
            const reply = unlinkResults.shift() ?? 1

            return reply instanceof Error ? [reply, null] : [null, reply]
          })
        }
      }
    }
  }

  return { client, state }
}

describe('keyspace scanner', () => {
  test('collects values, strips the prefix and skips nulls', async () => {
    const { client, state } = createClient({
      batches: [['app:a', 'app:b', 'app:gone']],
      pipelineResults: [[[null, '1'], [null, '2'], [null, null]]]
    })

    const data = await scanKeyspace({ client, keyPrefix: 'app:', logger: quietLogger, pattern: 'user:*' })

    assert.deepEqual(data, [{ a: '1' }, { b: '2' }])
    assert.deepEqual(state.patterns, ['app:user:*'])
    assert.deepEqual(state.pipelines[0], [['get', 'a'], ['get', 'b'], ['get', 'gone']])
  })

  test('scans the raw pattern when no prefix is configured', async () => {
    const { client, state } = createClient({
      batches: [['plain']],
      pipelineResults: [[[null, 'v']]]
    })

    const data = await scanKeyspace({ client, logger: quietLogger, pattern: 'user:*' })

    assert.deepEqual(state.patterns, ['user:*'], 'no prefix means no rewriting')
    assert.deepEqual(data, [{ plain: 'v' }])
  })

  test('leaves keys that do not carry the prefix untouched', async () => {
    const { client } = createClient({
      batches: [['app:mine', 'foreign:key']],
      pipelineResults: [[[null, '1'], [null, '2']]]
    })

    const data = await scanKeyspace({ client, keyPrefix: 'app:', logger: quietLogger })

    assert.deepEqual(data, [{ mine: '1' }, { 'foreign:key': '2' }], 'only a real prefix match is stripped')
  })

  test('skips keys whose read failed instead of rejecting the scan', async () => {
    const { client } = createClient({
      batches: [['a', 'h', 'b']],
      pipelineResults: [[[null, '1'], [new Error('WRONGTYPE'), null], [null, '2']]]
    })

    const data = await scanKeyspace({ client, logger: quietLogger, pattern: '*' })

    assert.deepEqual(data, [{ a: '1' }, { b: '2' }])
  })

  test('processes every batch and dedupes repeated keys', async () => {
    const { client } = createClient({
      batches: [['a'], ['a', 'b'], []],
      pipelineResults: [[[null, '1']], [[null, '1'], [null, '2']]]
    })

    const data = await scanKeyspace({ client, logger: quietLogger })

    assert.deepEqual(data, [{ a: '1' }, { b: '2' }], 'SCAN may return a key more than once')
  })

  test('rejects and destroys the stream when a batch read fails', async () => {
    const { client, state } = createClient({
      batches: [['a']],
      pipelineResults: [],
      fail: 'pipeline'
    })

    await assert.rejects(scanKeyspace({ client, logger: quietLogger }), /pipeline exploded/)
    assert.equal(state.destroyed, true)
  })

  test('rejects when the scan stream itself errors', async () => {
    const { client } = createClient({ batches: [], pipelineResults: [], fail: 'stream' })

    await assert.rejects(scanKeyspace({ client, logger: quietLogger }), /scan exploded/)
  })

  test('deletePattern unlinks each batch and sums the removals', async () => {
    const { client, state } = createClient({
      batches: [['app:a', 'app:b'], ['app:c']],
      pipelineResults: [],
      unlinkResults: [1, 1, 1]
    })

    const removed = await deletePattern({ client, keyPrefix: 'app:', logger: quietLogger, pattern: 'cache:*' })

    assert.equal(removed, 3)
    assert.deepEqual(state.patterns, ['app:cache:*'])
    assert.deepEqual(state.unlinked, [['a', 'b'], ['c']], 'keys are unlinked without the prefix')
    // One UNLINK per key: a variadic one would need every key in the same
    // slot, which nothing guarantees.
    assert.deepEqual(state.pipelines[0], [['unlink', 'a'], ['unlink', 'b']])
  })

  test('deletePattern leaves foreign keys addressable as they came', async () => {
    const { client, state } = createClient({
      batches: [['app:mine', 'foreign:key']],
      pipelineResults: [],
      unlinkResults: [2]
    })

    await deletePattern({ client, keyPrefix: 'app:', logger: quietLogger, pattern: '*' })

    assert.deepEqual(state.unlinked, [['mine', 'foreign:key']], 'only a real prefix match is stripped')
  })

  test('scanning without a pattern sweeps the whole keyspace', async () => {
    const { client, state } = createClient({ batches: [[]], pipelineResults: [] })

    await scanKeyspace({ client, logger: quietLogger })

    assert.deepEqual(state.patterns, ['*'])
  })

  test('deletePattern reports zero for an empty keyspace', async () => {
    const { client, state } = createClient({ batches: [[]], pipelineResults: [], unlinkResults: [] })

    assert.equal(await deletePattern({ client, logger: quietLogger, pattern: '*' }), 0)
    assert.deepEqual(state.unlinked, [], 'an empty batch must not issue UNLINK')
    assert.deepEqual(state.patterns, ['*'], 'no keyPrefix means the raw pattern, not a mangled one')
  })

  // Review finding: the per-key skip used to swallow EVERY error class, but
  // only WRONGTYPE (a non-string key) is documented and safe to skip. A MOVED
  // during a slot migration — which these node-level pipelines never follow —
  // was silently dropped, and the walk resolved with a truncated result that
  // looked complete.
  test('getAllStream skips WRONGTYPE only; any other per-key error is fatal', async () => {
    const wrongtype = createClient({
      batches: [['app:a', 'app:b']],
      pipelineResults: [[[new Error('WRONGTYPE Operation against a key holding the wrong kind of value'), null], [null, '2']]]
    })

    const data = await scanKeyspace({ client: wrongtype.client, keyPrefix: 'app:', logger: quietLogger, pattern: '*' })
    assert.deepEqual(data, [{ b: '2' }], 'a non-string key is skipped, as documented')

    const moved = createClient({
      batches: [['app:a', 'app:b']],
      pipelineResults: [[[new Error('MOVED 3999 127.0.0.1:7002'), null], [null, '2']]]
    })

    await assert.rejects(
      scanKeyspace({ client: moved.client, keyPrefix: 'app:', logger: quietLogger, pattern: '*' }),
      /MOVED/,
      'a truncated result that looks complete is worse than a failure'
    )
  })

  // A pipeline resolves even when one of its commands failed, reporting the
  // failure inside the reply. Reading past that would report a deletion that
  // only partly happened as a clean success.
  test('deletePattern surfaces a key that refused to be removed', async () => {
    const { client } = createClient({
      batches: [['app:a', 'app:b']],
      pipelineResults: [],
      unlinkResults: [1, new Error('WRONGTYPE Operation against a key holding the wrong kind of value')]
    })

    await assert.rejects(
      deletePattern({ client, keyPrefix: 'app:', logger: quietLogger, pattern: '*' }),
      /WRONGTYPE/
    )
  })

  test('deletePattern rejects when the scan stream itself errors', async () => {
    const { client } = createClient({ batches: [], pipelineResults: [], unlinkResults: [], fail: 'stream' })

    await assert.rejects(deletePattern({ client, logger: quietLogger, pattern: '*' }), /scan exploded/)
  })

  // A cluster has no scanStream of its own: each master holds a slice of the
  // keyspace, so a walk means walking every master and merging.
  test('scans every master of a cluster and merges the slices', async () => {
    const first = createClient({ batches: [['app:a']], pipelineResults: [[[null, '1']]] })
    const second = createClient({ batches: [['app:b']], pipelineResults: [[[null, '2']]] })
    // Faithful to ioredis: refreshSlotsCache(callback) is the public way to
    // re-read CLUSTER SLOTS. The walk asks for it first — between a failover
    // and the next refresh a promoted replica is still filed under 'slave', and
    // a walk that trusted the stale roster skipped that shard's slice silently.
    const cluster = {
      refreshed: 0,
      refreshSlotsCache (callback) { cluster.refreshed++; callback(null) },
      nodes: (role) => { cluster.askedFor = role; return [first.client, second.client] }
    }

    const data = await scanKeyspace({ client: cluster, keyPrefix: 'app:', logger: quietLogger })

    assert.deepEqual(data, [{ a: '1' }, { b: '2' }], 'both slices come back')
    assert.equal(cluster.refreshed, 1, 'the roster is refreshed before it is trusted')
    assert.equal(cluster.askedFor, 'master', 'replicas would report the same keys twice')
    assert.deepEqual(first.state.patterns, ['app:*'])
    assert.deepEqual(second.state.patterns, ['app:*'])
  })

  test('deletes across every master of a cluster', async () => {
    const first = createClient({ batches: [['app:a']], pipelineResults: [], unlinkResults: [1] })
    const second = createClient({ batches: [['app:b', 'app:c']], pipelineResults: [], unlinkResults: [1, 1] })
    const cluster = { refreshSlotsCache: (callback) => callback(null), nodes: () => [first.client, second.client] }

    assert.equal(await deletePattern({ client: cluster, keyPrefix: 'app:', logger: quietLogger, pattern: '*' }), 3)
    assert.deepEqual(second.state.pipelines[0], [['unlink', 'b'], ['unlink', 'c']])
  })

  test('deletePattern rejects and destroys the stream when unlink fails', async () => {
    const { client, state } = createClient({
      batches: [['a']],
      pipelineResults: [],
      unlinkResults: [],
      fail: 'unlink'
    })

    await assert.rejects(deletePattern({ client, logger: quietLogger, pattern: '*' }), /unlink exploded/)
    assert.equal(state.destroyed, true)
  })
})

// Fourth full-source review (22/08/2026).
describe('keyspace scanner — review findings', () => {
  // Probed against the real ioredis ScanStream: a paused Readable keeps
  // prefetching (17 SCAN pages queued while paused), and the in-flight batch's
  // resume() used to release them all AFTER the stream's own 'error' had
  // rejected the walk — deleteByPattern went on deleting behind a promise that
  // had already reported failure.
  test('nothing more is processed once the stream has failed', async () => {
    const state = { pipelines: 0, destroyed: 0, resumedAfterDestroy: false }
    let releaseBatch

    const client = {
      scanStream () {
        const handlers = {}
        const stream = {
          on (event, handler) { handlers[event] = handler; return stream },
          pause () {},
          resume () {
            if (state.destroyed) state.resumedAfterDestroy = true
            // The buffered pages, released after the error.
            handlers.data(['app:late-1'])
            handlers.data(['app:late-2'])
          },
          destroy () { state.destroyed++ }
        }

        queueMicrotask(() => {
          handlers.data(['app:first'])
          handlers.error(new Error('SCAN exploded'))
          // A chunk that was already in the pipe when the error fired: a
          // misbehaving stream can still deliver it, and it must be ignored —
          // and so must the 'end' a destroyed Readable may still emit.
          handlers.data(['app:straggler'])
          handlers.end()
        })

        return stream
      },
      pipeline () {
        state.pipelines++

        return { exec: () => new Promise((resolve) => { releaseBatch = () => resolve([[null, 1]]) }) }
      }
    }

    // The rejection lands while the first batch is still in flight; the
    // expectation is attached first so it is never an unhandled rejection.
    const refused = assert.rejects(
      deletePattern({ client, keyPrefix: 'app:', logger: quietLogger, pattern: '*' }),
      /SCAN exploded/
    )

    await new Promise((resolve) => setImmediate(resolve))
    releaseBatch()

    await refused
    await new Promise((resolve) => setImmediate(resolve))

    assert.equal(state.pipelines, 1, 'the buffered pages must not be deleted after the failure')
    assert.equal(state.destroyed, 1, 'the stream is destroyed — once, the late end must not finish the walk again')
    assert.equal(state.resumedAfterDestroy, false, 'a destroyed stream is never resumed')
  })

  // The prefix is a literal inside a glob: 'tenant[a]:' used to match
  // 'tenanta:…' — none of this client's keys, and possibly someone else's.
  test('glob metacharacters in the prefix are matched literally', async () => {
    const { client, state } = createClient({ batches: [], pipelineResults: [] })

    await scanKeyspace({ client, keyPrefix: 'tenant[a]?*\\:', logger: quietLogger, pattern: 'user:*' })

    assert.deepEqual(state.patterns, ['tenant\\[a\\]\\?\\*\\\\:user:*'])
  })

  // ioredis omits MATCH for a falsy pattern: '' with no prefix walked the
  // WHOLE database, '' with a prefix matched only a key named after it.
  test('an empty pattern is refused rather than walking the whole database', async () => {
    const { client } = createClient({ batches: [], pipelineResults: [] })

    await assert.rejects(scanKeyspace({ client, logger: quietLogger, pattern: '' }), {
      code: 'INVALID_ARGUMENT',
      operation: 'getAllStream'
    })
    await assert.rejects(deletePattern({ client, logger: quietLogger, pattern: '' }), {
      code: 'INVALID_ARGUMENT',
      operation: 'deleteByPattern'
    })
  })

  // The walks are independent; wall time is the slowest shard, not the sum.
  test('cluster masters are walked concurrently', async () => {
    const order = []
    const node = (name) => ({
      scanStream () {
        const handlers = {}
        order.push(`${name}:start`)
        queueMicrotask(() => { handlers.data([`app:${name}`]) })

        return {
          on (event, handler) { handlers[event] = handler; return this },
          pause () {},
          resume () { order.push(`${name}:end`); handlers.end() },
          destroy () {}
        }
      },
      pipeline: () => ({ exec: async () => [[null, 'v']] })
    })

    const cluster = { refreshSlotsCache: (cb) => cb(null), nodes: () => [node('a'), node('b')] }

    await scanKeyspace({ client: cluster, keyPrefix: 'app:', logger: quietLogger })

    assert.deepEqual(order.slice(0, 2), ['a:start', 'b:start'], 'both walks start before either finishes')
  })

  test('a roster refresh that fails fails the walk, loudly', async () => {
    const cluster = { refreshSlotsCache: (cb) => cb(new Error('CLUSTERDOWN')), nodes: () => [] }

    await assert.rejects(scanKeyspace({ client: cluster, logger: quietLogger }), /CLUSTERDOWN/)
  })
})

describe('keyspace scanner — cluster walk failures', () => {
  // The first failing shard aborts the others: a partial result is not a result,
  // and the surviving streams must not keep scanning into one nobody will read.
  test('a failing master aborts its siblings', async () => {
    const destroyed = []
    const node = (name, fail) => ({
      scanStream () {
        const handlers = {}
        queueMicrotask(() => { fail ? handlers.error(new Error(`${name} exploded`)) : handlers.data([`app:${name}`]) })

        return {
          on (event, handler) { handlers[event] = handler; return this },
          pause () {},
          resume () {},
          destroy () { destroyed.push(name) }
        }
      },
      pipeline: () => ({ exec: () => new Promise(() => {}) }) // the healthy shard's batch never settles
    })

    const cluster = { refreshSlotsCache: (cb) => cb(null), nodes: () => [node('a', false), node('b', true)] }

    await assert.rejects(scanKeyspace({ client: cluster, keyPrefix: 'app:', logger: quietLogger }), /b exploded/)

    assert.ok(destroyed.includes('a'), 'the healthy shard\'s stream is destroyed too')
    assert.ok(destroyed.includes('b'))
  })

  test('a non-string pattern is refused', async () => {
    const { client } = createClient({ batches: [], pipelineResults: [] })

    await assert.rejects(scanKeyspace({ client, logger: quietLogger, pattern: 42 }), { code: 'INVALID_ARGUMENT', operation: 'getAllStream' })
  })
})

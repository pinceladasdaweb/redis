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

          return commands[0]?.[0] === 'unlink'
            ? commands.map(() => [null, unlinkResults.shift() ?? 1])
            : pipelineResults.shift()
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
    const cluster = { nodes: (role) => { cluster.askedFor = role; return [first.client, second.client] } }

    const data = await scanKeyspace({ client: cluster, keyPrefix: 'app:', logger: quietLogger })

    assert.deepEqual(data, [{ a: '1' }, { b: '2' }], 'both slices come back')
    assert.equal(cluster.askedFor, 'master', 'replicas would report the same keys twice')
    assert.deepEqual(first.state.patterns, ['app:*'])
    assert.deepEqual(second.state.patterns, ['app:*'])
  })

  test('deletes across every master of a cluster', async () => {
    const first = createClient({ batches: [['app:a']], pipelineResults: [], unlinkResults: [1] })
    const second = createClient({ batches: [['app:b', 'app:c']], pipelineResults: [], unlinkResults: [1, 1] })
    const cluster = { nodes: () => [first.client, second.client] }

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

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

      return {
        async exec () {
          if (fail === 'pipeline') throw new Error('pipeline exploded')

          return pipelineResults.shift()
        }
      }
    },

    async unlink (...keys) {
      state.unlinked.push(keys)

      if (fail === 'unlink') throw new Error('unlink exploded')

      return unlinkResults.shift()
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
      unlinkResults: [2, 1]
    })

    const removed = await deletePattern({ client, keyPrefix: 'app:', logger: quietLogger, pattern: 'cache:*' })

    assert.equal(removed, 3)
    assert.deepEqual(state.patterns, ['app:cache:*'])
    assert.deepEqual(state.unlinked, [['a', 'b'], ['c']], 'keys are unlinked without the prefix')
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

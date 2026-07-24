// Unit tests for command argument building: executeCommand is stubbed, so
// no Redis server is involved. Regressions for the falsy-zero and arity bugs
// (AUDIT B1/B2/B8/B9/B10) live here.

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { RedisClient } from '../src/index.js'

const quietLogger = { info () {}, warn () {}, error () {}, debug () {} }

const capture = () => {
  const client = new RedisClient({ logger: quietLogger })
  const calls = []

  client.executeCommand = async (...args) => {
    calls.push(args)
    return null
  }
  client.executeBlockingCommand = async (command, args) => {
    calls.push(['BLOCKING', command, ...args])
    return null
  }

  return { client, calls }
}

describe('command argument building', () => {
  test('xread honors count: 0 and routes block: 0 (block forever) to a dedicated connection', async () => {
    const { client, calls } = capture()

    await client.xread({ count: 0, block: 0 }, ['s1', '0-0'])

    assert.deepEqual(calls[0], ['BLOCKING', 'xread', 'COUNT', 0, 'BLOCK', 0, 'STREAMS', 's1', '0-0'])
  })

  test('xread without block stays on the shared connection', async () => {
    const { client, calls } = capture()

    await client.xread({ count: 10 }, ['s1', '0-0'])

    assert.deepEqual(calls[0], ['xread', 'COUNT', 10, 'STREAMS', 's1', '0-0'])
  })

  test('xreadgroup honors block: 0 and noack', async () => {
    const { client, calls } = capture()

    await client.xreadgroup('g', 'c', { block: 0, noack: true }, ['s1', '>'])

    assert.deepEqual(calls[0], ['BLOCKING', 'xreadgroup', 'GROUP', 'g', 'c', 'BLOCK', 0, 'NOACK', 'STREAMS', 's1', '>'])
  })

  // Regression: a blanket trailing id turned XGROUP DESTROY into a protocol
  // error ('$' sent as a fourth argument).
  test('xgroup builds per-subcommand arities', async () => {
    const { client, calls } = capture()

    await client.xgroup('DESTROY', 'stream', 'group')
    await client.xgroup('CREATE', 'stream', 'group', '$', true)
    await client.xgroup('CREATE', 'stream', 'group')
    await client.xgroup('SETID', 'stream', 'group', '0')
    await client.xgroup('DELCONSUMER', 'stream', 'group', 'consumer-1')

    assert.deepEqual(calls, [
      ['xgroup', 'DESTROY', 'stream', 'group'],
      ['xgroup', 'CREATE', 'stream', 'group', '$', 'MKSTREAM'],
      ['xgroup', 'CREATE', 'stream', 'group', '$'],
      ['xgroup', 'SETID', 'stream', 'group', '0'],
      ['xgroup', 'DELCONSUMER', 'stream', 'group', 'consumer-1']
    ])
  })

  test('xrange and xpending honor count: 0', async () => {
    const { client, calls } = capture()

    await client.xrange('s', '-', '+', { count: 0 })
    await client.xpending('s', 'g', { start: '-', end: '+', count: 0 })

    assert.deepEqual(calls[0], ['xrange', 's', '-', '+', 'COUNT', 0])
    assert.deepEqual(calls[1], ['xpending', 's', 'g', '-', '+', 0])
  })

  // Regression: spop used to force count = 1, silently changing the return
  // type from a single member to an array.
  test('spop only sends a count when one is given', async () => {
    const { client, calls } = capture()

    await client.spop('set')
    await client.spop('set', 2)

    assert.deepEqual(calls[0], ['spop', 'set'])
    assert.deepEqual(calls[1], ['spop', 'set', 2])
  })

  test('mset forwards the object without magic serialization', async () => {
    const { client, calls } = capture()

    await client.mset({ a: '1', b: '2' })

    assert.deepEqual(calls[0], ['mset', { a: '1', b: '2' }])
  })

  test('hmset delegates to hset (HMSET is deprecated)', async () => {
    const { client, calls } = capture()

    await client.hmset('h', { f: 'v' })

    assert.deepEqual(calls[0], ['hset', 'h', { f: 'v' }])
  })

  test('watch and unwatch reject with UNSUPPORTED_OPERATION', async () => {
    const { client } = capture()

    await assert.rejects(client.watch('k'), { name: 'RedisClientError', code: 'UNSUPPORTED_OPERATION' })
    await assert.rejects(client.unwatch(), { name: 'RedisClientError', code: 'UNSUPPORTED_OPERATION' })
  })

  // Review finding: xtrim without a count used to reach the server and fail
  // with a confusing 'value is not an integer' reply.
  test('xtrim without a count rejects with INVALID_ARGUMENT', async () => {
    const { client, calls } = capture()

    await assert.rejects(client.xtrim('s', 'MAXLEN'), { name: 'RedisClientError', code: 'INVALID_ARGUMENT' })
    assert.equal(calls.length, 0, 'nothing must be sent to the server')

    await client.xtrim('s', 'MAXLEN', true, 1000)
    assert.deepEqual(calls[0], ['xtrim', 's', 'MAXLEN', '~', 1000])
  })

  test('getOrSet validates ttl and producer before touching the connection', async () => {
    const { client } = capture()

    await assert.rejects(client.getOrSet('k', 0, () => 'x'), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(client.getOrSet('k', 1.5, () => 'x'), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(client.getOrSetJson('k', 60, 'not-a-function'), { code: 'INVALID_ARGUMENT' })
  })

  test('deleteByPattern requires an explicit non-empty pattern', async () => {
    const { client } = capture()

    await assert.rejects(client.deleteByPattern(), { code: 'INVALID_ARGUMENT' })
    await assert.rejects(client.deleteByPattern(''), { code: 'INVALID_ARGUMENT' })
  })

  // Regression: maxRetryAttempts: 0 was clobbered to Infinity by `||`.
  test('retryStrategy honors maxRetryAttempts: 0', () => {
    const client = new RedisClient({ maxRetryAttempts: 0, logger: quietLogger })

    assert.equal(client.config.retryStrategy(1), null)
  })
})

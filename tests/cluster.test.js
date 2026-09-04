import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { isCluster, masters, nodeKey } from '../src/utils/cluster.js'

describe('cluster helpers', () => {
  test('isCluster duck-types nodes() and tolerates a missing client', () => {
    assert.equal(isCluster({ nodes: () => [] }), true)
    assert.equal(isCluster({}), false)
    assert.equal(isCluster(null), false)
    assert.equal(isCluster(undefined), false)
  })

  test('masters asks a cluster for its masters and wraps a standalone client', () => {
    const a = {}
    const cluster = { nodes: (role) => (role === 'master' ? [a] : []) }

    assert.deepEqual(masters(cluster), [a])
    assert.deepEqual(masters(a), [a])
  })

  test('nodeKey is host:port', () => {
    assert.equal(nodeKey({ options: { host: '10.0.0.1', port: 7001 } }), '10.0.0.1:7001')
  })
})

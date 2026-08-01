import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import RedisClientError, { RedisClientError as Named } from '../src/utils/errors.js'
import { RedisClientError as FromPackage } from '../src/index.js'

describe('redis client error', () => {
  test('carries the operation and defaults to a generic code', () => {
    const error = new RedisClientError('something broke', 'set')

    assert.equal(error.name, 'RedisClientError')
    assert.equal(error.message, 'something broke')
    assert.equal(error.operation, 'set')
    assert.equal(error.code, 'REDIS_CLIENT_ERROR', 'consumers branch on code, so it is never undefined')
    assert.ok(error instanceof Error)
  })

  test('keeps the code it is given', () => {
    assert.equal(new RedisClientError('down', 'get', 'REDIS_UNAVAILABLE').code, 'REDIS_UNAVAILABLE')
  })

  test('is exported from the package root as the same class', () => {
    assert.equal(Named, RedisClientError)
    assert.equal(FromPackage, RedisClientError)
  })
})

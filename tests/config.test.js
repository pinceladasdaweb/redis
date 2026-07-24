import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import RedisConfig from '../src/connection/config.js'

const quietLogger = { info () {}, warn () {}, error () {}, debug () {} }

describe('redis config', () => {
  test('sentinel options are only forwarded when configured', () => {
    const standalone = new RedisConfig({ host: 'h', port: 6379, logger: quietLogger })

    assert.equal('sentinels' in standalone.getOptions(), false)
    assert.equal('name' in standalone.getOptions(), false)

    const sentinel = new RedisConfig({
      logger: quietLogger,
      sentinels: [{ host: 's1', port: 26379 }, { host: 's2', port: 26379 }],
      name: 'mymaster',
      sentinelPassword: 'secret',
      role: 'slave'
    })
    const options = sentinel.getOptions()

    assert.deepEqual(options.sentinels, [{ host: 's1', port: 26379 }, { host: 's2', port: 26379 }])
    assert.equal(options.name, 'mymaster')
    assert.equal(options.sentinelPassword, 'secret')
    assert.equal(options.role, 'slave')
  })

  test('retryStrategy backs off exponentially up to the cap', () => {
    const config = new RedisConfig({
      logger: quietLogger,
      maxRetryAttempts: Infinity,
      baseRetryDelay: 100,
      maxRetryDelay: 500
    })

    assert.equal(config.retryStrategy(1), 200)
    assert.equal(config.retryStrategy(2), 400)
    assert.equal(config.retryStrategy(3), 500)
    assert.equal(config.retryStrategy(10), 500)
  })
})

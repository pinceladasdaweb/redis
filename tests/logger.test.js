import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import defaultLogger, { createLogger } from '../src/logger.js'

describe('logger', () => {
  test('default export exposes all four level methods', () => {
    for (const method of ['error', 'warn', 'info', 'debug']) {
      assert.equal(typeof defaultLogger[method], 'function')
    }
  })

  test('routes levels to the matching console methods', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const warnMock = t.mock.method(console, 'warn', () => {})
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('debug')

    logger.error('boom')
    logger.warn('careful')
    logger.info('hello')
    logger.debug('details')

    assert.equal(errorMock.mock.callCount(), 1)
    assert.equal(warnMock.mock.callCount(), 1)
    assert.equal(logMock.mock.callCount(), 2)
  })

  test('suppresses messages below the configured level', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const warnMock = t.mock.method(console, 'warn', () => {})
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('warn')

    logger.error('shown')
    logger.warn('shown')
    logger.info('hidden')
    logger.debug('hidden')

    assert.equal(errorMock.mock.callCount(), 1)
    assert.equal(warnMock.mock.callCount(), 1)
    assert.equal(logMock.mock.callCount(), 0)
  })

  test('defaults to info level, hiding debug output', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger(undefined)

    logger.info('shown')
    logger.debug('hidden')

    assert.equal(logMock.mock.callCount(), 1)
  })

  test('falls back to info for unknown levels', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('bananas')

    logger.info('shown')
    logger.debug('hidden')

    assert.equal(logMock.mock.callCount(), 1)
  })

  test('prefixes messages with a timestamp and the level name', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('info')

    logger.info('formatted message')

    const line = logMock.mock.calls[0].arguments[0]

    assert.match(line, /^\d{4}-\d{2}-\d{2}T[\d:.]+Z \[info\] formatted message$/)
  })

  test('forwards extra arguments to the console', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('info')
    const details = { key: 'orders' }

    logger.info('with context', details)

    assert.equal(logMock.mock.calls[0].arguments[1], details)
  })
})

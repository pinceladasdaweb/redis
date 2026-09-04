import assert from 'node:assert/strict'
import { test, describe } from 'node:test'
import defaultLogger, { createLogger } from '../src/utils/logger.js'

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

  test('takes its default level from LOG_LEVEL', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})
    const previous = process.env.LOG_LEVEL

    process.env.LOG_LEVEL = 'debug'

    try {
      createLogger().debug('visible because LOG_LEVEL says so')
      assert.equal(logMock.mock.callCount(), 1)
    } finally {
      if (previous === undefined) delete process.env.LOG_LEVEL
      else process.env.LOG_LEVEL = previous
    }
  })

  test('labels each line with its own level', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const warnMock = t.mock.method(console, 'warn', () => {})

    const logger = createLogger('debug')

    logger.error('bad')
    logger.warn('careful')

    assert.match(errorMock.mock.calls[0].arguments[0], /\[error\] bad$/)
    assert.match(warnMock.mock.calls[0].arguments[0], /\[warn\] careful$/)
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

// Fourth full-source review (22/08/2026).
describe('logger — review findings', () => {
  test('level names are case-insensitive', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    createLogger('DEBUG').debug('shown')
    createLogger(' Debug ').debug('shown')

    assert.equal(logMock.mock.callCount(), 2)
  })

  test('silent turns the fallback logger off entirely', (t) => {
    const errorMock = t.mock.method(console, 'error', () => {})
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger('silent')
    logger.error('hidden')
    logger.info('hidden')

    assert.equal(errorMock.mock.callCount(), 0)
    assert.equal(logMock.mock.callCount(), 0)
  })

  // LEVELS['constructor'] was Object — a function, not nullish — so the `??`
  // never fired, `3 > Object` was NaN-false for every level, and an env value
  // of "constructor" logged everything.
  test('prototype property names are not levels', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    for (const name of ['constructor', 'toString', '__proto__', 'hasOwnProperty']) {
      createLogger(name).debug('hidden')
    }

    assert.equal(logMock.mock.callCount(), 0, 'unknown names fall back to info, which hides debug')
  })

  // The default logger used to snapshot LOG_LEVEL at import time, so dotenv
  // loaded after the client import — or process.env set at startup — was
  // ignored. It is read when a line is written.
  test('the default logger reads LOG_LEVEL when a line is written, not at import', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})
    const previous = process.env.LOG_LEVEL

    try {
      process.env.LOG_LEVEL = 'debug'
      defaultLogger.debug('shown now')
      process.env.LOG_LEVEL = 'error'
      defaultLogger.debug('hidden now')

      assert.equal(logMock.mock.callCount(), 1)
    } finally {
      if (previous === undefined) delete process.env.LOG_LEVEL
      else process.env.LOG_LEVEL = previous
    }
  })
})

describe('logger — level coercion edge', () => {
  test('a null level falls back to info like an unknown one', (t) => {
    const logMock = t.mock.method(console, 'log', () => {})

    const logger = createLogger(null)
    logger.info('shown')
    logger.debug('hidden')

    assert.equal(logMock.mock.callCount(), 1)
  })
})

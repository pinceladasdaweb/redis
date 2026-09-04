// Dependency-free default logger. The library logs through whatever the
// application injects via the `logger` option; this console-based fallback
// only exists so the out-of-the-box experience still has visible, leveled
// logs. See the README ("Logging Options") for injecting pino/winston/etc.

// `silent` is a real level: without it there was no way to quiet the fallback
// short of injecting a logger.
const LEVELS = { silent: -1, error: 0, warn: 1, info: 2, debug: 3 }

// Only the four names, in any case — `LOG_LEVEL=DEBUG` is the common
// convention and used to fall silently to info. Own keys only: a plain lookup
// on `LEVELS['constructor']` returned Object (a function, not nullish), the
// `??` never fired, and `3 > Object` is NaN-false for every level — so an env
// value of "constructor" logged everything.
const thresholdFor = (level) => {
  const name = String(level ?? 'info').trim().toLowerCase()

  return Object.hasOwn(LEVELS, name) ? LEVELS[name] : LEVELS.info
}

// `level` may be a value or a function returning one. The default is a
// function so the environment is read when a line is written, not when this
// module was first imported — `import 'dotenv/config'` after the client
// import, or setting process.env at startup, used to be ignored.
const createLogger = (level = () => process.env.LOG_LEVEL || 'info') => {
  const threshold = typeof level === 'function'
    ? () => thresholdFor(level())
    : () => thresholdFor(level)

  const write = (method, levelName) => (message, ...args) => {
    if (LEVELS[levelName] > threshold()) return

    console[method](`${new Date().toISOString()} [${levelName}] ${message}`, ...args)
  }

  return {
    error: write('error', 'error'),
    warn: write('warn', 'warn'),
    info: write('log', 'info'),
    debug: write('log', 'debug')
  }
}

const logger = createLogger()

export { createLogger }
export default logger

import pino from 'pino'

const isDevelopment = process.env.NODE_ENV === 'development'

const config = {
  level: process.env.LOG_LEVEL || 'debug',
  formatters: {
    level: (level) => ({ level }),
    ...(!isDevelopment ? { bindings: (bindings) => ({ hostname: bindings.hostname }) } : {})
  },
  timestamp: () => `,"time":"${new Date(Date.now()).toISOString()}"`,
  ...(isDevelopment && {
    transport: {
      target: 'pino-pretty',
      options: {
        colorize: true,
        levelFirst: true,
        ignore: 'time,pid,hostname'
      }
    }
  })
}

const Logger = pino(config)

export default Logger

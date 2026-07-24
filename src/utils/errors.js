// Structured errors: consumers branch on `code` (and `operation`), never on
// message text — messages change, codes are contract.

class RedisClientError extends Error {
  constructor (message, operation, code = 'REDIS_CLIENT_ERROR') {
    super(message)
    this.name = 'RedisClientError'
    this.operation = operation
    this.code = code
  }
}

export { RedisClientError }
export default RedisClientError

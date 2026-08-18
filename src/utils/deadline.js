import RedisClientError from './errors.js'

// Bounds a driver call that has no timeout of its own.
//
// Born for shutdown: ioredis only answers QUIT right away while the offline
// queue is empty. With anything queued it parks the QUIT behind it and replies
// once the connection is back — which, under the default infinite retry
// policy, may be never. A graceful shutdown that never finishes is worse than
// an abrupt one, so every quit() on the way out gets a deadline.
//
// It has since grown a second caller class: probes that must degrade
// gracefully when a server hangs instead of erroring (the keyspace CONFIG
// probe). Those rejections reach consumers, so the timeout is a structured
// RedisClientError — consumers branch on `code`, never on message text.

const withDeadline = (promise, { clock, ms, operation }) => new Promise((resolve, reject) => {
  const timer = clock.setTimeout(() => {
    reject(new RedisClientError(
      `'${operation}' did not answer within ${ms}ms.`,
      operation,
      'OPERATION_TIMEOUT'
    ))
  }, ms)

  const settle = (fn) => (value) => {
    clock.clearTimeout(timer)
    fn(value)
  }

  // The abandoned promise keeps its handlers, so the late rejection that
  // arrives when the socket is finally forced closed is never unhandled.
  promise.then(settle(resolve), settle(reject))
})

export { withDeadline }
export default withDeadline

// Shared connection settings for every example.
//
// Overridable through environment variables, matching .env.example:
//
//   REDIS_HOST   host (default: 127.0.0.1)
//   REDIS_PORT   port (default: 6379)
//
// Start a local server with `docker compose up -d` before running these.

export const baseConfig = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT || 6379)
}

// Examples print their own progress; this keeps the library's internal logs
// out of the way. Drop it to see everything the client is doing.
export const quietLogger = {
  error: (message) => console.error(`  [redis] ${message}`),
  warn: () => {},
  info: () => {},
  debug: () => {}
}

export const heading = (title) => console.log(`\n${title}\n${'─'.repeat(title.length)}`)

export const done = (summary) => console.log(`\n✅ ${summary}\n`)

export default baseConfig

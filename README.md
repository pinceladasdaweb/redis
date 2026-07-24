# Redis client

A battle-tested Redis client for Node.js applications, built on top of ioredis with advanced resilience mechanisms, automatic reconnection strategies, and comprehensive error handling.

## Features
- **Robust Connection Management**: Intelligent handling of connection lifecycle with automatic reconnection and health checks.
- **Comprehensive Error Handling**: Sophisticated error categorization and management for different Redis connection scenarios.
- **JSON Operations**: Built-in JSON serialization and deserialization for seamless object storage and retrieval.
- **Advanced Health Checks**: Periodic health monitoring with configurable intervals and timeout protection.
- **Exponential Backoff**: Smart retry mechanism with configurable attempts and delay strategies.
- **Rich Redis Operations**: Complete support for Redis data structures (Strings, Hashes, Lists, Sets) with intuitive methods.
- **Connection State Tracking**: Real-time monitoring of connection status with detailed logging.
- **Prefix Management**: Automatic key prefix handling for better organization and multi-tenant support.
- **Stream Operations**: Efficient handling of Redis streams with built-in error recovery.
- **Transaction Support**: Built-in methods for handling Redis transactions and atomic operations.

## Advantages
- **Enhanced Resilience**: Sophisticated connection management ensures your application remains stable even during Redis unavailability.
- **Developer-Friendly**: Clean and intuitive API design makes Redis operations straightforward and easy to implement.
- **Production-Ready**: Battle-tested error handling and reconnection strategies suitable for production environments.
- **Extensive Monitoring**: Comprehensive logging and health checking capabilities for better operational visibility.
- **Flexible Configuration**: Highly configurable settings for retry attempts, timeouts, and connection management.
- **Type Safety**: Consistent method signatures and error handling patterns enhance code reliability.
- **Performance Optimized**: Efficient connection pooling and command execution with timeout protection.
- **Easy Debugging**: Detailed error messages and logging make troubleshooting simpler.
- **Background Recovery**: Non-blocking reconnection attempts ensure application responsiveness.

## Use Cases
- **Caching Layer**: Reliable caching implementation with automatic recovery from connection issues.
- **Session Management**: Robust session storage with configurable expiration and error handling.
- **Rate Limiting**: Dependable rate limiting implementation with atomic operations support.
- **Job Queues**: Reliable message queuing with automatic reconnection and error recovery.
- **Real-time Analytics**: Fast data storage and retrieval for analytics with connection resilience.
- **Distributed Locking**: Robust distributed locking mechanisms with automatic lock release on connection issues.
- **Leaderboards**: High-performance leaderboard implementation using sorted sets with error handling.
- **Microservices State**: Reliable state management for microservices architectures.
- **Event Broadcasting**: Robust pub/sub implementation for event distribution across services.

## Installation

### Create access token

To install private packages you will first need to generate a token from your Github account with write/read permissions. (Yes, you will need a personal token to proceed with the installation of this library). Github has a [great guide](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens) explaining how you can do this.

The next step is to create or edit your .npmrc file (which is located in the root of your project) and add your newly created token.

```bash
@oisamitech:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=PUT_YOUR_TOKEN_HERE
```

### Package manager

Using npm:

```bash
npm install @oisamitech/redis
```

Using yarn:

```bash
yarn add @oisamitech/redis
```

Using pnpm:

```bash
pnpm add @oisamitech/redis
```

Once the package is installed, you can import the library using `import` or `require` approach:

```bash
import Redisclient from '@oisamitech/redis'
```

Or if you use require for importing:

```bash
const { RedisClient } = require('@oisamitech/redis')
```

## Constructor Options

The `RedisClient` constructor accepts an options object with the following parameters:

### Connection Options

- **host** `{string}` *(required)*: Redis server hostname.
  - Example: `'redis.example.com'`

- **port** `{number}` *(required)*: Redis server port.
  - Example: `6379`

- **username** `{string}` *(optional)*: Authentication username.
  - Default: `null`
  - Example: `'your-username'`

- **password** `{string}` *(optional)*: Authentication password.
  - Default: `null`
  - Example: `'your-password'`

- **db** `{number}` *(optional)*: Database number to use.
  - Default: `0`
  - Example: `1`

- **keyPrefix** `{string}` *(optional)*: Prefix to be added to all keys.
  - Default: `''`
  - Example: `'myapp:'`

### Retry and Reconnection Options

- **maxRetryAttempts** `{number}` *(optional)*: Maximum number of retry attempts when connection is lost.
  - Default: `Infinity`
  - Example: `5`
  - Set to `Infinity` for unlimited retries

- **baseRetryDelay** `{number}` *(optional)*: Initial delay between retry attempts in milliseconds.
  - Default: `1000`
  - Example: `2000`
  - Used in exponential backoff calculation

- **maxRetryDelay** `{number}` *(optional)*: Maximum delay between retry attempts in milliseconds.
  - Default: `30000`
  - Example: `60000`
  - Caps the exponential backoff

- **reconnectInterval** `{number}` *(optional)*: Time between reconnection attempts in milliseconds.
  - Default: `5000`
  - Example: `10000`

### Health Check Options

- **healthCheckInterval** `{number}` *(optional)*: Interval between health checks in milliseconds.
  - Default: `5000`
  - Example: `10000`

- **healthCheckTimeout** `{number}` *(optional)*: Timeout for health check operations in milliseconds.
  - Default: `1000`
  - Example: `2000`

### Advanced Options

- **maxRetriesPerRequest** `{number|null}` *(optional)*: Maximum number of retries per command.
  - Default: `null`
  - Example: `3`
  - Set to `null` for unlimited retries

- **enableReadyCheck** `{boolean}` *(optional)*: Checks if Redis server is ready.
  - Default: `true`

- **autoResubscribe** `{boolean}` *(optional)*: Automatically resubscribe to channels after reconnection.
  - Default: `true`

- **autoResendUnfulfilledCommands** `{boolean}` *(optional)*: Automatically resend unfulfilled commands after reconnection.
  - Default: `true`

- **lazyConnect** `{boolean}` *(optional)*: Whether to establish connection on client instantiation.
  - Default: `true`

- **logger** `{object}` *(optional)*: Custom logger instance.
  - Must implement `info`, `warn`, and `error` methods
  - Default: Internal logger implementation

### Example Usage

```javascript
const { RedisClient } = require('@oisamitech/redis')

const redis = new RedisClient({
  // Required connection options
  host: 'redis.example.com',
  port: 6379,
  
  // Optional configuration
  username: 'your-username',
  password: 'secure-password',
  db: 0,
  keyPrefix: 'myapp:',

  // Retry options
  maxRetryAttempts: 5,
  baseRetryDelay: 2000,
  maxRetryDelay: 30000,
  reconnectInterval: 5000,

  // Health check options
  healthCheckInterval: 5000,
  healthCheckTimeout: 1000,

  // Advanced options
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  autoResubscribe: true,
  autoResendUnfulfilledCommands: true,
  lazyConnect: true,

  // Custom logger
  logger: customLogger
})

// Connect to Redis
await redis.connect()
```

## Available Methods

### Connection Methods

- **connect()**: Initiates a connection to Redis server.
  ```javascript
  await redis.connect()
  ```

- **disconnect()**: Gracefully closes the Redis connection.
  ```javascript
  await redis.disconnect()
  ```

- **checkHealth()**: Verifies if the Redis connection is healthy.
  ```javascript
  const isHealthy = await redis.checkHealth()
  ```

### String Operations

- **get(key)**: Retrieves the value of a key.
  ```javascript
  const value = await redis.get('myKey')
  ```

- **set(key, value)**: Sets the string value of a key.
  ```javascript
  await redis.set('myKey', 'myValue')
  ```

- **setex(key, seconds, value)**: Sets the value and expiration of a key.
  ```javascript
  await redis.setex('myKey', 3600, 'myValue') // Expires in 1 hour
  ```

### Hash Operations

- **hset(key, field, value)**: Sets the string value of a hash field.
  ```javascript
  await redis.hset('user:1', 'name', 'John')
  ```

- **hget(key, field)**: Gets the value of a hash field.
  ```javascript
  const name = await redis.hget('user:1', 'name')
  ```

- **hgetall(key)**: Gets all the fields and values in a hash.
  ```javascript
  const userData = await redis.hgetall('user:1')
  ```

- **hmset(key, obj)**: Sets multiple hash fields to multiple values.
  ```javascript
  await redis.hmset('user:1', { name: 'John', email: 'john@example.com' })
  ```

- **hmget(key, ...fields)**: Gets the values of multiple hash fields.
  ```javascript
  const [name, email] = await redis.hmget('user:1', 'name', 'email')
  ```

- **hincrby(key, field, increment)**: Increments the integer value of a hash field by a specified amount.
  ```javascript
  await redis.hincrby('user:1', 'visits', 1)
  ```

- **hexists(key, field)**: Determines if a hash field exists.
  ```javascript
  const exists = await redis.hexists('user:1', 'name')
  ```

- **hdel(key, ...fields)**: Deletes one or more hash fields.
  ```javascript
  await redis.hdel('user:1', 'temporary_field', 'another_field')
  ```

### List Operations

- **lpush(key, ...values)**: Inserts values at the head of a list.
  ```javascript
  await redis.lpush('myList', 'value1', 'value2')
  ```

- **rpop(key)**: Removes and gets the last element in a list.
  ```javascript
  const value = await redis.rpop('myList')
  ```

- **lrange(key, start, stop)**: Gets a range of elements from a list.
  ```javascript
  const elements = await redis.lrange('myList', 0, -1) // Get all elements
  ```

- **llen(key)**: Gets the length of a list.
  ```javascript
  const length = await redis.llen('myList')
  ```

- **lrem(key, count, value)**: Removes elements from a list.
  ```javascript
  await redis.lrem('myList', 2, 'value') // Remove first 2 occurrences of 'value'
  ```

- **lpushx(key, value)**: Inserts a value at the head of a list, only if the list exists.
  ```javascript
  await redis.lpushx('myList', 'newValue')
  ```

- **rpushx(key, value)**: Inserts a value at the tail of a list, only if the list exists.
  ```javascript
  await redis.rpushx('myList', 'newValue')
  ```

### Set Operations

- **sadd(key, ...members)**: Adds members to a set.
  ```javascript
  await redis.sadd('mySet', 'member1', 'member2')
  ```

- **smembers(key)**: Gets all members in a set.
  ```javascript
  const members = await redis.smembers('mySet')
  ```

- **sismember(key, member)**: Determines if a member exists in a set.
  ```javascript
  const isMember = await redis.sismember('mySet', 'member1')
  ```

- **scard(key)**: Gets the number of members in a set.
  ```javascript
  const memberCount = await redis.scard('mySet')
  ```

- **spop(key, count = 1)**: Removes and returns random members from a set.
  ```javascript
  const members = await redis.spop('mySet', 3) // Remove 3 random members
  ```

- **srem(key, ...members)**: Removes specified members from a set.
  ```javascript
  await redis.srem('mySet', 'member1', 'member2')
  ```

### Sort Operations

- **sort(key, options)**: Sorts elements in a list, set or sorted set.
  ```javascript
  const sorted = await redis.sort('myList', {
    by: 'weight_*',
    limit: { offset: 0, count: 10 },
    get: 'object_*',
    direction: 'DESC',
    alpha: true
  })
  ```

### Key Operations

- **del(...keys)**: Deletes one or more keys.
  ```javascript
  await redis.del('key1', 'key2')
  ```

- **expire(key, seconds)**: Sets a key's time to live in seconds.
  ```javascript
  await redis.expire('myKey', 3600)
  ```

- **ttl(key)**: Gets the time to live for a key in seconds.
  ```javascript
  const timeToLive = await redis.ttl('myKey')
  ```

### Multiple Key Operations

- **mset(obj)**: Sets multiple key-value pairs simultaneously.
  ```javascript
  await redis.mset({
    'key1': 'value1',
    'key2': 'value2',
    'key3': { complex: 'object' }
  })
  ```

- **mget(...keys)**: Gets the values of multiple keys.
  ```javascript
  const values = await redis.mget('key1', 'key2', 'key3')
  ```

### Key Management Operations

- **exists(key)**: Checks if a key exists.
  ```javascript
  const keyExists = await redis.exists('myKey')
  ```

- **type(key)**: Gets the type stored at key.
  ```javascript
  const keyType = await redis.type('myKey')
  ```

- **rename(key, newkey)**: Renames a key.
  ```javascript
  await redis.rename('oldKey', 'newKey')
  ```

- **renamenx(key, newkey)**: Renames a key only if the new key does not exist.
  ```javascript
  await redis.renamenx('oldKey', 'newKey')
  ```

- **persist(key)**: Removes the expiration from a key.
  ```javascript
  await redis.persist('myKey')
  ```

### Counter Operations

- **incr(key)**: Increments the integer value of a key by one.
  ```javascript
  await redis.incr('counter')
  ```

- **decr(key)**: Decrements the integer value of a key by one.
  ```javascript
  await redis.decr('counter')
  ```

### JSON Operations

- **setJson(key, value)**: Sets a JSON value for a key with automatic serialization.
  ```javascript
  await redis.setJson('user:1', { name: 'John', age: 30 })
  ```

- **getJson(key)**: Retrieves and automatically deserializes a JSON value.
  ```javascript
  const user = await redis.getJson('user:1')
  ```

- **setexJson(key, seconds, value)**: Sets a JSON value with expiration.
  ```javascript
  await redis.setexJson('user:1', 3600, { name: 'John', age: 30 }) // Expires in 1 hour
  ```

### Transaction Operations

- **multi()**: Starts a Redis transaction.
  ```javascript
  const transaction = await redis.multi()
  ```

- **watch(...keys)**: Watches keys for changes in a transaction.
  ```javascript
  await redis.watch('key1', 'key2')
  ```

- **unwatch()**: Unwatches all keys in a transaction.
  ```javascript
  await redis.unwatch()
  ```

### Stream Operations

- **xadd(key, id, ...args)**: Adds new entries to a stream.
  ```javascript
  await redis.xadd('mystream', '*', 'field1', 'value1', 'field2', 'value2')
  ```

- **xread(options, streams)**: Reads data from one or more streams.
  ```javascript
  await redis.xread({ 
    count: 100, 
    block: 5000 
  }, ['stream1', '0-0', 'stream2', '0-0'])
  ```

- **xreadgroup(groupName, consumerName, options, streams)**: Reads data from a stream as part of a consumer group.
  ```javascript
  await redis.xreadgroup('mygroup', 'consumer1', {
    count: 100,
    block: 5000,
    noack: true
  }, ['stream1', '>', 'stream2', '>'])
  ```

- **xgroup(command, key, groupName, id)**: Manages consumer groups.
  ```javascript
  await redis.xgroup('CREATE', 'mystream', 'mygroup', '$', true) // Create group
  await redis.xgroup('DESTROY', 'mystream', 'mygroup') // Delete group
  ```

- **xlen(key)**: Gets the length of a stream.
  ```javascript
  const length = await redis.xlen('mystream')
  ```

- **xinfo(subcommand, key, ...args)**: Gets information about a stream.
  ```javascript
  const streamInfo = await redis.xinfo('STREAM', 'mystream')
  const groupsInfo = await redis.xinfo('GROUPS', 'mystream')
  ```

- **xrange(key, start, end, options)**: Returns a range of elements from a stream.
  ```javascript
  const messages = await redis.xrange('mystream', '-', '+', { count: 10 })
  ```

- **xrevrange(key, end, start, options)**: Returns a range of elements from a stream, in reverse order.
  ```javascript
  const recentMessages = await redis.xrevrange('mystream', '+', '-', { count: 10 })
  ```

- **xdel(key, ...ids)**: Removes entries from a stream.
  ```javascript
  await redis.xdel('mystream', 'message-id-1', 'message-id-2')
  ```

- **xtrim(key, strategy, approx, count)**: Trims a stream to a specified length.
  ```javascript
  await redis.xtrim('mystream', 'MAXLEN', false, 1000) // Keep exactly 1000 entries
  await redis.xtrim('mystream', 'MAXLEN', true, 1000)  // Approximately 1000 entries
  ```

- **xpending(key, group, options)**: Inspects pending messages in a consumer group.
  ```javascript
  const pending = await redis.xpending('mystream', 'mygroup', {
    start: '-',
    end: '+',
    count: 10,
    consumer: 'consumer1'
  })
  ```

- **xclaim(key, group, consumer, minIdleTime, ...ids)**: Claims pending messages in a consumer group.
  ```javascript
  await redis.xclaim('mystream', 'mygroup', 'consumer1', 3600000, 'message-id-1')
  ```

- **getAllStream**(pattern = '*'): Retrieves all keys and values matching a pattern.
  ```javascript
  const allData = await redis.getAllStream('user:*')
  ```

### Notes

- All methods handle connection issues gracefully with built-in retry mechanisms
- Methods return null when Redis is not healthy or unavailable
- Key prefixes (if configured) are automatically handled for all operations
- All methods use the internal health check mechanism before executing commands
- Error handling is consistent across all methods with detailed logging

## Contributing

**Use issues for everything**

- For a small change, just send a PR.
- For bigger changes open an issue for discussion before sending a PR.
- PR should have:
  - Documentation
  - Example (If it makes sense)
- You can also contribute by:
  - Reporting issues
  - Suggesting new features or enhancements
  - Improve/fix documentation

## Author
- Pedro Assis - [Github](https://github.com/pinceladasdaweb)

import RedisClientError from '../utils/errors.js'

// Registered Lua, the way the lock manager has always run it internally.
//
// `executeCommand('eval', …)` already works and sends the script body on every
// call. That is fine for something occasional and wrong for a hot path: a
// distributed circuit breaker evaluates state on every request, and EVALSHA
// exists precisely so that becomes ~40 bytes instead of a program. Registering
// through the driver's defineCommand also means NOSCRIPT reloads itself after
// a restart or a failover, which the cluster suite proves against a real
// SCRIPT FLUSH.
//
// Scripts are registered under a namespace on the driver, never under their
// bare name: `defineScript('get', …)` would otherwise shadow `client.get`, and
// the collision would only surface as a very confusing bug much later.
const DRIVER_NAMESPACE = 'userScript_'

class ScriptRegistry {
  #definitions = new Map()
  // Which client the definitions are currently installed on, and which of them
  // made it. A connect() cycle builds a brand-new client, so the definitions
  // have to travel to it before the next call.
  #installed = { client: null, names: new Set() }

  constructor ({ connection, logger }) {
    this.connection = connection
    this.logger = logger
  }

  define (name, { lua, numberOfKeys, readOnly = false } = {}) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new RedisClientError(
        'defineScript requires a name to call the script by.',
        'defineScript',
        'INVALID_ARGUMENT'
      )
    }

    if (typeof lua !== 'string' || lua.trim().length === 0) {
      throw new RedisClientError(
        `defineScript('${name}') requires the Lua source as a non-empty string.`,
        'defineScript',
        'INVALID_ARGUMENT'
      )
    }

    // How many of the arguments are KEYS is what makes the script routable in
    // a cluster and prefixable at all, so it is required rather than guessed.
    if (!Number.isInteger(numberOfKeys) || numberOfKeys < 0) {
      throw new RedisClientError(
        `defineScript('${name}') requires numberOfKeys as a non-negative integer (got ${JSON.stringify(numberOfKeys)}).`,
        'defineScript',
        'INVALID_ARGUMENT'
      )
    }

    this.#definitions.set(name, { lua, numberOfKeys, readOnly })
    // A redefinition has to reach the driver too, not just this map.
    this.#installed.names.delete(name)

    this.logger.debug?.(`Lua script '${name}' registered (${numberOfKeys} key(s)).`)
  }

  async run (name, keys = [], args = []) {
    const definition = this.#definitions.get(name)

    if (!definition) {
      throw new RedisClientError(
        `No Lua script named '${name}'. Register it with defineScript() first.`,
        'runScript',
        'INVALID_ARGUMENT'
      )
    }

    if (!Array.isArray(keys) || !Array.isArray(args)) {
      throw new RedisClientError(
        `runScript('${name}') takes the keys and the arguments as two arrays.`,
        'runScript',
        'INVALID_ARGUMENT'
      )
    }

    // The driver splits positionally and cannot tell a misplaced key from a
    // deliberate one: get the boundary wrong and the script reads a key nobody
    // meant to name, on a node nobody meant to reach. We know the arity, so
    // this is a loud error instead of a silent one.
    if (keys.length !== definition.numberOfKeys) {
      throw new RedisClientError(
        `runScript('${name}') expects exactly ${definition.numberOfKeys} key(s), got ${keys.length}.`,
        'runScript',
        'INVALID_ARGUMENT'
      )
    }

    const client = this.#client(name)

    try {
      return await client[DRIVER_NAMESPACE + name](...keys, ...args)
    } catch (err) {
      this.logger.error(`Lua script '${name}' failed: ${err.message}`)

      throw err
    }
  }

  /** Names registered so far, in registration order. */
  get names () {
    return [...this.#definitions.keys()]
  }

  #client (operation) {
    const client = this.connection.assertReady(operation)

    if (this.#installed.client !== client) {
      this.#installed = { client, names: new Set() }
    }

    for (const [name, { lua, numberOfKeys, readOnly }] of this.#definitions) {
      if (this.#installed.names.has(name)) {
        continue
      }

      client.defineCommand(DRIVER_NAMESPACE + name, { lua, numberOfKeys, readOnly })
      this.#installed.names.add(name)
    }

    return client
  }
}

export { ScriptRegistry }
export default ScriptRegistry

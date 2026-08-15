import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import ScriptRegistry from '../src/scripting/scripts.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

// Faithful to ioredis: defineCommand installs a NEW method on the client, and
// calling it splits the arguments positionally at numberOfKeys. A fake that
// accepted any name or ignored the split would approve exactly the mistakes
// this registry exists to prevent.
const createClient = () => {
  const client = { calls: [], defined: [] }

  client.defineCommand = (name, { lua, numberOfKeys, readOnly }) => {
    client.defined.push({ name, lua, numberOfKeys, readOnly })

    client[name] = async (...argv) => {
      client.calls.push({
        name,
        keys: argv.slice(0, numberOfKeys),
        args: argv.slice(numberOfKeys)
      })

      return 'OK'
    }
  }

  return client
}

const createRegistry = () => {
  const clients = [createClient()]
  const logs = []

  const registry = new ScriptRegistry({
    connection: {
      assertReady: () => {
        if (!clients.at(-1)) throw Object.assign(new Error('down'), { code: 'REDIS_UNAVAILABLE' })

        return clients.at(-1)
      }
    },
    logger: { ...quietLogger, error: (message) => logs.push(message) }
  })

  return { registry, clients, logs, cycle: () => clients.push(createClient()) }
}

const LUA = 'return redis.call("get", KEYS[1])'

describe('script registry', () => {
  test('registers lazily and calls the script by name', async () => {
    const { registry, clients } = createRegistry()

    registry.define('fetch', { lua: LUA, numberOfKeys: 1 })
    assert.deepEqual(clients[0].defined, [], 'defining must not need a connection')

    assert.equal(await registry.run('fetch', ['k'], ['a', 'b']), 'OK')

    assert.deepEqual(clients[0].defined, [{
      name: 'userScript_fetch',
      lua: LUA,
      numberOfKeys: 1,
      readOnly: false
    }])
    assert.deepEqual(clients[0].calls, [{ name: 'userScript_fetch', keys: ['k'], args: ['a', 'b'] }])
  })

  // Regression risk that only shows up much later: defineScript('get', …) must
  // not replace client.get on the driver.
  test('a script name can never shadow a driver command', async () => {
    const { registry, clients } = createRegistry()

    registry.define('get', { lua: LUA, numberOfKeys: 1 })
    await registry.run('get', ['k'])

    assert.equal(clients[0].defined[0].name, 'userScript_get')
    assert.equal('get' in clients[0], false, 'the bare name must stay untouched')
  })

  test('the script is reinstalled on the client of the next connection cycle', async () => {
    const { registry, clients, cycle } = createRegistry()

    registry.define('fetch', { lua: LUA, numberOfKeys: 1 })
    await registry.run('fetch', ['k'])
    await registry.run('fetch', ['k'])

    assert.equal(clients[0].defined.length, 1, 'installed once per client, not once per call')

    // connect() builds a brand-new client; the definitions have to travel.
    cycle()
    await registry.run('fetch', ['k'])

    assert.equal(clients[1].defined.length, 1)
    assert.deepEqual(clients[1].calls.at(-1).keys, ['k'])
  })

  test('a script defined after the first call still reaches the driver', async () => {
    const { registry, clients } = createRegistry()

    registry.define('first', { lua: LUA, numberOfKeys: 1 })
    await registry.run('first', ['k'])

    registry.define('second', { lua: LUA, numberOfKeys: 1 })
    await registry.run('second', ['k'])

    assert.deepEqual(clients[0].defined.map((d) => d.name), ['userScript_first', 'userScript_second'])
  })

  test('redefining a name replaces the script on the driver too', async () => {
    const { registry, clients } = createRegistry()

    registry.define('fetch', { lua: LUA, numberOfKeys: 1 })
    await registry.run('fetch', ['k'])

    registry.define('fetch', { lua: 'return 42', numberOfKeys: 1 })
    await registry.run('fetch', ['k'])

    assert.deepEqual(clients[0].defined.map((d) => d.lua), [LUA, 'return 42'])
  })

  // The whole reason keys and args are two arrays: the driver splits
  // positionally and cannot tell a misplaced key from a deliberate one, so the
  // script would read a key nobody named — and route to the wrong node.
  test('the declared key count is enforced on every call', async () => {
    const { registry } = createRegistry()

    registry.define('cas', { lua: LUA, numberOfKeys: 2 })

    for (const keys of [[], ['a'], ['a', 'b', 'c']]) {
      await assert.rejects(registry.run('cas', keys, ['x']), {
        code: 'INVALID_ARGUMENT',
        operation: 'runScript',
        message: new RegExp(`expects exactly 2 key\\(s\\), got ${keys.length}`)
      }, `${keys.length} key(s) must be refused`)
    }

    assert.equal(await registry.run('cas', ['a', 'b'], ['x']), 'OK')
  })

  test('a script with no keys is legitimate', async () => {
    const { registry, clients } = createRegistry()

    registry.define('ping', { lua: 'return 1', numberOfKeys: 0 })

    assert.equal(await registry.run('ping'), 'OK')
    assert.deepEqual(clients[0].calls[0], { name: 'userScript_ping', keys: [], args: [] })
  })

  test('readOnly travels to the driver so cluster reads can hit a replica', async () => {
    const { registry, clients } = createRegistry()

    registry.define('peek', { lua: LUA, numberOfKeys: 1, readOnly: true })
    await registry.run('peek', ['k'])

    assert.equal(clients[0].defined[0].readOnly, true)
  })

  test('running an unregistered name says how to register it', async () => {
    const { registry } = createRegistry()

    await assert.rejects(registry.run('missing', ['k']), {
      code: 'INVALID_ARGUMENT',
      operation: 'runScript',
      message: /No Lua script named 'missing'.*defineScript/s
    })
  })

  test('keys and args must be arrays, not a flat list', async () => {
    const { registry } = createRegistry()

    registry.define('fetch', { lua: LUA, numberOfKeys: 1 })

    for (const [keys, args] of [['k', []], [['k'], 'x'], [null, []], [['k'], null]]) {
      await assert.rejects(registry.run('fetch', keys, args), {
        code: 'INVALID_ARGUMENT',
        operation: 'runScript',
        message: /two arrays/
      }, `${JSON.stringify([keys, args])} must be refused`)
    }
  })

  test('a malformed definition is refused at registration, not at first use', () => {
    const { registry } = createRegistry()

    const bad = [
      ['', { lua: LUA, numberOfKeys: 1 }, /name to call the script by/],
      [undefined, { lua: LUA, numberOfKeys: 1 }, /name to call the script by/],
      [42, { lua: LUA, numberOfKeys: 1 }, /name to call the script by/],
      [{ toString: () => 'sneaky' }, { lua: LUA, numberOfKeys: 1 }, /name to call the script by/],
      ['ok', { lua: '', numberOfKeys: 1 }, /non-empty string/],
      ['ok', { lua: '   ', numberOfKeys: 1 }, /non-empty string/],
      ['ok', { lua: LUA, numberOfKeys: -1 }, /non-negative integer/],
      ['ok', { lua: LUA, numberOfKeys: 1.5 }, /non-negative integer/],
      ['ok', { lua: LUA }, /non-negative integer/],
      ['ok', undefined, /non-empty string/]
    ]

    for (const [name, definition, message] of bad) {
      assert.throws(() => registry.define(name, definition), {
        code: 'INVALID_ARGUMENT',
        operation: 'defineScript',
        message
      }, `${JSON.stringify(definition)} must be refused`)
    }
  })

  test('a failing script is logged and rethrown, never swallowed', async () => {
    const { registry, clients, logs } = createRegistry()

    registry.define('boom', { lua: LUA, numberOfKeys: 1 })
    await registry.run('boom', ['k'])

    clients[0].userScript_boom = async () => { throw new Error('ERR wrong number of args') }

    await assert.rejects(registry.run('boom', ['k']), /wrong number of args/)
    assert.match(logs.at(-1), /Lua script 'boom' failed.*wrong number of args/)
  })

  // The registry logs a debug line on registration, and `logger.debug?.()` is
  // not decoration: a hand-rolled three-level logger is an ordinary thing to
  // inject, and without the guard defineScript would throw on it.
  test('a logger without debug() is not a crash', async () => {
    const { registry } = createRegistry()

    registry.logger = { error () {}, warn () {}, info () {} }

    assert.doesNotThrow(() => registry.define('quiet', { lua: LUA, numberOfKeys: 1 }))
    assert.equal(await registry.run('quiet', ['k']), 'OK')
  })

  test('names lists what is registered, in order', () => {
    const { registry } = createRegistry()

    registry.define('b', { lua: LUA, numberOfKeys: 0 })
    registry.define('a', { lua: LUA, numberOfKeys: 0 })

    assert.deepEqual(registry.names, ['b', 'a'])
  })
})

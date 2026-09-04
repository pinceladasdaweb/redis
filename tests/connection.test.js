import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { describe, test } from 'node:test'
import ConnectionManager from '../src/connection/manager.js'
import { RedisClient } from '../src/index.js'
import { createManualClock } from './helpers/manual-clock.js'

const quietLogger = { error () {}, warn () {}, info () {}, debug () {} }

const createDriverClient = ({ connectFails = false, quitFails = false } = {}) => {
  const client = new EventEmitter()

  client.status = 'wait'
  client.calls = []
  client.connect = async () => {
    client.calls.push('connect')

    if (connectFails) throw new Error('connection refused')

    client.status = 'ready'
    client.emit('ready')
  }
  // Probed against ioredis 6 (16/08 and 22/08/2026). Three facts, each one a
  // lie this fake used to tell:
  //
  //   - quit() RESOLVES while the status is still 'ready'; the socket closes
  //     afterwards. (The fake used to flip 'end' synchronously.)
  //   - The driver assigns status SYNCHRONOUSLY and emits on nextTick, running
  //     close→end in ONE stack. So when a 'close' listener runs, status is
  //     already 'end' — 'close' is never observable. (The fake used to emit
  //     'close' with status 'close', which is what let a dead deferral and a
  //     listener-stripping bug pass their tests.)
  //   - While 'reconnecting' the socket is already destroyed: quit() is
  //     answered locally with 'OK' and disconnect() produces NO events at all.
  //     'end' never comes. (The fake used to emit it anyway.)
  const die = () => {
    if (client.status === 'reconnecting') return

    setImmediate(() => {
      client.status = 'end'
      client.emit('close')
      client.emit('end')
    })
  }

  client.quit = async () => {
    client.calls.push('quit')

    if (quitFails) throw new Error('quit failed')

    die()

    return 'OK'
  }
  client.disconnect = () => {
    client.calls.push('disconnect')
    die()
  }

  return client
}

const createManager = (clientOptions = {}) => {
  const created = []
  const events = []
  const clock = createManualClock()
  // A test can hook the facade emit — the way a supervisor in the application
  // would react to 'close' — and it runs INSIDE the emit, as it does for real.
  const hooks = { onEmit: null }

  const manager = new ConnectionManager({
    redisConfig: {
      createRedisClient: () => {
        const client = createDriverClient(clientOptions)
        created.push(client)
        return client
      }
    },
    logger: quietLogger,
    clock,
    emit: (...args) => {
      events.push(args)
      hooks.onEmit?.(...args)
    }
  })

  return { manager, created, events, clock, hooks }
}

describe('connection manager', () => {
  test('connect creates exactly one client and reports ready', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()

    assert.equal(created.length, 1)
    assert.equal(manager.isConnected, true)
    assert.deepEqual(events, [['ready']])
  })

  test('concurrent connects share a single attempt', async () => {
    const { manager, created } = createManager()

    await Promise.all([manager.connect(), manager.connect(), manager.connect()])

    assert.equal(created.length, 1, 'a connect mutex must prevent duplicate clients')
  })

  // Regression: the client is assigned synchronously, so a second caller used
  // to short-circuit on "a client already exists" and resolve while the
  // connection was still being established — reporting success too early.
  test('a concurrent connect waits for the attempt already in flight', async () => {
    const { manager } = createManager()
    const client = createDriverClient()

    client.connect = async () => {
      await new Promise((resolve) => setTimeout(resolve, 50))
      client.status = 'ready'
      client.emit('ready')
    }
    manager.redisConfig = { createRedisClient: () => client }

    const first = manager.connect()
    const second = manager.connect()

    await second
    assert.equal(manager.isConnected, true, 'awaiting connect() must mean the connection is usable')

    await first
  })

  test('connecting again with a live client is a no-op', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    await manager.connect()

    assert.equal(created.length, 1)
  })

  test('a failed initial connect keeps the client for background retries', async () => {
    const { manager, created } = createManager({ connectFails: true })

    await manager.connect()

    assert.equal(created.length, 1)
    assert.equal(manager.isConnected, false)
    assert.equal(manager.client, created[0], 'the driver keeps retrying on this client')
    // The settle race armed a once('ready') that never fired; it must be
    // detached, or it piles up and fires against a later state.
    assert.equal(created[0].listenerCount('ready'), 1, 'only the manager\'s own ready handler remains')
    assert.equal(created[0].listenerCount('end'), 1)
  })

  // A driver that rejects with the client already dead must not leave the
  // reference behind: connect() would short-circuit on it forever.
  test('a client that dies during connect is released, and connect can retry', async () => {
    const { manager } = createManager()
    const dead = createDriverClient()

    dead.connect = async () => {
      dead.status = 'end'
      throw new Error('gave up without emitting end')
    }

    const healthy = createDriverClient()
    let next = dead
    manager.redisConfig = { createRedisClient: () => next }

    await manager.connect()

    assert.equal(manager.client, null, 'the dead client must not stay assigned')
    assert.equal(manager.isConnected, false)

    // A later connect() starts a fresh cycle instead of reusing the corpse.
    next = healthy
    await manager.connect()

    assert.equal(manager.client, healthy)
    assert.equal(manager.isConnected, true)
  })

  test('driver lifecycle events reach the facade', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    const client = created[0]

    client.emit('close')
    assert.equal(manager.isConnected, false)

    client.emit('reconnecting', 250)
    client.emit('error', new Error('socket reset'))
    client.emit('end')

    assert.deepEqual(events.map(([name]) => name), ['ready', 'close', 'reconnecting', 'connectionError', 'end'])
    assert.equal(events.find(([name]) => name === 'reconnecting')[1], 250)
    assert.equal(events.some(([name]) => name === 'error'), false, "'error' would crash listener-less processes")
  })

  test('end releases the client so a later connect starts a fresh cycle', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    created[0].emit('end')

    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)

    await manager.connect()
    assert.equal(created.length, 2)
  })

  test('assertReady gates commands on the driver status', async () => {
    const { manager, created } = createManager()

    assert.throws(() => manager.assertReady('set'), {
      name: 'RedisClientError',
      code: 'REDIS_UNAVAILABLE',
      operation: 'set'
    })

    await manager.connect()
    assert.equal(manager.assertReady('set'), created[0])

    created[0].status = 'reconnecting'
    assert.throws(() => manager.assertReady('get'), { code: 'REDIS_UNAVAILABLE' })
  })

  test('disconnect quits, releases and stays disconnected', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit'])
    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)
    assert.deepEqual(events.map(([name]) => name).at(-1), 'end')
  })

  test('disconnect is idempotent and safe before connect', async () => {
    const { manager, created } = createManager()

    await manager.disconnect()
    await manager.connect()
    await manager.disconnect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit'])
  })

  test('disconnect forces the socket closed when quit fails', async () => {
    const { manager, created } = createManager({ quitFails: true })

    await manager.connect()
    await manager.disconnect()

    assert.deepEqual(created[0].calls, ['connect', 'quit', 'disconnect'])
    assert.equal(manager.client, null)
  })

  test('disconnect reacts to the end event, never to its escape timer', async () => {
    const { manager, clock } = createManager()

    await manager.connect()
    await manager.disconnect()

    // Resolved without the clock moving at all, and nothing left armed.
    assert.equal(clock.pending(), 0, 'the escape timer must be cleared by the end event')
  })

  // A driver that goes quiet must not hang shutdown forever: the escape timer
  // is the deadline, and it fires exactly when it says it does.
  test('disconnect gives up on a silent driver after exactly two seconds', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].quit = async () => {
      created[0].calls.push('quit')
      return 'OK'
    }

    let done = false
    const shutdown = manager.disconnect().then(() => { done = true })

    await clock.advance(1999)
    assert.equal(done, false, 'one millisecond before the deadline it is still waiting')

    await clock.advance(1)
    await shutdown

    assert.equal(done, true, 'and exactly on it, shutdown completes anyway')
    assert.equal(manager.client, null, 'the client is released regardless')
  })

  // Regression: ioredis only answers QUIT while its offline queue is empty.
  // With anything queued it parks the QUIT behind it and replies once the
  // connection is back — which, under the default infinite retries, is never.
  // The escape timer above used to sit BEHIND this await, so it could not fire
  // and disconnect() hung for the lifetime of the process.
  test('disconnect gives up on a quit that never answers', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    let done = false
    const shutdown = manager.disconnect().then(() => { done = true })

    await clock.advance(1999)
    assert.equal(done, false, 'one millisecond before the deadline it is still waiting')

    await clock.advance(1)
    await shutdown

    assert.equal(done, true, 'and on the deadline it finishes instead of hanging forever')
    assert.deepEqual(created[0].calls, ['connect', 'quit', 'disconnect'], 'the socket is forced closed')
    assert.equal(manager.client, null, 'and the client is released')
  })

  // Regression: disconnect() left the in-flight connect promise in place, so
  // the next connect() joined an attempt whose client had already been closed
  // — it resolved with nothing behind it and every command failed until a
  // third connect() happened to build a real one.
  test('connect after a disconnect mid-attempt starts a fresh cycle', async () => {
    const created = []
    const clock = createManualClock()
    let releaseFirstConnect

    const manager = new ConnectionManager({
      redisConfig: {
        createRedisClient: () => {
          const client = createDriverClient()

          if (created.length === 0) {
            // The first attempt is still negotiating when shutdown arrives.
            client.connect = () => {
              client.calls.push('connect')

              return new Promise((resolve) => { releaseFirstConnect = resolve })
            }
          }

          created.push(client)

          return client
        }
      },
      logger: quietLogger,
      clock,
      emit: () => {}
    })

    const first = manager.connect()
    await new Promise((resolve) => setImmediate(resolve))

    await manager.disconnect()

    const second = manager.connect()
    releaseFirstConnect()
    await Promise.all([first, second])

    assert.equal(created.length, 2, 'the second connect must build its own client')
    assert.equal(manager.client, created[1], 'and leave that client in place')
    assert.equal(manager.isConnected, true)
  })

  // Review finding: the reuse branch never checked liveness, so a connect()
  // during the (up to ~4s) teardown window resolved successfully against a
  // client that 'end' was about to null — "connected", with nothing behind it
  // and no retry in flight.
  test('connect() during disconnect() waits for the teardown and starts fresh', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    // A quit that never answers pins the teardown on its 2s deadline.
    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const teardown = manager.disconnect()
    const reconnect = manager.connect()

    let reconnected = false
    reconnect.then(() => { reconnected = true })

    await clock.advance(1999)
    assert.equal(reconnected, false, 'connect() must wait out the teardown, not race it')
    assert.equal(created.length, 1, 'and must not build a client while the old one is dying')

    await clock.advance(1)
    await teardown
    await reconnect

    assert.equal(created.length, 2, 'a fresh cycle starts once the teardown finished')
    assert.equal(manager.client, created[1], 'and the caller gets the LIVE client')
    assert.equal(manager.isConnected, true)
  })

  // Review finding: the mutex was checked BEFORE the two waits above (a
  // teardown in flight, the 'close' ambiguity deferral), so a caller that
  // arrived during one of them sailed past it and out through the "reusing an
  // existing client" branch — resolving while the connection was still being
  // built. A guard is only a mutex if it is claimed before the first
  // suspension point, which is why the whole cycle now lives behind it.
  test('concurrent connects during a teardown all wait for the live client', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    // The replacement takes a real handshake: with an instant one the second
    // caller resumes to find a client that is ALREADY ready, and the window
    // this test exists for never opens.
    const slow = createDriverClient()
    slow.connect = async () => {
      slow.calls.push('connect')
      await new Promise((resolve) => setTimeout(resolve, 50))
      slow.status = 'ready'
      slow.emit('ready')
    }
    manager.redisConfig = { createRedisClient: () => { created.push(slow); return slow } }

    const teardown = manager.disconnect()
    const first = manager.connect()
    const second = manager.connect()

    await clock.advance(2000)
    await teardown
    await second

    assert.equal(manager.isConnected, true, 'awaiting the SECOND connect() must mean the connection is usable')
    assert.equal(created.length, 2, 'and both callers share one fresh cycle')

    await first
    assert.equal(manager.client, slow)
  })

  // Same hole, reached through the other wait: the one-turn deferral that
  // resolves the close→reconnecting / close→end ambiguity. The deferral ends
  // on 'end' here, so the cycle builds a fresh client — and that is precisely
  // the window in which a second caller used to find a half-built one and
  // report success on it.
  test('concurrent connects from a closed socket all wait for the live client', async () => {
    const { manager, created } = createManager()

    await manager.connect()

    // The driver gives up one turn later, exactly as the deferral expects.
    created[0].status = 'close'
    created[0].emit('close')
    setImmediate(() => { created[0].status = 'end' })

    // A handshake slow enough that "already exists" is reachable while the
    // replacement is still connecting.
    const slow = createDriverClient()
    slow.connect = async () => {
      slow.calls.push('connect')
      await new Promise((resolve) => setTimeout(resolve, 50))
      slow.status = 'ready'
      slow.emit('ready')
    }
    manager.redisConfig = { createRedisClient: () => { created.push(slow); return slow } }

    const first = manager.connect()
    const second = manager.connect()

    await second
    assert.equal(manager.isConnected, true, 'the second caller must not resolve on a half-built client')
    assert.equal(manager.client, slow)
    assert.equal(created.length, 2, 'and must not have built a second replacement')

    await first
  })

  // The client is assigned SYNCHRONOUSLY inside connect(), and that is what
  // lets a disconnect() issued in the same tick find something to tear down.
  // Any extra suspension point before #establishConnection — an await on a
  // teardown that is not in flight, a deferral taken when the socket is not
  // closed — hands that disconnect() a null client, which it reports as
  // nothing to do, while the connect that resumes afterwards builds a client
  // nobody will ever close.
  test('a disconnect() issued in the same tick as connect() still tears it down', async () => {
    const { manager, created } = createManager()

    const connecting = manager.connect()
    const teardown = manager.disconnect()

    await Promise.all([connecting, teardown])

    assert.equal(created.length, 1, 'exactly one client is built')
    assert.ok(created[0].calls.includes('quit'), 'and the teardown must have reached it')
    assert.equal(manager.client, null, 'no client may outlive the disconnect that raced it')
    assert.equal(manager.isConnected, false)
  })

  // Cluster-shaped: a standalone client never shows status 'close' to a
  // listener, but a Cluster's pool-'drain' path does, and the deferral exists
  // for it. It is a real suspension point, so a disconnect() can land inside
  // it — a supervisor reconnecting from 'close' while the application is
  // shutting down. The caller's LAST word was disconnect, so the reconnect
  // must wait the teardown out and then stand down, not build a client the
  // caller no longer wants.
  test('a disconnect() that lands during the close deferral wins', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].status = 'close'
    created[0].emit('close')
    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const reconnect = manager.connect()
    const teardown = manager.disconnect()

    let reconnected = false
    reconnect.then(() => { reconnected = true })

    await clock.advance(1999)
    assert.equal(reconnected, false, 'the reconnect must wait out the teardown it collided with')
    assert.equal(created.length, 1, 'and must not build against a client that is being quit')

    await clock.advance(1)
    await teardown
    await reconnect

    assert.equal(created.length, 1, 'the later disconnect() is the last word: no fresh cycle')
    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)
  })

  // Review finding: a disconnect() that JOINED an in-flight teardown never ran
  // #teardown, so it never abandoned a connect() queued behind that teardown.
  // The connect resumed once the join resolved and built a live client — the
  // caller's last word was "disconnect", yet isConnected came back true.
  test('a connect() queued behind a teardown is abandoned by a later disconnect()', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const first = manager.disconnect()
    const queued = manager.connect()
    const second = manager.disconnect()

    await clock.advance(2000)
    await Promise.all([first, second, queued])

    assert.equal(created[0].calls.filter((c) => c === 'quit').length, 1, 'the second disconnect() joined the teardown, not doubled it')
    assert.equal(created.length, 1, 'the queued connect() must not build a client nobody wants')
    assert.equal(manager.client, null)
    assert.equal(manager.isConnected, false)
  })

  // ...whereas with connect() as the last word, the queued attempt proceeds:
  // the two tests together pin "last word wins".
  test('a connect() queued behind a teardown proceeds when nothing countermands it', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const teardown = manager.disconnect()
    const queued = manager.connect()

    await clock.advance(2000)
    await Promise.all([teardown, queued])

    assert.equal(created.length, 2)
    assert.equal(manager.client, created[1])
    assert.equal(manager.isConnected, true)
  })

  // Review finding, probed against ioredis 6: while the client is
  // 'reconnecting' the socket is already destroyed. quit() is answered
  // locally — disconnect() + 'OK' — and that disconnect() can produce no
  // 'close', so 'end' NEVER arrives. Every shutdown during an outage sat out
  // the whole 2s escape, logged "disconnected successfully", and released in
  // silence: the facade 'end' event was never emitted for that cycle.
  test('a teardown while reconnecting ends the cycle at once, with a facade end', async () => {
    const { manager, created, events, clock } = createManager()

    await manager.connect()
    const client = created[0]

    // The outage: driver gave up on the socket and is between retries.
    client.status = 'reconnecting'
    client.emit('close')
    client.emit('reconnecting', 2000)
    client.flushQueue = (err) => { client.calls.push(['flushQueue', err.code, err.operation]) }

    await manager.disconnect()

    assert.equal(client.calls.includes('quit'), false, 'there is nothing to say goodbye to')
    assert.ok(client.calls.includes('disconnect'), 'the driver retry must be cancelled')
    assert.deepEqual(client.calls.at(-1), ['flushQueue', 'REDIS_UNAVAILABLE', 'disconnect'], 'parked commands are rejected, best effort, under the shutdown\'s name')
    assert.deepEqual(clock.delays(), [], 'no escape timer: the outcome is known immediately')
    assert.equal(events.at(-1)[0], 'end', 'the cycle ended, and the facade must hear it')
    assert.equal(manager.client, null)
    assert.equal(client.listenerCount('end'), 0)
  })

  // Review finding, probed against ioredis 6: Cluster.connect() removes its
  // own close listener at 'refresh' and, when the first ready-check reports
  // cluster_state:fail, calls disconnect(true) without ever resolving or
  // rejecting. The cluster becomes 'ready' on its own moments later while
  // `await client.connect()` hangs forever — pinning the connect slot for the
  // life of the process. The cycle must settle on the client's own 'ready'.
  test('connect() settles on the client becoming ready even if the driver promise never does', async () => {
    const { manager, created } = createManager()
    const client = createDriverClient()

    client.connect = () => {
      client.calls.push('connect')

      // The orphaned promise; the client reaches 'ready' through its internal
      // reconnect, which this models one turn later.
      setImmediate(() => {
        client.status = 'ready'
        client.emit('ready')
      })

      return new Promise(() => {})
    }
    manager.redisConfig = { createRedisClient: () => { created.push(client); return client } }

    await manager.connect()

    assert.equal(manager.isConnected, true)
    assert.equal(manager.client, client)

    // ...and the slot is free again: a later connect() is a cheap no-op, not
    // a join on a promise that never resolves.
    await manager.connect()
    assert.equal(created.length, 1)
  })

  test('connect() gives up when the client ends before it was ready, whatever the driver promise does', async () => {
    const { manager, created, events } = createManager()
    const client = createDriverClient()

    client.connect = () => {
      client.calls.push('connect')
      setImmediate(() => {
        client.status = 'end'
        client.emit('close')
        client.emit('end')
      })

      return new Promise(() => {})
    }
    manager.redisConfig = { createRedisClient: () => { created.push(client); return client } }

    await manager.connect()

    assert.equal(manager.client, null, 'a client that ended before ready is released')
    assert.equal(manager.isConnected, false)
    assert.equal(events.filter(([name]) => name === 'end').length, 1, 'exactly one facade end')
  })

  // Review finding: the facade's #closing gate and the manager's readiness
  // gate were two gates with two labels. The manager is the ONE gate now, and
  // it refuses work from the moment a shutdown begins — before quit() has
  // touched the driver — under the operation the caller named.
  test('assertReady refuses work from beginShutdown() on, under the caller\'s operation', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    manager.beginShutdown()

    assert.equal(created[0].status, 'ready', 'the driver has not been told yet')
    assert.equal(manager.closing, true)
    assert.throws(() => manager.assertReady('xread'), {
      code: 'REDIS_UNAVAILABLE',
      operation: 'xread',
      message: /disconnect\(\) is in progress/
    })

    await manager.disconnect()
    await manager.connect()

    assert.equal(manager.closing, false, 'connect() reopens the gate')
    assert.doesNotThrow(() => manager.assertReady('xread'))
  })

  test('disconnect() is joined, never doubled, while a teardown is in flight', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()

    created[0].quit = () => {
      created[0].calls.push('quit')

      return new Promise(() => {})
    }

    const first = manager.disconnect()
    const second = manager.disconnect()

    await clock.advance(2000)
    await Promise.all([first, second])

    assert.equal(created[0].calls.filter((c) => c === 'quit').length, 1, 'one teardown, however many callers')
  })

  // Probed against ioredis 6 (22/08/2026): setStatus assigns the status
  // SYNCHRONOUSLY and emits on nextTick, and a give-up runs close→end in ONE
  // stack. So a supervisor reconnecting from the facade's 'close' event runs
  // with status ALREADY 'end' — and the driver's 'end' emit still queued
  // behind the 'close' being handled. This test used to call the supervisor
  // before the emit, with status 'close': an ordering the driver never
  // produces, and the reason a listener-stripping bug passed its own test.
  // With the real ordering the manager stripped its 'end' handler on the
  // corpse and the facade never emitted 'end' for that cycle.
  test('a connect() issued from the close handler of a give-up starts fresh and still ends the old cycle', async () => {
    const { manager, created, events, hooks } = createManager()

    await manager.connect()
    const dying = created[0]

    // The supervisor: reconnect the moment the facade reports 'close'. It
    // runs INSIDE the emit, exactly where an application handler would.
    let reconnect = null
    hooks.onEmit = (name) => {
      if (name === 'close') reconnect = manager.connect()
    }

    // The give-up cascade as the driver produces it: status is 'end' before
    // either listener runs; the two emits follow in order.
    dying.status = 'end'
    dying.emit('close')
    dying.emit('end')

    await reconnect

    assert.equal(created.length, 2, 'the supervisor must get a fresh cycle, not the corpse')
    assert.equal(manager.client, created[1])
    assert.equal(manager.isConnected, true)
    assert.deepEqual(
      events.map(([name]) => name),
      ['ready', 'close', 'end', 'ready'],
      'the dead cycle must still be reported as ended — exactly once'
    )
    assert.equal(dying.listenerCount('end'), 0, 'and the corpse carries nothing')
  })

  test('a connect() during a mere flap keeps the flapping client', async () => {
    const { manager, created, hooks } = createManager()

    await manager.connect()
    const flapping = created[0]

    // The flap cascade: close→reconnecting in one stack, so the supervisor
    // sees 'reconnecting'. The driver keeps retrying on the SAME client —
    // building a second one would duplicate it.
    let reconnect = null
    hooks.onEmit = (name) => {
      if (name === 'close') reconnect = manager.connect()
    }

    flapping.status = 'reconnecting'
    flapping.emit('close')
    flapping.emit('reconnecting', 50)

    await reconnect

    assert.equal(created.length, 1, 'the driver owns the retry — no second client')
    assert.equal(manager.client, flapping)
  })

  test('connect() refuses to reuse a client the driver already ended', async () => {
    const { manager, created } = createManager()

    await manager.connect()

    // The give-up path can leave #client set with status 'end' for the tick
    // between the status flip and the 'end' handler running.
    created[0].status = 'end'

    await manager.connect()

    assert.equal(created.length, 2, 'an ended client is never "reused"')
    assert.equal(manager.client, created[1])
    assert.equal(created[0].listenerCount('end'), 0, 'the corpse must be released, not just replaced')
  })

  test('reconnecting is reported even when the driver omits the delay', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    created[0].emit('reconnecting')

    assert.deepEqual(events.at(-1), ['reconnecting', undefined])
  })

  test('disconnect skips quit for an already ended client', async () => {
    const { manager, created } = createManager()

    await manager.connect()
    created[0].status = 'end'
    created[0].calls.length = 0

    await manager.disconnect()

    assert.deepEqual(created[0].calls, [])
    assert.equal(manager.client, null)
  })

  test('disconnecting an already ended client schedules no wait at all', async () => {
    const { manager, created, clock } = createManager()

    await manager.connect()
    created[0].status = 'end'

    await manager.disconnect()

    assert.deepEqual(clock.delays(), [], 'there is no end event left to wait for')
  })
})

// The facade must wire the real collaborators together: these run with a fake
// driver but no fake collaborators, so a broken constructor is caught.
describe('facade wiring', () => {
  const createFacade = (options = {}) => {
    const redis = new RedisClient({ logger: quietLogger, ...options })
    const driver = createDriverClient()

    driver.ping = async () => 'PONG'
    redis.connection.redisConfig = { createRedisClient: () => driver }

    return { redis, driver }
  }

  // Review finding: disconnect() never touched the HealthChecker, and an
  // in-flight probe's timer is deliberately ref'd (it is awaited) — a PING
  // that would never be answered kept the loop alive for up to
  // healthCheckTimeout after disconnect() resolved.
  test('disconnect() cancels an in-flight health probe', async () => {
    const clock = createManualClock()
    const { redis, driver } = createFacade({ clock, healthCheckTimeout: 30000 })

    // A wedged server: the PING never settles on its own.
    driver.ping = () => new Promise(() => {})

    await redis.connect()

    const probe = redis.checkHealth()
    await redis.disconnect()

    assert.equal(await probe, false, 'the cancelled probe settles as unhealthy')
    assert.equal(clock.pending(), 0, 'and its 30s timer must not survive disconnect()')
  })

  // The subscriber runs on its own connection, so its traffic has to be
  // bridged onto the facade explicitly. Without this, redis.on('message')
  // stays silent while the handler passed to subscribe() still fires — half
  // the documented API working is worse than none of it.
  test('subscriber traffic reaches the facade events too', async () => {
    const { redis, driver } = createFacade()
    const subscriber = new EventEmitter()

    subscriber.status = 'ready'
    subscriber.subscribe = async () => 1
    subscriber.psubscribe = async () => 1
    driver.duplicate = () => subscriber

    await redis.connect()

    const seen = []
    redis.on('message', (...args) => seen.push(['message', ...args]))
    redis.on('pmessage', (...args) => seen.push(['pmessage', ...args]))
    redis.on('connectionError', (err) => seen.push(['connectionError', err.message]))

    await redis.subscribe('news')
    await redis.psubscribe('logs.*')

    subscriber.emit('message', 'news', 'hello')
    subscriber.emit('pmessage', 'logs.*', 'logs.app', 'entry')
    subscriber.emit('error', new Error('subscriber socket reset'))

    assert.deepEqual(seen, [
      ['message', 'news', 'hello'],
      ['pmessage', 'logs.*', 'logs.app', 'entry'],
      ['connectionError', 'subscriber socket reset']
    ])
  })

  // The library logs through whatever the application injects; the built-in
  // console logger only exists so the out-of-the-box experience still has
  // visible logs. Omitting the option must land on it, not on undefined.
  test('without a logger the client falls back to the built-in one', () => {
    const redis = new RedisClient({ host: 'h', port: 6379 })

    for (const level of ['error', 'warn', 'info', 'debug']) {
      assert.equal(typeof redis.logger[level], 'function', `the fallback logger must expose ${level}()`)
    }
  })

  // Drivers do not always emit an Error: a bare string or a plain object must
  // still produce a readable line instead of "undefined".
  test('a non-Error failure is still logged and re-emitted', async () => {
    const logged = []
    const { redis, driver } = createFacade({
      logger: { ...quietLogger, error: (message) => logged.push(message) }
    })

    await redis.connect()

    const seen = []
    redis.on('connectionError', (err) => seen.push(err))

    driver.emit('error', 'ECONNRESET without an Error wrapper')

    assert.deepEqual(seen, ['ECONNRESET without an Error wrapper'])
    assert.match(logged.at(-1), /ECONNRESET without an Error wrapper/)
  })

  test('driver events are re-emitted by the client itself', async () => {
    const { redis, driver } = createFacade()
    const seen = []

    for (const event of ['ready', 'close', 'reconnecting', 'connectionError', 'end']) {
      redis.on(event, () => seen.push(event))
    }

    await redis.connect()
    driver.emit('close')
    driver.emit('reconnecting', 100)
    driver.emit('error', new Error('socket reset'))
    driver.emit('end')

    assert.deepEqual(seen, ['ready', 'close', 'reconnecting', 'connectionError', 'end'])
  })

  test('the health checker reads the live connection', async () => {
    const { redis } = createFacade()

    assert.equal(await redis.checkHealth(), false, 'no connection means unhealthy')

    await redis.connect()
    assert.equal(await redis.checkHealth(), true)
  })

  test('constructor defaults reach the collaborators', () => {
    const { redis } = createFacade()

    assert.equal(redis.keyPrefix, '', 'no prefix by default')
    assert.equal(redis.redisConfig.maxRetryAttempts, Infinity)
    assert.equal(redis.redisConfig.baseRetryDelay, 1000)
    assert.equal(redis.redisConfig.maxRetryDelay, 30000)
    assert.equal(redis.health.interval, 5000)
    assert.equal(redis.health.timeout, 1000)
    assert.equal(redis.connection.logger, redis.logger, 'collaborators share the injected logger')
    assert.equal(redis.subscriptions.logger, redis.logger)
    assert.equal(redis.locks.logger, redis.logger)
  })

  test('constructor options override every default', () => {
    const { redis } = createFacade({
      keyPrefix: 'app:',
      maxRetryAttempts: 3,
      baseRetryDelay: 25,
      maxRetryDelay: 250,
      healthCheckInterval: 111,
      healthCheckTimeout: 222
    })

    assert.equal(redis.keyPrefix, 'app:')
    assert.equal(redis.redisConfig.maxRetryAttempts, 3)
    assert.equal(redis.redisConfig.baseRetryDelay, 25)
    assert.equal(redis.redisConfig.maxRetryDelay, 250)
    assert.equal(redis.health.interval, 111)
    assert.equal(redis.health.timeout, 222)
  })

  test('zero is honored where it means something', () => {
    const { redis } = createFacade({
      maxRetryAttempts: 0,
      baseRetryDelay: 0,
      maxRetryDelay: 0,
      healthCheckInterval: 0
    })

    assert.equal(redis.redisConfig.maxRetryAttempts, 0)
    assert.equal(redis.redisConfig.baseRetryDelay, 0)
    assert.equal(redis.redisConfig.maxRetryDelay, 0)
    assert.equal(redis.health.interval, 0)
  })

  // Review finding: 0 used to be accepted for the two TIMEOUTS as well, where
  // it is not "none" but a 0ms timer that beats every reply — ioredis's
  // Command.setTimeout has no `ms > 0` guard, and neither did the health
  // probe. `commandTimeout: 0` (a common idiom for "no timeout") failed every
  // command; `healthCheckTimeout: 0` made checkHealth() permanently false.
  test('zero is refused for the timeouts, where it means "instantly"', () => {
    for (const name of ['commandTimeout', 'healthCheckTimeout']) {
      assert.throws(() => createFacade({ [name]: 0 }), {
        code: 'INVALID_OPTION',
        operation: 'constructor',
        message: /0 is not "no timeout"/
      }, `${name}: 0 must not arm a 0ms timer`)
    }

    // connectTimeout: 0 IS disabled in the driver (a truthiness check), so it
    // stays legitimate.
    assert.doesNotThrow(() => createFacade({ connectTimeout: 0 }))
  })
})

// Fourth full-source review (22/08/2026): the facade's own shutdown promise.
describe('facade shutdown ordering', () => {
  // Faithful to the driver: every cycle is a NEW client object. A fake that
  // handed the same object back let a previous cycle's queued 'end' land on
  // the next cycle's fresh connection.
  const createFacade = (options = {}) => {
    const redis = new RedisClient({ logger: quietLogger, ...options })
    const drivers = []

    redis.connection.redisConfig = {
      createRedisClient: () => {
        const driver = createDriverClient()
        driver.ping = async () => 'PONG'
        drivers.push(driver)
        return driver
      }
    }

    return { redis, drivers }
  }

  const flush = () => new Promise((resolve) => setImmediate(resolve))

  // The facade has steps to run BEFORE the driver is told (subscribers,
  // dedicated connections). A connect() landing during those found a manager
  // that knew nothing of the teardown and "reused" the client about to be
  // quit — connected, with nothing behind it a moment later.
  test('a connect() during disconnect() waits for the whole shutdown, then starts fresh', async () => {
    const clock = createManualClock()
    const { redis, drivers } = createFacade({ clock })

    await redis.connect()
    const dying = drivers[0]
    dying.quit = () => { dying.calls.push('quit'); return new Promise(() => {}) } // pin the teardown

    const closing = redis.disconnect()
    const reconnect = redis.connect()

    let reconnected = false
    reconnect.then(() => { reconnected = true })

    // Let the shutdown reach the driver (and arm its deadline) before the
    // clock moves — a timer registered after the clock advanced would be due
    // in a future no advance() here ever reaches.
    await flush()

    await clock.advance(1999)
    assert.equal(reconnected, false, 'connect() must wait out the shutdown it collided with')
    assert.equal(drivers.length, 1, 'and must not build a client while the old one is being quit')

    await clock.advance(1)
    await closing
    await reconnect

    assert.equal(drivers.length, 2, 'a fresh cycle starts once the shutdown finished')
    assert.equal(redis.client, drivers[1], 'and the caller gets the LIVE client')
    assert.equal(redis.isConnected, true)
  })

  test('concurrent disconnect() calls share one shutdown', async () => {
    const { redis, drivers } = createFacade()

    await redis.connect()

    await Promise.all([redis.disconnect(), redis.disconnect(), redis.disconnect()])

    assert.equal(drivers[0].calls.filter((c) => c === 'quit').length, 1, 'one teardown, however many callers')
    assert.equal(redis.client, null)
  })
})

describe('connection manager — listener hygiene', () => {
  // #settled arms a once('ready') and a once('end') to resolve connect() on
  // the client's own lifecycle; whichever does not fire is detached. Left in
  // place they would pile up on the driver and fire against the next cycle's
  // state. Exactly one listener per event survives: the manager's own.
  test('connect() leaves exactly the manager\'s own listeners on the client', async () => {
    const { manager, created } = createManager()

    await manager.connect()

    assert.equal(created[0].listenerCount('ready'), 1)
    assert.equal(created[0].listenerCount('end'), 1)
    assert.equal(created[0].listenerCount('close'), 1)
  })
})

describe('connection manager — teardown without a flushQueue', () => {
  // flushQueue is a driver INTERNAL (TypeScript-private); the teardown uses it
  // best-effort and must not depend on it being there.
  test('a reconnecting client with no flushQueue is still released cleanly', async () => {
    const { manager, created, events } = createManager()

    await manager.connect()
    const client = created[0]
    client.status = 'reconnecting'
    client.emit('close')
    client.emit('reconnecting', 2000)
    assert.equal(typeof client.flushQueue, 'undefined', 'the fake models a driver without it')

    await manager.disconnect()

    assert.equal(manager.client, null)
    assert.equal(events.at(-1)[0], 'end')
  })
})

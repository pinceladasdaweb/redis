import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'
import { createClock } from '../src/utils/clock.js'
import { createManualClock } from './helpers/manual-clock.js'

// Node 22's runner cancels a test whose only pending work is an unref'd timer,
// and clock.sleep() is unref'd by design. A ref'd heartbeat keeps the loop
// alive for this file so the runner has something to wait on.
let heartbeat

before(() => { heartbeat = setInterval(() => {}, 1000) })
after(() => { clearInterval(heartbeat) })

describe('real clock', () => {
  test('reads the wall clock', () => {
    const clock = createClock()
    const before = Date.now()
    const now = clock.now()

    assert.ok(now >= before && now <= Date.now())
  })

  test('schedules and cancels a timeout', async () => {
    const clock = createClock()
    let fired = false

    const handle = clock.setTimeout(() => { fired = true }, 1)
    clock.clearTimeout(handle)

    await clock.sleep(20)
    assert.equal(fired, false, 'a cleared timeout must never run')

    clock.setTimeout(() => { fired = true }, 1)
    await clock.sleep(20)
    assert.equal(fired, true, 'an armed timeout must run')
  })

  test('schedules and cancels a repeating interval', async () => {
    const clock = createClock()
    let ticks = 0

    const handle = clock.setInterval(() => { ticks++ }, 2)
    await clock.sleep(30)
    clock.clearInterval(handle)

    const settled = ticks
    assert.ok(settled >= 2, `the interval must repeat, got ${settled} ticks`)

    await clock.sleep(30)
    assert.equal(ticks, settled, 'a cleared interval must stop')
  })

  test('sleep resolves after the requested delay', async () => {
    const clock = createClock()
    const before = Date.now()

    await clock.sleep(20)

    assert.ok(Date.now() - before >= 15, 'sleep must actually wait')
  })
})

// The manual clock is what every timing assertion in this suite stands on: a
// lenient fake would approve broken production code.
describe('manual clock', () => {
  test('starts away from zero so offsets cannot masquerade as absolutes', () => {
    assert.ok(createManualClock().now() > 0)
    assert.equal(createManualClock(5000).now(), 5000)
  })

  test('advance fires everything due, in chronological order', async () => {
    const clock = createManualClock()
    const fired = []

    clock.setTimeout(() => fired.push('second'), 200)
    clock.setTimeout(() => fired.push('first'), 100)
    clock.setTimeout(() => fired.push('never'), 500)

    await clock.advance(200)

    assert.deepEqual(fired, ['first', 'second'])
    assert.equal(clock.now(), 1200, 'time lands exactly where it was told to')
  })

  test('advance stops at the boundary, not one millisecond past it', async () => {
    const clock = createManualClock()
    let fired = false

    clock.setTimeout(() => { fired = true }, 100)

    await clock.advance(99)
    assert.equal(fired, false)

    await clock.advance(1)
    assert.equal(fired, true)
  })

  test('jump moves time without firing anything', async () => {
    const clock = createManualClock()
    let fired = false

    clock.setTimeout(() => { fired = true }, 100)
    clock.jump(1000)

    assert.equal(fired, false, 'jump models work that arrived before its timer')
    assert.equal(clock.now(), 2000)
    assert.equal(clock.pending(), 1, 'the timer is still armed')
  })

  test('intervals repeat until cleared', async () => {
    const clock = createManualClock()
    let ticks = 0

    const handle = clock.setInterval(() => { ticks++ }, 100)

    await clock.advance(350)
    assert.equal(ticks, 3)

    clock.clearInterval(handle)
    await clock.advance(1000)
    assert.equal(ticks, 3)
    assert.equal(clock.pending(), 0)
  })

  test('records every requested delay for assertions', async () => {
    const clock = createManualClock()

    clock.setTimeout(() => {}, 10)
    clock.sleep(20)
    clock.setInterval(() => {}, 30)

    assert.deepEqual(clock.delays(), [10, 20, 30])
  })

  test('a timer scheduled from a callback still fires in the same advance', async () => {
    const clock = createManualClock()
    const fired = []

    clock.setTimeout(() => {
      fired.push('outer')
      clock.setTimeout(() => fired.push('inner'), 50)
    }, 100)

    await clock.advance(200)

    assert.deepEqual(fired, ['outer', 'inner'])
  })

  test('sleep resolves when its delay comes due', async () => {
    const clock = createManualClock()
    let woke = false

    clock.sleep(100).then(() => { woke = true })

    await clock.advance(99)
    assert.equal(woke, false)

    await clock.advance(1)
    assert.equal(woke, true)
  })
})

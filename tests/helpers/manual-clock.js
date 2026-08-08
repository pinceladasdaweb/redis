// Deterministic stand-in for the clock seam.
//
//   advance(ms) — moves time forward AND fires everything that comes due,
//                 in chronological order, letting callbacks schedule more work
//   jump(ms)    — moves time forward WITHOUT firing anything, which models the
//                 race where the work landed before its timer got a chance
//
// Time starts away from zero on purpose: with an origin of 0, `now - start`,
// `now + start` and a hardcoded 0 are indistinguishable in an assertion.

const flush = () => new Promise((resolve) => setImmediate(resolve))

const createManualClock = (start = 1000) => {
  let current = start
  let sequence = 0

  const timers = new Map()
  // Every duration ever requested, in order — enough to assert a backoff
  // range without having to control Math.random.
  const delays = []

  const schedule = (callback, ms, repeat) => {
    const id = ++sequence

    delays.push(ms)
    timers.set(id, { at: current + ms, callback, repeat })

    // Deliberately a bare handle: real timers expose unref(), manual ones do
    // not, which is exactly what the `unref?.()` call sites must tolerate.
    return { id }
  }

  const cancel = (handle) => {
    if (handle && timers.has(handle.id)) {
      timers.delete(handle.id)
    }
  }

  const nextDue = (deadline) => {
    let due = null

    for (const [id, timer] of timers) {
      if (timer.at <= deadline && (due === null || timer.at < due.timer.at)) {
        due = { id, timer }
      }
    }

    return due
  }

  return {
    now: () => current,
    setTimeout: (callback, ms) => schedule(callback, ms, null),
    clearTimeout: cancel,
    setInterval: (callback, ms) => schedule(callback, ms, ms),
    clearInterval: cancel,
    sleep: (ms) => new Promise((resolve) => schedule(resolve, ms, null)),

    async advance (ms) {
      const deadline = current + ms

      for (let due = nextDue(deadline); due; due = nextDue(deadline)) {
        current = due.timer.at

        if (due.timer.repeat === null) {
          timers.delete(due.id)
        } else {
          due.timer.at = current + due.timer.repeat
        }

        due.timer.callback()

        // Let whatever the callback started settle before the next timer.
        await flush()
      }

      current = deadline
      await flush()
    },

    jump (ms) {
      current += ms
    },

    // Introspection for assertions.
    delays: () => [...delays],
    pending: () => timers.size
  }
}

export { createManualClock, flush }
export default createManualClock

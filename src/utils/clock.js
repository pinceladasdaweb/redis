// The one seam through which every reading of "now" and every timer passes.
// Nothing in src/ may call Date.now/setTimeout/setInterval directly: tests
// drive time through a manual clock instead of sleeping, which is what makes
// timeout and backoff boundaries provable rather than approximated.
//
// One clock per facade, handed to every collaborator that schedules anything.

const createClock = () => ({
  now: () => Date.now(),

  setTimeout: (callback, ms) => setTimeout(callback, ms),
  clearTimeout: (handle) => clearTimeout(handle),

  setInterval: (callback, ms) => setInterval(callback, ms),
  clearInterval: (handle) => clearInterval(handle),

  // A delay between retries is not a reason to keep a process alive: whatever
  // is retrying holds a live connection, which keeps the loop running anyway.
  sleep: (ms) => new Promise((resolve) => {
    const handle = setTimeout(resolve, ms)

    handle.unref?.()
  })
})

export { createClock }
export default createClock

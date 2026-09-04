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

  // Ref'd on purpose. This used to unref on the premise that "whatever is
  // retrying holds a live connection, which keeps the loop running anyway" —
  // but the connection can END during the backoff (server gone, retries
  // exhausted, a concurrent disconnect()), and then nothing held the loop:
  // Node exited 0 mid-await, the caller's rejection, finally blocks and log
  // flushes never ran, and a job runner reported success. Someone awaiting a
  // retry delay IS pending work.
  sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms))
})

export { createClock }
export default createClock

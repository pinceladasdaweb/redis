// The whole mutation policy of this project, in one file.
//
// Rule: ZERO tool annotations in src/. Nothing under src/ may mention Stryker,
// because a survivor is a statement about the TESTS, and paying for it with
// noise in the production source gets the cost backwards.
//
// What this ignores is text that the library explicitly documents as
// non-contract:
//
//   - Strings handed to logger.error/warn/info/debug. "A lib não é dona do
//     logging": consumers read events and error codes, never log lines.
//   - The MESSAGE argument of a thrown error. src/utils/errors.js states it
//     outright: "consumers branch on `code` (and `operation`), never on
//     message text — messages change, codes are contract". So argument 0 is
//     ignored and arguments 1 and 2 (operation, code) are NOT.
//
// Everything else stays killable on purpose. In particular this does NOT
// ignore string literals that are ARGUMENTS to real behavior — `nodes('master')`,
// `assertReady('subscribe')`, the option names in the cluster split, the
// notify-keyspace-events class map. Those look like "just strings" and are
// nothing of the sort: mutate them and the library misbehaves in silence,
// which is exactly what the gate is for.
//
// THE TRAP (playbook §5.3): an ignore covers the node AND ALL ITS CHILDREN.
// Ignoring a whole call expression would also silence killable mutants sitting
// inside its arguments. This file is narrow on purpose — it matches only the
// literal node, and refuses even that when a template literal interpolates
// anything a mutator could touch. Concrete case in this codebase:
//
//   logger.info(`Redis client is reconnecting${typeof delay === 'number' ? ` in ${delay}ms` : ''}...`)
//
// That conditional IS killable (tests/connection.test.js proves the delay-less
// branch). A naive matcher would suppress it along with the message text.

import { declareClassPlugin, PluginKind } from '@stryker-mutator/api/plugin'

const LOG_LEVELS = new Set(['error', 'warn', 'info', 'debug'])
const ERROR_CONSTRUCTORS = new Set(['RedisClientError', 'Error'])

const LOG_MESSAGE = 'Log line text: the library documents events and error codes as the contract, never log output.'
const ERROR_MESSAGE = 'Error message text: src/utils/errors.js documents `code` and `operation` as the contract, not the message.'

// `foo.bar` / `foo?.bar`, in both the plain and optional flavors.
const memberProperty = (node) =>
  (node?.type === 'MemberExpression' || node?.type === 'OptionalMemberExpression') &&
  node.property?.type === 'Identifier'
    ? node.property.name
    : null

const isLoggerObject = (node) => {
  if (node?.type === 'Identifier') return node.name === 'logger'

  return memberProperty(node) === 'logger'
}

// logger.warn(...), this.logger.warn(...), this.logger.debug?.(...)
const isLoggerCall = (node) => {
  if (node?.type !== 'CallExpression' && node?.type !== 'OptionalCallExpression') return false

  const level = memberProperty(node.callee)

  return level !== null && LOG_LEVELS.has(level) && isLoggerObject(node.callee.object)
}

// new RedisClientError(message, operation, code) / new Error(message)
const isErrorMessageArgument = (node, parent) => {
  if (parent?.type !== 'NewExpression' || parent.callee?.type !== 'Identifier') return false
  if (!ERROR_CONSTRUCTORS.has(parent.callee.name)) return false

  // Argument 0 only. `operation` and `code` are contract and must stay killable.
  return parent.arguments[0] === node
}

// A template literal is safe to ignore only when nothing inside its `${}`
// could have been mutated on its own. Identifiers and plain member chains
// have no mutators; anything else (a conditional, a call, an optional chain)
// might, and suppressing it here would be the §5.3 trap.
const isInertExpression = (node) => {
  if (node.type === 'Identifier' || node.type === 'ThisExpression') return true

  if (node.type === 'MemberExpression' && !node.computed) {
    return isInertExpression(node.object)
  }

  return false
}

const isTextOnly = (node) => {
  if (node.type === 'StringLiteral') return true

  return node.type === 'TemplateLiteral' && node.expressions.every(isInertExpression)
}

export class MessageTextIgnorer {
  shouldIgnore (path) {
    const node = path.node

    if (!isTextOnly(node)) return undefined

    const parent = path.parentPath?.node

    if (isLoggerCall(parent) && parent.arguments.includes(node)) {
      return LOG_MESSAGE
    }

    if (isErrorMessageArgument(node, parent)) {
      return ERROR_MESSAGE
    }

    return undefined
  }
}

export const strykerPlugins = [
  declareClassPlugin(PluginKind.Ignore, 'message-text', MessageTextIgnorer)
]

// ---------------------------------------------------------------------------
// LEDGER OF ACCEPTED SURVIVORS
// ---------------------------------------------------------------------------
//
// A survivor is a claim that the tests do not pin something down. Most of the
// time the honest answer is to write the test. These are the ones where the
// answer is "the mutant does not change behavior", each with the reason —
// because "equivalent" asserted without proof is just a nicer word for
// ignoring the gate.
//
// A survivor NOT on this list is an open debt, not an accepted cost.
//
// --- Truly equivalent -------------------------------------------------------
//
// src/utils/clock.js:22   `handle.unref?.()` -> `handle.unref()`
//   The optional call can never be needed HERE: this is the real clock, whose
//   setTimeout always returns a Node Timeout, which always has unref(). The
//   guard exists for the seam's sake — a ManualClock hands back a bare handle
//   — but a ManualClock brings its own sleep() and never reaches this line.
//   Killing it would mean testing the real clock with a fake timer, which is
//   a contradiction.
//
// src/connection/config.js:26   `'nodes'` inside CLUSTER_LEVEL_OPTIONS
//   Dead weight covered by a second defense: the split loop already does
//   `if (key === 'nodes') continue` before consulting the set. Defense-in-depth
//   PAIR (playbook §5.4) — each member makes the other's mutant survive.
//   Documented rather than deleted: "nunca remover defesa por dedução".
//
// --- Repaired downstream ----------------------------------------------------
//
// src/connection/manager.js:93,111,182   `if (this.#client === client)`
// src/messaging/pubsub.js:81,152         `if (this.#subscriber === subscriber)`
//   Ownership fences. Removing one only matters when TWO generations of a
//   connection are alive at the same instant and the older one settles last;
//   in every other ordering the assignment it guards is idempotent. Killing
//   these needs a test that keeps generation N-1 in flight while N is already
//   installed — worth writing the day this file grows a third generation, not
//   before.
//
// src/connection/manager.js:49   `if (this.#connectPromise === attempt)`
//   Same shape, and the closest to killable: tests/connection.test.js already
//   drives connect → disconnect → connect. It survives because the abandoned
//   attempt settles AFTER the replacement finished, so clearing the slot twice
//   is a no-op. A test where attempt N+1 is still pending would kill it.
//
// --- Message text the matcher above deliberately does not reach -------------
//
// src/connection/manager.js, src/messaging/pubsub.js   `operation: 'quit'`
//   The `operation` handed to withDeadline — which since 17/08 becomes the
//   structured `operation` of an OPERATION_TIMEOUT RedisClientError. At THESE
//   two call sites that error never reaches a consumer: manager logs its
//   message and forces the socket closed; pubsub's release swallows it into a
//   disconnect(). Where the timeout DOES surface (the keyspace CONFIG probe),
//   the operation IS asserted and its mutant killed. Widening the matcher to
//   "any property named operation" would swallow the contract field
//   everywhere, so these two stay as documented survivors.
//
// --- Added with the shutdown-lifecycle fixes (17/08/2026) -------------------
//
// The blocking-pool trio — index.js #lease `if (reuse)`, #return's
// `status === 'ready'` and `length < MAX` — is a FAKE LIMITATION, not an
// equivalence: the wire recorder's duplicate() returns the same object, so a
// pooled and a fresh connection are indistinguishable at the unit level. The
// behavior itself is pinned end-to-end by the integration test 'consecutive
// blocking reads reuse one connection', which counts real sockets via CLIENT
// LIST. Killing these at unit level needs a recorder that mints distinct
// duplicates — noted as the next faithful-fake upgrade, not papered over.
//
// `logger.debug?.()` call sites the recorder cannot drive (index.js dedicated
// 'error' listener — the recorder's on() is a no-op; the cache-fallback debug;
// scanner's skip line; manager's reuse line): the guard itself is proven by
// 'a logger without debug() is not a crash' on representative sites; these
// residual ones fire only inside paths a faithful driver fake cannot reach.
//
// index.js `...(typeof lock === 'object' ? lock : {})` -> `true` branch:
// spreading a boolean is a JS no-op, so for `lock: true` the mutant IS the
// original; the object path is pinned by the options-merge test. Spec
// equivalence (§5.4's "spec de JS" class).
//
// index.js #getOrSet catch: the two `err?.` optional chains (an error caught
// from an await is never nullish here — only Errors are thrown on that path)
// and the `code !== …` operand-to-false variant, which would misroute only an
// error that carries lockName `cache:<key>` WITHOUT the LOCK_NOT_ACQUIRED
// code — unconstructible: LockManager is the only writer of lockName and
// always pairs it with that code.
//
// zadd's isMemberMap guard chain (index.js): each surviving mutant shifts
// WHICH guard rejects an input the contract never admits (an object plus
// trailing args, a null single argument) — the downstream throw or the
// driver's own error repairs it. Repair-downstream class.
//
// pubsub.js `if (handler)` pair (subscribe/subscribeEverywhere): the ->true
// variant stores `undefined` as a handler, and #dispatch drops falsy handlers
// on delivery — set-then-ignore, a no-op. Same class for #restore's no-op
// halves (restoring an identical previous, deleting an absent key).
//
// pubsub.js #watchTopology identity guard ->false / empty-block: without the
// early return the watcher detaches and re-attaches on the SAME client —
// idempotent (listener count unchanged, same handler). Repaired by the
// re-arm being idempotent.
//
// pubsub.js resync-tick `assertReady('subscribe')` string: the operation name
// of a probe whose failure the tick swallows by design — unobservable.
//
// pubsub.js close() `if (#nodeResync)` ->true and lock.js `if (watchdog)`
// ->true: clearing a null timer handle is a no-op in both the real clock and
// the manual one. Guard-of-noop class.
//
// manager.js 'reconnecting' log template (three mutants) and lock.js's
// "after ${retries + 1} attempt(s)" (two): message text whose template
// interpolates something mutable, which the matcher above deliberately
// refuses to ignore (§5.3 trap — swallowing the node would swallow killable
// siblings). Killable only by asserting log text, which this library declares
// non-contract. Same class: index.js's ` on cluster node ${node}` ternary arm.
//
// ---------------------------------------------------------------------------
// Everything else still surviving is an open debt. Run
// `npm run test:mutation && npm run mutation:summary` and compare.
//
// State when this policy landed (08/08/2026): 1378 mutants, 51 ignored,
// 64 survivors, score 95.17% — from 135 survivors and 90.20% before it. The
// jump was NOT the ignores: of the 71 survivors that went away, 34 were
// ignored as message text and 37 were killed by tests the gate demanded —
// the notify-keyspace-events class map, the cluster option split, an injected
// logger without debug(), and a fake that had been answering nodes('master')
// for any role at all.
//
// After the shutdown-lifecycle fixes (17/08/2026): the new code initially
// left 17 net-new survivors; a dedicated pass killed the killable ones with
// 20 targeted tests — including three found only by reading the INSTRUMENTED
// file (§5.5: the report blamed xread's ternary at one line while the live
// mutant sat on xreadgroup's, and two wantsRange operands survived because
// the partial-range cases covered every PAIR but no single field alone). The
// remainder is documented above, each with its proof.

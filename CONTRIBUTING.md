# Contributing

Thanks for taking the time. This library guards a few invariants that are easy
to break by accident, so this document is mostly about those — not about
formatting, which the tooling handles for you.

## Getting set up

```bash
npm install
npm run hooks        # once per clone: enables lint and commit-message hooks
docker compose up -d # a pinned redis:7.4-alpine for the integration suite
```

Node.js >= 22 is required.

## The checks

```bash
npm test              # unit tests, no server needed (< 1s)
npm run test:coverage # the same, with a coverage report over src/
npm run test:integration  # everything, against the real server
npm run examples      # all 17 examples against the real server
npm run check:types   # tsc --strict over index.d.ts
npm run test:mutation # the mutation gate (see below)
```

CI runs lint, types, unit, integration and the examples on Node 22 and 24.

## Invariants worth knowing before you change code

**Reconnection belongs to the driver.** ioredis owns the retry loop; this
library keeps one client per `connect()`..`disconnect()` cycle and only tracks
state. If you find yourself writing a reconnection loop, the bug is elsewhere.

**A command that cannot run must fail loudly.** Nothing may resolve to `null`
or a silent no-op because the connection is down — commands reject with
`RedisClientError` carrying a stable `code`. Codes are contract; message text
is not, so never branch on a message and never assert one in a test.

**Options are validated before the connection is touched.** Bad arguments
reject locally with `INVALID_ARGUMENT` rather than reaching the server and
coming back as a protocol error.

**Zero is a legitimate value.** Use `??`, never `||`, for defaults:
`maxRetryAttempts: 0` means *no retries*, `block: 0` means *block forever*.

**`keyPrefix` applies everywhere.** The driver prefixes keys by argument
position, which silently misses commands whose key follows a subcommand
(`XGROUP`, `XINFO`) and patterns (`SCAN MATCH`, `SORT BY/GET`). If you add a
command in that shape, prefix it yourself and cover it with a wire test.

**Time goes through the clock seam.** No `Date.now`, `setTimeout` or
`setInterval` in `src/` — take them from the injected clock, so tests can drive
time instead of sleeping.

**A timer that someone awaits must be able to fire.** `unref()` only belongs on
timers nothing waits for (the lock watchdog); an awaited timer must be cleared
on the happy path instead.

## Tests

Every change needs a test that fails without it. Beyond that:

- **New public method?** Add a case to `tests/wire.test.js`. A guard test fails
  the build when a public method has no wire contract, so you will hear about
  it either way.
- **Timing behavior?** Use the `ManualClock` from `tests/helpers/` and assert
  exact boundaries (`advance(n - 1)` still waiting, `advance(1)` fires). Real
  sleeps in the clock domain are not accepted.
- **Fixed a bug?** Name the test after the behavior and mark it with a
  `// Regression:` comment explaining what used to happen.
- **Mutation gate.** `npm run test:mutation` grades whether the tests actually
  assert. A surviving mutant is either a missing assertion or an equivalent
  mutant — if it is the latter, say why in the pull request.
- **Node 22 first.** The test runner cancels tests whose only pending work is
  an unref'd timer. Run `npx node@22 --test 'tests/**/*.test.js'` before
  pushing anything that adds tests; CI is not the place to discover it.

The integration suite is **destructive** — it kills connections server-side
with `CLIENT KILL` and flushes keys. Never point it at a shared Redis, and
never run it in parallel with the examples against the same server.

## Commits and pull requests

Conventional commits, enforced by commitlint (header up to 100 characters):

```
feat: add support for X
fix: stop dropping Y when Z
docs: ...  test: ...  chore: ...  ci: ...  refactor: ...
```

Pull requests target `development`, never `main`. A merge into `main` publishes
to npm, and the version bump is derived from the merge commit subject: include
`minor` for a feature, `BREAKING CHANGE` for a break, and anything else ships
as a patch.

Please do not add `Co-Authored-By` trailers.

## Reporting a bug

Include the library version, the Node version, and the smallest snippet that
reproduces it. If it involves reconnection or a failover, describe how the
connection was interrupted — that detail is usually the whole bug.

## What changed

<!-- What behavior is different after this, and why. -->

## How it was verified

<!-- Which checks you ran locally. CI runs lint, types, unit, integration and
     the examples on Node 22 and 24. -->

- [ ] `npm test` (and `npm run test:integration` if the behavior touches the server)
- [ ] `npx node@22 --test 'tests/**/*.test.js'` — only needed when adding tests
- [ ] `npm run test:mutation` — surviving mutants explained below, if any
- [ ] `npm run examples` — only needed when the public API changed

## Checklist

- [ ] A test fails without this change
- [ ] New public methods have a case in `tests/wire.test.js`
- [ ] Timing behavior is asserted through the `ManualClock`, not by sleeping
- [ ] Errors carry a stable `code`; no test asserts message text
- [ ] Defaults use `??` so `0` survives
- [ ] README and `index.d.ts` updated if the public surface moved

<!-- Merging into main publishes to npm. The version bump comes from the merge
     commit subject: include `minor` for a feature, `BREAKING CHANGE` for a
     break, anything else ships as a patch. -->

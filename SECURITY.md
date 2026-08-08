# Security Policy

## Supported versions

The latest published minor is supported. Fixes ship forward — there are no
backport branches for older lines.

| Version | Supported |
| --- | --- |
| 1.1.x | ✅ |
| < 1.1 | ❌ |

## Reporting a vulnerability

Please report privately through
[GitHub Security Advisories](https://github.com/pinceladasdaweb/redis/security/advisories/new)
rather than opening a public issue.

Include the affected version, what an attacker can achieve, and a reproduction
if you have one. You can expect an acknowledgement within a few days; once a
fix is released, credit goes in the release notes unless you prefer otherwise.

## What this library does and does not touch

Useful context when judging whether something is a vulnerability here or in
your application:

- **Credentials** are passed to ioredis and never logged. The only connection
  detail that reaches a log line is the host.
- **TLS** is not configured by this library. Connections are plain TCP unless
  the driver is configured otherwise; if you need TLS today, use the raw
  ioredis instance exposed as `client`.
- **Values are never evaluated.** Data read from Redis is returned as-is, and
  the only parsing is `JSON.parse` inside the explicit `*Json` helpers — so a
  malformed payload throws a `SyntaxError` instead of producing something
  half-decoded.
- **The Lua scripts** used by the locking API are static: they take the lock
  key and holder token as `KEYS`/`ARGV` and never interpolate user input into
  the script body.
- **Keys are not escaped.** A `keyPrefix` and a key are concatenated verbatim,
  so a key built from untrusted input can address another key. Validate keys
  the same way you would validate a path.
- **`deleteByPattern` is destructive by design** and requires an explicit
  pattern. Passing user input to it is equivalent to handing out a delete.
- **Runtime dependencies:** exactly one, `ioredis`. The published package
  declares no lifecycle scripts, and releases are published from CI with
  provenance (SLSA v1) — you can verify a release with `npm audit signatures`.

// Validates the artifact, not the repo: packs the tarball, installs it in a
// clean project and imports it through both module systems. Catches what no
// unit test can — a file missing from `files`, a broken `exports` condition,
// a stray dependency, a lifecycle script that would nag every consumer.

import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readdirSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const run = (command, args, cwd) =>
  execFileSync(command, args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })

const repo = process.cwd()
const sandbox = mkdtempSync(join(tmpdir(), 'redis-package-'))

try {
  console.log('• packing the tarball')
  const tarball = run('npm', ['pack', '--pack-destination', sandbox], repo).trim().split('\n').pop()

  console.log(`• installing ${tarball} in a clean project`)
  run('npm', ['init', '-y'], sandbox)
  const install = run('npm', ['install', join(sandbox, tarball)], sandbox)

  assert.ok(
    !/allow-scripts/.test(install),
    'the package must install without lifecycle-script approval prompts'
  )

  const installed = readdirSync(join(sandbox, 'node_modules')).filter((entry) => !entry.startsWith('.'))
  const unexpected = installed.filter((entry) => !['@ioredis', '@pinceladasdaweb', 'ioredis', 'cluster-key-slot', 'debug', 'denque', 'ms', 'redis-errors', 'redis-parser', 'standard-as-callback'].includes(entry))

  assert.deepEqual(unexpected, [], `unexpected packages in the consumer tree: ${unexpected.join(', ')}`)

  console.log('• importing through ESM')
  writeFileSync(join(sandbox, 'esm.mjs'), `
    import RedisClient, { RedisClientError, createLogger } from '@pinceladasdaweb/redis'
    import assert from 'node:assert/strict'

    assert.equal(typeof RedisClient, 'function')
    assert.equal(typeof RedisClientError, 'function')
    assert.equal(typeof createLogger, 'function')
    assert.equal(typeof new RedisClient({ logger: createLogger('error') }).zadd, 'function')
    console.log('  ESM ok')
  `)
  console.log(run('node', ['esm.mjs'], sandbox).trim())

  console.log('• importing through CommonJS')
  writeFileSync(join(sandbox, 'cjs.cjs'), `
    const { RedisClient, RedisClientError, createLogger } = require('@pinceladasdaweb/redis')
    const assert = require('node:assert/strict')

    assert.equal(typeof RedisClient, 'function')
    assert.equal(typeof RedisClientError, 'function')
    assert.equal(typeof createLogger, 'function')
    assert.equal(typeof new RedisClient({ logger: createLogger('error') }).zadd, 'function')
    console.log('  CJS ok')
  `)
  console.log(run('node', ['cjs.cjs'], sandbox).trim())

  console.log('• type declarations resolve for both module systems')
  run('npm', ['install', '--save-dev', 'typescript', '@types/node'], sandbox)
  const usage = `
    import RedisClient, { ScoredMember } from '@pinceladasdaweb/redis'

    const redis = new RedisClient({ host: 'localhost', port: 6379 })
    export const top = (): Promise<ScoredMember[]> =>
      redis.zrevrange('board', 0, 9, { withScores: true })
  `
  writeFileSync(join(sandbox, 'usage.mts'), usage)
  writeFileSync(join(sandbox, 'usage.cts'), usage)
  run('npx', ['tsc', '--noEmit', '--strict', '--module', 'node16', '--moduleResolution', 'node16', '--target', 'es2022', 'usage.mts', 'usage.cts'], sandbox)
  console.log('  types ok for .mts and .cts consumers')

  console.log('\n✅ package verified end to end')
} finally {
  rmSync(sandbox, { recursive: true, force: true })
}

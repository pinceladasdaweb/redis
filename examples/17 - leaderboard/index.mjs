// Sorted sets: rankings that stay ordered as scores change.

import assert from 'node:assert/strict'
import RedisClient from '../../src/index.js'
import { baseConfig, quietLogger, heading, done } from '../config.mjs'

heading('17 — Leaderboards with sorted sets')

const redis = new RedisClient({ ...baseConfig, logger: quietLogger, keyPrefix: 'example:leaderboard:' })
await redis.connect()
await redis.deleteByPattern('*')

const BOARD = 'weekly'

// Members are unique and always kept in score order by the server.
await redis.zadd(BOARD, { ada: 120, alan: 95, grace: 180, edsger: 95 })
console.log(`  zcard               → ${await redis.zcard(BOARD)} players`)

// Scores update atomically — no read-modify-write race between processes.
const adaScore = await redis.zincrby(BOARD, 45, 'ada')
console.log(`  ada +45             → ${adaScore} (a number, not a string)`)

// Top 3, highest first. WITHSCORES returns { member, score } pairs instead of
// the flat array Redis actually sends.
//
// Note alan and edsger are tied at 95: Redis breaks ties lexicographically,
// and reverse order flips that too — so edsger ranks ahead of alan here.
const top = await redis.zrevrange(BOARD, 0, 2, { withScores: true })
console.log('  top 3:')
top.forEach(({ member, score }, index) => console.log(`    ${index + 1}. ${member.padEnd(7)} ${score}`))

// A player's own standing. Ranks are zero-based, so add one for display.
const rank = await redis.zrevrank(BOARD, 'alan')
console.log(`  alan's position     → #${rank + 1} with ${await redis.zscore(BOARD, 'alan')} points (tied, ranked after edsger)`)

// Missing members are null — never NaN, never an error.
console.log(`  unknown player      → score ${await redis.zscore(BOARD, 'nobody')}, rank ${await redis.zrevrank(BOARD, 'nobody')}`)

// Range queries by score, for tiers and pagination.
console.log(`  100+ points         → ${(await redis.zrangebyscore(BOARD, 100, '+inf')).join(', ')}`)
console.log(`  players in 90..130  → ${await redis.zcount(BOARD, 90, 130)}`)

const page = await redis.zrange(BOARD, '+inf', '-inf', {
  byScore: true,
  rev: true,
  limit: { offset: 1, count: 2 },
  withScores: true
})
console.log(`  page 2 (2 per page) → ${page.map(({ member }) => member).join(', ')}`)

// Infinite scores pin a member to the top forever — and come back as
// Infinity, which is exactly why scores are parsed instead of returned raw.
await redis.zadd(BOARD, { champion: '+inf' })
console.log(`  pinned champion     → score ${await redis.zscore(BOARD, 'champion')}`)
await redis.zrem(BOARD, 'champion')

// Sorted sets also make a natural priority queue: pop the lowest score first.
await redis.zadd('queue', { 'job:urgent': 1, 'job:normal': 5, 'job:later': 10 })
const next = await redis.zpopmin('queue')
console.log(`  next job by priority → ${next.member} (priority ${next.score})`)

// Trimming keeps a leaderboard bounded: drop everyone below the top 3.
const dropped = await redis.zremrangebyrank(BOARD, 0, -4)
console.log(`  trimmed to top 3    → removed ${dropped}, ${await redis.zcard(BOARD)} remain`)

assert.equal(adaScore, 165)
assert.deepEqual(top.map(({ member }) => member), ['grace', 'ada', 'edsger'], 'ties break lexicographically')
assert.equal(await redis.zscore(BOARD, 'nobody'), null)
assert.equal(next.member, 'job:urgent')
assert.equal(await redis.zcard(BOARD), 3)

await redis.deleteByPattern('*')
await redis.disconnect()
done('Ranking maintained, paginated, pinned and trimmed — scores as real numbers')

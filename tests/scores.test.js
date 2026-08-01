import assert from 'node:assert/strict'
import { describe, test } from 'node:test'
import { parseScore, parseScoredMembers } from '../src/utils/scores.js'

describe('score parsing', () => {
  test('turns Redis score strings into numbers', () => {
    assert.equal(parseScore('42'), 42)
    assert.equal(parseScore('3.5'), 3.5)
    assert.equal(parseScore('-7'), -7)
    assert.equal(parseScore('0'), 0)
  })

  // Number('inf') is NaN — the whole reason this helper exists.
  test('understands the infinities Redis reports', () => {
    assert.equal(parseScore('inf'), Number.POSITIVE_INFINITY)
    assert.equal(parseScore('+inf'), Number.POSITIVE_INFINITY)
    assert.equal(parseScore('-inf'), Number.NEGATIVE_INFINITY)
  })

  test('keeps a missing score as null instead of NaN', () => {
    assert.equal(parseScore(null), null)
    assert.equal(parseScore(undefined), null)
  })

  test('pairs flat WITHSCORES replies into members and scores', () => {
    assert.deepEqual(parseScoredMembers(['ada', '100', 'alan', '90.5']), [
      { member: 'ada', score: 100 },
      { member: 'alan', score: 90.5 }
    ])
  })

  test('handles empty and non-array replies', () => {
    assert.deepEqual(parseScoredMembers([]), [])
    assert.deepEqual(parseScoredMembers(null), [])
  })

  test('parses infinite scores inside a ranking', () => {
    assert.deepEqual(parseScoredMembers(['pinned', 'inf']), [{ member: 'pinned', score: Number.POSITIVE_INFINITY }])
  })
})

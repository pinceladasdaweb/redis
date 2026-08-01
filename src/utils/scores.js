// Redis reports sorted-set scores as strings, infinities included — and
// Number('inf') is NaN, which turns into a silent corruption the first time a
// leaderboard uses an infinite score. Parsing lives here so every sorted-set
// method returns real numbers.

const parseScore = (value) => {
  if (value === null || value === undefined) {
    return null
  }

  if (value === 'inf' || value === '+inf') {
    return Number.POSITIVE_INFINITY
  }

  if (value === '-inf') {
    return Number.NEGATIVE_INFINITY
  }

  return Number(value)
}

// WITHSCORES replies are flat: [member, score, member, score, ...]. Pairing
// them up is the difference between a usable ranking and an index-arithmetic
// bug in every caller.
const parseScoredMembers = (reply) => {
  if (!Array.isArray(reply)) {
    return []
  }

  const entries = []

  for (let index = 0; index < reply.length; index += 2) {
    entries.push({ member: reply[index], score: parseScore(reply[index + 1]) })
  }

  return entries
}

export { parseScore, parseScoredMembers }
export default parseScore

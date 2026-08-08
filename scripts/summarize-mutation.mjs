import { readFileSync } from 'node:fs'

const report = JSON.parse(readFileSync(process.argv[2] ?? 'reports/mutation/mutation.json', 'utf8'))
const totals = {}
const rows = []

for (const [file, { mutants }] of Object.entries(report.files)) {
  const by = {}
  for (const m of mutants) by[m.status] = (by[m.status] ?? 0) + 1
  for (const [k, v] of Object.entries(by)) totals[k] = (totals[k] ?? 0) + v
  rows.push([file.replace(/^.*\/src\//, 'src/'), by.Survived ?? 0, by.Timeout ?? 0, by.Killed ?? 0, mutants.length])
}

rows.sort((a, b) => b[1] - a[1])
console.log('arquivo'.padEnd(28), 'Surv', 'Time', 'Kill', 'Total')
for (const [f, s, t, k, n] of rows) console.log(f.padEnd(28), String(s).padStart(4), String(t).padStart(4), String(k).padStart(4), String(n).padStart(5))

const killed = (totals.Killed ?? 0) + (totals.Timeout ?? 0)
const valid = killed + (totals.Survived ?? 0)
console.log('\nTOTAIS:', JSON.stringify(totals))
console.log('score:', (killed / valid * 100).toFixed(2) + '%')

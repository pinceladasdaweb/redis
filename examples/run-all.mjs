// Runs every example against a real Redis and fails if any of them stops
// doing real work. Examples assert their own outcomes, so a green run here
// means the documented behavior still holds — exit codes alone would not.

import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { readdir } from 'node:fs/promises'
import { spawn } from 'node:child_process'

const here = dirname(fileURLToPath(import.meta.url))

const run = (name) => new Promise((resolve) => {
  const child = spawn(process.execPath, [join(here, name, 'index.mjs')], { encoding: 'utf8' })
  let output = ''

  child.stdout.on('data', (chunk) => { output += chunk })
  child.stderr.on('data', (chunk) => { output += chunk })
  child.on('close', (code) => resolve({ name, code, output }))
})

const examples = (await readdir(here, { withFileTypes: true }))
  .filter((entry) => entry.isDirectory())
  .map((entry) => entry.name)
  .sort((a, b) => Number.parseInt(a, 10) - Number.parseInt(b, 10))

console.log(`Running ${examples.length} examples against ${process.env.REDIS_HOST || '127.0.0.1'}:${process.env.REDIS_PORT || 6379}\n`)

const failures = []

for (const name of examples) {
  const { code, output } = await run(name)
  // A silent success is not a success: every example must reach its own
  // assertions and print the summary line.
  const completed = output.includes('✅')

  if (code === 0 && completed) {
    console.log(`  ✔ ${name}`)
  } else {
    failures.push({ name, code, output })
    console.log(`  ✖ ${name} (exit ${code}${completed ? '' : ', no summary line'})`)
  }
}

if (failures.length > 0) {
  console.log('\nFailures:\n')

  for (const { name, output } of failures) {
    console.log(`── ${name} ${'─'.repeat(Math.max(0, 60 - name.length))}`)
    console.log(output.trimEnd())
    console.log()
  }

  process.exitCode = 1
} else {
  console.log(`\n✅ All ${examples.length} examples completed successfully\n`)
}

import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { isDeepStrictEqual } from 'node:util'

import type { JournalEntry, KillPoint, KillRunOptions } from './kill-switch.js'
import type { ReferenceModel } from './reference-model.js'
import { PIPE_ID } from './s1.js'
import { type S1Store, type Simulator, follow, readStore } from './s1-harness.js'

const KILL_RUN = fileURLToPath(new URL('./kill-run.ts', import.meta.url))
const PACKAGE_ROOT = fileURLToPath(new URL('..', import.meta.url))
const TIMEOUT_MS = 15_000

type Exit = { code: number | null; signal: NodeJS.Signals | null; output: string }

/**
 * Runs S1 in a child process that kills itself with SIGKILL at `point`, and returns the store as
 * the kill left it.
 *
 * The oracle recovers from the store first, as the child does, then follows every batch the child
 * journaled before it died. The run must die by SIGKILL with the kill as the journal's last line:
 * a run that ends, fails or dies elsewhere fails here, so a matrix row whose point stops firing
 * cannot pass.
 */
export async function runKilled({
  simulator,
  dir,
  model,
  point,
}: {
  simulator: Simulator
  dir: string
  model: ReferenceModel
  point: KillPoint
}): Promise<S1Store> {
  const before = await readStore(dir)
  model.recover(before.state)

  const scratch = await mkdtemp(path.join(tmpdir(), 'sqd-ct2-journal-'))
  const journal = path.join(scratch, 'journal.ndjson')

  try {
    const state = `_sqd_parquet_state.${PIPE_ID}.json`
    const exit = await spawnRun({ url: simulator.chain.url, dir, journal, state, point })
    const entries = await readJournal(journal)

    requireKilled(point, exit, entries)

    for (const entry of entries) {
      if ('batch' in entry) {
        follow(model, simulator, entry.batch)
      }
    }
  } finally {
    await rm(scratch, { recursive: true, force: true })
  }

  return readStore(dir)
}

function spawnRun(options: KillRunOptions): Promise<Exit> {
  return new Promise((resolve, reject) => {
    // The child loads TypeScript itself; options meant for the test process are not its own.
    const { NODE_OPTIONS, NODE_V8_COVERAGE, ...env } = process.env
    const child = spawn(process.execPath, ['--import', 'tsx', KILL_RUN, JSON.stringify(options)], {
      cwd: PACKAGE_ROOT,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })

    let output = ''
    const collect = (chunk: string) => {
      output += chunk
    }
    child.stdout.setEncoding('utf8').on('data', collect)
    child.stderr.setEncoding('utf8').on('data', collect)

    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`the run passed ${TIMEOUT_MS} ms without reaching '${options.point.name}':\n${output}`))
    }, TIMEOUT_MS)

    child.on('error', (error) => {
      clearTimeout(timer)
      reject(error)
    })
    child.on('close', (code, signal) => {
      clearTimeout(timer)
      resolve({ code, signal, output })
    })
  })
}

async function readJournal(journal: string): Promise<JournalEntry[]> {
  let text: string
  try {
    text = await readFile(journal, 'utf8')
  } catch (error) {
    // A run that dies before its first line leaves no journal.
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return []
    }

    throw error
  }

  const lines = text.split('\n').filter((line) => line.length > 0)

  return lines.map((line) => JSON.parse(line) as JournalEntry)
}

function requireKilled(point: KillPoint, exit: Exit, entries: JournalEntry[]): void {
  if (exit.signal !== 'SIGKILL') {
    const ending = exit.code === 0 ? 'ended' : `failed (exit ${exit.code}, signal ${exit.signal})`

    throw new Error(`the run ${ending} before the kill point '${point.name}' fired:\n${exit.output}`)
  }

  const killedHere = isDeepStrictEqual(entries.at(-1), { killed: point.name })
  if (!killedHere) {
    throw new Error(`the run died by SIGKILL, but not at '${point.name}':\n${exit.output}`)
  }
}

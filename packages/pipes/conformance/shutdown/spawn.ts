import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { it as base, vi } from 'vitest'

import { readJournal } from '../kill/spawn.js'

const PACKAGE_ROOT = fileURLToPath(new URL('../..', import.meta.url))

export type Exit = { code: number | null; signal: NodeJS.Signals | null; output: string }

export type ChildRun<Entry> = {
  /** Sends a real signal to the child. */
  kill(signal: NodeJS.Signals): void
  /** Waits until a journal line passes `found`, and returns it. */
  entry(found: (entry: Entry) => boolean): Promise<Entry>
  journal(): Promise<Entry[]>
  exit: Promise<Exit>
}

export type StartChild = <Entry>(script: string, options: Record<string, unknown>) => Promise<ChildRun<Entry>>

/** Runs scripts in child processes. A child still alive when the test ends is killed. */
export const startChildFixture = async ({}: object, use: (startChild: StartChild) => Promise<void>) => {
  const kills: (() => void)[] = []
  const scratch = await mkdtemp(path.join(tmpdir(), 'sqd-shutdown-'))
  let started = 0

  const startChild: StartChild = async <Entry>(
    script: string,
    options: Record<string, unknown>,
  ): Promise<ChildRun<Entry>> => {
    const journal = path.join(scratch, `journal-${started++}.ndjson`)

    // The child loads TypeScript itself; options meant for the test process are not its own.
    const { NODE_OPTIONS, NODE_V8_COVERAGE, ...env } = process.env
    const child = spawn(process.execPath, ['--import', 'tsx', script, JSON.stringify({ ...options, journal })], {
      cwd: PACKAGE_ROOT,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    kills.push(() => child.kill('SIGKILL'))

    let output = ''
    const collect = (chunk: string) => {
      output += chunk
    }
    child.stdout.setEncoding('utf8').on('data', collect)
    child.stderr.setEncoding('utf8').on('data', collect)

    const exit = new Promise<Exit>((resolve, reject) => {
      child.on('error', reject)
      child.on('close', (code, signal) => resolve({ code, signal, output }))
    })

    const read = () => readJournal<Entry>(journal)

    return {
      exit,
      journal: read,
      kill: (signal: NodeJS.Signals) => child.kill(signal),
      entry: (found: (entry: Entry) => boolean) =>
        vi.waitFor(
          async () => {
            const entry = (await read()).find(found)
            if (entry === undefined) {
              throw new Error(`no such journal line yet. The child said:\n${output}`)
            }

            return entry
          },
          { timeout: 15_000, interval: 20 },
        ),
    }
  }

  await use(startChild)

  for (const kill of kills) {
    kill()
  }

  await rm(scratch, { recursive: true, force: true })
}

export const withChild = base.extend<{ startChild: StartChild }>({
  startChild: startChildFixture,
})

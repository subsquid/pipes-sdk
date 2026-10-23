import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const PACKAGE_ROOT = fileURLToPath(new URL('../..', import.meta.url))
const TIMEOUT_MS = 15_000

type Exit = { code: number | null; signal: NodeJS.Signals | null; output: string }

/**
 * Runs `script` in a child process that is to kill itself at `point`, and returns its journal. The
 * child gets `options`, the point and the journal path as JSON in its one argument.
 *
 * The run must die by SIGKILL with the kill as the journal's last line: a run that ends, fails or
 * dies elsewhere fails here, so a matrix row whose point stops firing cannot pass.
 */
export async function killChild<Entry extends object>({
  script,
  point,
  options,
  diagnose,
}: {
  script: string
  point: { name: string }
  options: Record<string, unknown>
  /** On a timeout, what the child's surroundings show while it still hangs: the store's view of it. */
  diagnose?: () => Promise<string>
}): Promise<Entry[]> {
  const scratch = await mkdtemp(path.join(tmpdir(), 'sqd-ct2-journal-'))
  const journal = path.join(scratch, 'journal.ndjson')

  try {
    const exit = await spawnRun(script, point.name, { ...options, point, journal }, diagnose)
    const entries = await readJournal<Entry>(journal)

    requireKilled(point, exit, entries)

    return entries
  } finally {
    await rm(scratch, { recursive: true, force: true })
  }
}

function spawnRun(
  script: string,
  pointName: string,
  options: Record<string, unknown>,
  diagnose?: () => Promise<string>,
): Promise<Exit> {
  return new Promise((resolve, reject) => {
    // The child loads TypeScript itself; options meant for the test process are not its own.
    const { NODE_OPTIONS, NODE_V8_COVERAGE, ...env } = process.env
    const child = spawn(process.execPath, ['--import', 'tsx', script, JSON.stringify(options)], {
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

    // Diagnosed before the kill, while whatever holds the child still holds it.
    const timer = setTimeout(async () => {
      const diagnosis = diagnose === undefined ? '' : await diagnose().catch((error) => `no diagnosis: ${error}`)
      child.kill('SIGKILL')
      reject(new Error(`the run passed ${TIMEOUT_MS} ms without reaching '${pointName}':\n${output}\n${diagnosis}`))
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

async function readJournal<Entry>(journal: string): Promise<Entry[]> {
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

  return lines.map((line) => JSON.parse(line) as Entry)
}

function requireKilled(point: { name: string }, exit: Exit, entries: object[]): void {
  if (exit.signal !== 'SIGKILL') {
    const ending = exit.code === 0 ? 'ended' : `failed (exit ${exit.code}, signal ${exit.signal})`

    throw new Error(`the run ${ending} before the kill point '${point.name}' fired:\n${exit.output}`)
  }

  const last = entries.at(-1)
  const killedHere = last !== undefined && 'killed' in last && last.killed === point.name
  if (!killedHere) {
    throw new Error(`the run died by SIGKILL, but not at '${point.name}':\n${exit.output}`)
  }
}

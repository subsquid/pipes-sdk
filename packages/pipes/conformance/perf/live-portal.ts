import { type ChildProcess, fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'

import type { ChainProfile, FromPortal, PortalReport, ToPortal } from './protocol.js'

const SCRIPT = fileURLToPath(new URL('./portal-process.ts', import.meta.url))
const PACKAGE_ROOT = fileURLToPath(new URL('../..', import.meta.url))

/** The live chain's portal, running in a child process. */
export class LivePortal {
  readonly url: string
  readonly #child: ChildProcess

  private constructor(child: ChildProcess, url: string) {
    this.#child = child
    this.url = url
  }

  static async open(profile: ChainProfile): Promise<LivePortal> {
    const { NODE_OPTIONS, NODE_V8_COVERAGE, ...env } = process.env
    const child = fork(SCRIPT, [JSON.stringify(profile)], {
      cwd: PACKAGE_ROOT,
      env,
      execArgv: ['--import', 'tsx'],
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    })

    const ready = await next(child, 'ready')

    return new LivePortal(child, ready.url)
  }

  /** Starts the chain; resolves once the last block is produced. */
  async run(): Promise<void> {
    const finished = next(this.#child, 'finished')
    this.#send({ type: 'start' })
    await finished
  }

  async report(): Promise<PortalReport> {
    const reply = next(this.#child, 'report')
    this.#send({ type: 'report' })

    return (await reply).report
  }

  close() {
    if (this.#child.connected) this.#child.disconnect()
  }

  #send(message: ToPortal) {
    this.#child.send(message)
  }
}

function next<K extends FromPortal['type']>(child: ChildProcess, type: K): Promise<Extract<FromPortal, { type: K }>> {
  return new Promise((resolve, reject) => {
    const onMessage = (message: FromPortal) => {
      if (message.type !== type) return
      cleanup()
      resolve(message as Extract<FromPortal, { type: K }>)
    }
    const onExit = (code: number | null) => {
      cleanup()
      reject(new Error(`portal process exited with ${code} before "${type}"`))
    }
    const cleanup = () => {
      child.off('message', onMessage)
      child.off('exit', onExit)
    }

    child.on('message', onMessage)
    child.on('exit', onExit)
  })
}

import fs from 'node:fs'
import fsp from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import path from 'node:path'

import type { Observation } from './s1.js'

/** A step of the commit protocol, named by the file operation the kill lands on (FM-30, FM-31). */
export type KillPoint =
  | {
      /** The matrix row, written to the journal when the kill lands. */
      name: string
      /** A `node:fs/promises` call. A rename is matched by its destination. */
      op: 'rename' | 'writeFile' | 'unlink'
      /** Matches the operation's path, relative to the store, `/`-separated. */
      path: string
      /** The matching operation the kill lands on, counting from 1. */
      at: number
      /** Before the operation starts, or once it has completed. */
      when: 'before' | 'after'
      /**
       * Counts only a unit whose window starts at or below the committed cursor: a sparse table's
       * unit that stretches back over checkpoints it sat out, which straddles the cursor (CN-12).
       */
      straddling?: true
    }
  | {
      name: string
      /** A stream write. The kill lands before the file's second write: its header is on disk, its rows are not. */
      op: 'write'
      path: string
      /** The matching file, counting from 1 in the order the run opens them. */
      at: number
    }

/** One line of a killed run's journal: a batch the pipe delivered, or the kill itself. */
export type JournalEntry = { batch: Observation } | { killed: string }

/** What the parent hands the child, as JSON in its one argument. */
export type KillRunOptions = {
  url: string
  dir: string
  journal: string
  /** The pipe's state record, relative to the store. */
  state: string
  point: KillPoint
}

/** IB-22: a published unit is named for its window, `<from>-<to>.parquet`. */
const UNIT_WINDOW = /(\d+)-(\d+)\.parquet$/

/** Decides, call by call, whether a file operation is the kill point. It never kills by itself. */
export class KillSwitch {
  readonly #point: KillPoint
  readonly #pattern: RegExp
  readonly #committed: () => number | undefined
  #matched = 0
  /** The open files a `write` point watches, by descriptor. */
  readonly #files = new Map<number, { ordinal: number; writes: number }>()

  /** `committed` reads the cursor the store holds now, for a `straddling` point. */
  constructor(point: KillPoint, committed: () => number | undefined) {
    this.#point = point
    this.#pattern = new RegExp(point.path)
    this.#committed = committed
  }

  /** For a promise call: whether the kill lands before it, after it, or not at all. */
  call(op: 'rename' | 'writeFile' | 'unlink', target: string): 'before' | 'after' | undefined {
    const point = this.#point
    if (point.op !== op || !this.#pattern.test(target)) {
      return undefined
    }

    if (point.straddling && !this.#straddles(target)) {
      return undefined
    }

    this.#matched += 1

    return this.#matched === point.at ? point.when : undefined
  }

  opened(fd: number, target: string): void {
    if (this.#point.op !== 'write' || !this.#pattern.test(target)) {
      return
    }

    this.#matched += 1
    this.#files.set(fd, { ordinal: this.#matched, writes: 0 })
  }

  closed(fd: number): void {
    this.#files.delete(fd)
  }

  /** Whether the kill lands before this write. */
  wrote(fd: number): boolean {
    const file = this.#files.get(fd)
    if (!file) {
      return false
    }

    file.writes += 1

    return file.ordinal === this.#point.at && file.writes === 2
  }

  #straddles(target: string): boolean {
    const window = UNIT_WINDOW.exec(target)
    const cursor = this.#committed()

    return window !== null && cursor !== undefined && Number(window[1]) <= cursor
  }
}

type Callback = (error: NodeJS.ErrnoException | null, fd: number) => void
type Patchable = Record<
  'rename' | 'writeFile' | 'unlink' | 'open' | 'write' | 'writev' | 'close',
  (...args: unknown[]) => unknown
>

/**
 * Wraps the file operations of this process so `kill` runs at `point`. The builtin modules'
 * exports are re-synced, so code that imported the functions by name gets the wrappers too. Only a
 * process that is about to die may arm it: nothing restores the originals.
 */
export function armKillSwitch({
  dir,
  point,
  committed,
  kill,
}: {
  dir: string
  point: KillPoint
  /** Reads the cursor the store holds now. */
  committed: () => number | undefined
  kill: () => never
}): void {
  const killSwitch = new KillSwitch(point, committed)
  const relative = (target: unknown) => path.relative(dir, String(target)).split(path.sep).join('/')
  const promises = fsp as unknown as Patchable
  const callbacks = fs as unknown as Patchable

  for (const op of ['rename', 'writeFile', 'unlink'] as const) {
    const real = promises[op]
    const targetOf = (args: unknown[]) => (op === 'rename' ? args[1] : args[0])

    promises[op] = async (...args: unknown[]) => {
      const when = killSwitch.call(op, relative(targetOf(args)))
      if (when === 'before') {
        kill()
      }

      const result = await real(...args)
      if (when === 'after') {
        kill()
      }

      return result
    }
  }

  // File streams open, write and close through the callback API, looked up on the module at call time.
  const open = callbacks.open
  callbacks.open = (target: unknown, ...rest: unknown[]) => {
    const callback = rest.pop() as Callback

    return open(target, ...rest, (error: NodeJS.ErrnoException | null, fd: number) => {
      if (!error) {
        killSwitch.opened(fd, relative(target))
      }

      callback(error, fd)
    })
  }

  for (const op of ['write', 'writev'] as const) {
    const real = callbacks[op]

    callbacks[op] = (fd: unknown, ...rest: unknown[]) => {
      if (killSwitch.wrote(fd as number)) {
        kill()
      }

      return real(fd, ...rest)
    }
  }

  const close = callbacks.close
  callbacks.close = (fd: unknown, ...rest: unknown[]) => {
    killSwitch.closed(fd as number)

    return close(fd, ...rest)
  }

  syncBuiltinESMExports()
}

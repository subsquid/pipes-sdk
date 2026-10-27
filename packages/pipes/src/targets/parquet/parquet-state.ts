import { mkdir, readFile, readdir, rename, unlink, writeFile } from 'node:fs/promises'
import path from 'node:path'

import { type BlockCursor, type Logger, type TargetState, formatBlock } from '~/core/index.js'

import { PARQUET_ERROR_CODES, ParquetTargetError } from './errors.js'
import { fsyncDir, fsyncFile } from './fs-durable.js'
import { TMP_PREFIX } from './segment.js'

/** Base name of the durable state file living at the root of the output dir. */
const STATE_BASENAME = '_sqd_parquet_state'

/** Published data files are named `<from>-<to>.parquet`; nothing else in a table dir is ours. */
const DATA_FILE_RE = /^(\d+)-(\d+)\.parquet$/

/** Any key's state file, the id-less form included. */
const STATE_FILE_RE = new RegExp(`^${STATE_BASENAME}(\\..+)?\\.json$`)

type PersistedState = {
  /** Optional pipe namespace, mirrored from `settings.id`. */
  id?: string
  /** Last durably-checkpointed cursor — `read` resumes from `cursor.number + 1`. */
  cursor: BlockCursor
  /**
   * Finalized head observed at the checkpoint, persisted so the source can re-seed its monotonic
   * finalized watermark after an unclean restart mid-fork. Absent in state written before this
   * field existed (treated as "no persisted finalized head").
   */
  finalized?: BlockCursor
  /**
   * Per-table first block the next published file will cover. Tracked per table (not derived from
   * `cursor`) because a table with no rows in a window publishes no file while the cursor moves on
   * regardless — only this record remembers how far back that table's next file must reach. Absent
   * in state written before coverage naming existed.
   */
  coverage?: Record<string, number>
}

export type ParquetStateOptions = {
  /** Output base directory — the isolation unit (one pipe per dir). */
  dir: string
  /** Declared table names; their sub-directories are created on startup. */
  tables: string[]
  /**
   * Cursor key, which names the state file. The target passes the pipe's source id unless an
   * explicit `settings.id` pins it.
   */
  id?: string
  logger: Logger
}

/**
 * Durable cursor + crash recovery for the Parquet target.
 *
 * The output directory belongs to one pipe. Recovery treats every file in it as this pipe's own,
 * so startup refuses a directory that holds another cursor key's state file (`E2321`). The cursor
 * is persisted to a single JSON file written atomically (temp + fsync + rename + dir fsync).
 *
 * Recovery on startup reconciles the on-disk files with the committed cursor: any published file
 * holding blocks above the cursor is a remnant of a checkpoint that published files but crashed
 * before persisting the new cursor, and any `.tmp-*` file is an aborted in-progress segment —
 * both are deleted so `read(cursor)` regenerates that (deterministic, finalized) data cleanly.
 */
export class ParquetState {
  readonly #baseDir: string
  readonly #tables: string[]
  readonly #id: string | undefined
  readonly #statePath: string
  readonly #logger: Logger

  #coverage: Record<string, number> | undefined

  constructor(options: ParquetStateOptions) {
    this.#baseDir = options.dir
    this.#tables = options.tables
    this.#id = options.id
    this.#logger = options.logger
    this.#statePath = path.join(
      options.dir,
      options.id ? `${STATE_BASENAME}.${options.id}.json` : `${STATE_BASENAME}.json`,
    )
  }

  /**
   * Per-table coverage starts read from the state file, or `undefined` when the state predates
   * coverage naming (or there is no state at all). Populated by {@link getCursor}.
   */
  get coverage(): Record<string, number> | undefined {
    return this.#coverage
  }

  /**
   * Prepares the output directory and returns the cursor to resume from (or `undefined` to start
   * from the stream beginning).
   *
   * Always: `mkdir -p` the base dir + every table dir, refuse a directory another key's state file
   * claims (`E2321`) before touching anything, and delete every `.tmp-*` file. When a committed
   * cursor exists, additionally delete every published data file whose coverage end exceeds it
   * (incomplete-checkpoint remnants) — refusing up front if one straddles the cursor without being
   * explained by the persisted coverage, which would mean the state file and the data files came
   * from different runs (`E2317`).
   */
  async getCursor(): Promise<TargetState | undefined> {
    await mkdir(this.#baseDir, { recursive: true })
    await this.#refuseForeignState()

    for (const table of this.#tables) {
      await mkdir(path.join(this.#baseDir, table), { recursive: true })
    }

    await this.#deleteTempFiles()

    const state = await this.#readState()
    if (!state) {
      return undefined
    }

    this.#coverage = state.coverage

    await this.#deleteFilesAboveCursor(state.cursor.number)

    // Hand the persisted finalized head back as resume state so the source can re-seed its
    // monotonic watermark (explicit `null` when no finalized head was stored).
    return { latest: state.cursor, finalized: state.finalized ?? null }
  }

  /**
   * Atomically persists the checkpoint cursor: write a temp file, fsync it, rename over the
   * state file, then fsync the directory so the rename is durable. Called only after every open
   * writer for the checkpoint has been published, so `coverage` describes the files on disk.
   */
  async saveCursor(cursor: BlockCursor, finalized?: BlockCursor, coverage?: Record<string, number>): Promise<void> {
    const payload: PersistedState = { cursor }
    if (this.#id) {
      payload.id = this.#id
    }
    if (finalized) {
      payload.finalized = finalized
    }
    if (coverage) {
      payload.coverage = coverage
    }

    const tmpPath = path.join(this.#baseDir, `${TMP_PREFIX}state-${path.basename(this.#statePath)}`)

    await writeFile(tmpPath, JSON.stringify(payload))
    await fsyncFile(tmpPath)
    await rename(tmpPath, this.#statePath)
    await fsyncDir(this.#baseDir)
  }

  async #readState(): Promise<PersistedState | undefined> {
    let raw: string
    try {
      raw = await readFile(this.#statePath, 'utf8')
    } catch {
      return undefined
    }

    try {
      const parsed = JSON.parse(raw) as PersistedState
      if (!parsed?.cursor || typeof parsed.cursor.number !== 'number') {
        throw new Error('missing cursor.number')
      }

      return parsed
    } catch (error) {
      throw new ParquetTargetError(
        PARQUET_ERROR_CODES.STATE_CORRUPT,
        `Parquet state file '${this.#statePath}' exists but could not be parsed: ` +
          `${error instanceof Error ? error.message : String(error)}. Inspect or remove it to recover.`,
      )
    }
  }

  /**
   * Data and temp files carry no cursor key, so recovery cannot tell another pipe's files from its
   * own: in a directory another key's state file claims, it would delete them (ADR-16). Two first
   * runs started together on an empty directory both pass; the next start of either refuses.
   */
  async #refuseForeignState(): Promise<void> {
    const own = path.basename(this.#statePath)
    // No fallback to an empty listing: an unreadable directory may hold another key's state.
    const entries = await readdir(this.#baseDir)
    const foreign = entries.filter((name) => STATE_FILE_RE.test(name) && name !== own).sort()

    if (foreign.length === 0) {
      return
    }

    throw new ParquetTargetError(
      PARQUET_ERROR_CODES.DIRECTORY_SHARED,
      `Parquet output directory '${this.#baseDir}' holds another pipe's state (${foreign.join(', ')}), ` +
        `but this pipe's state file is '${own}'. A directory belongs to one pipe: crash recovery ` +
        `deletes files it cannot tell apart from another pipe's. Point this pipe at a directory of its ` +
        `own. If the pipe was renamed, or its state predates pipe ids, rename the state file to '${own}' ` +
        `to resume from it.`,
    )
  }

  /** Deletes every `.tmp-*` file in the base dir and each table dir. */
  async #deleteTempFiles(): Promise<void> {
    for (const dir of [this.#baseDir, ...this.#tables.map((t) => path.join(this.#baseDir, t))]) {
      const entries = await readdir(dir).catch(() => [] as string[])
      for (const name of entries) {
        if (name.startsWith(TMP_PREFIX)) {
          await unlink(path.join(dir, name)).catch(() => {})
        }
      }
    }
  }

  /**
   * Deletes every published `<from>-<to>.parquet` whose `to` exceeds the committed cursor.
   *
   * A surviving over-cursor file is a correctness hazard, not a cosmetic leftover: `read(cursor)`
   * re-fetches the same blocks and either collides with it (fatal) or, if rotation lands on a
   * different range, overlaps it (silent duplicate rows). So a failed `unlink` here is fatal —
   * we surface it with remediation guidance rather than swallowing it and counting it as removed.
   *
   * Reading the range off the filename is sound for both naming schemes: a coverage-named file
   * ends at the checkpoint cursor that published it, and a legacy min/max-named file ends at its
   * last row, which is itself never above that cursor. Either way `to > cursor` means "published
   * after the last commit".
   *
   * An over-cursor file that also *starts* at or below the cursor (`from <= cursor < to`)
   * straddles the commit point, so on the face of it it holds committed data (blocks `<= cursor`)
   * that a resume from `cursor + 1` will never re-fetch. Two very different things produce that
   * shape, and the persisted coverage tells them apart:
   *
   * - `from` equals the table's persisted coverage start — this is the file that table was about
   *   to publish when the checkpoint was interrupted. A sparse table's coverage start legitimately
   *   sits at or below the cursor (it kept the start while it sat out earlier checkpoints), so its
   *   *stretched* remnant straddles by construction. Its rows can only have come from processing
   *   blocks above the cursor, so a resume regenerates them: delete it like any other remnant.
   * - `from` is anything else — the files were written by a run that committed further than the
   *   cursor now records (a restored older state file, or a cursor rewound by hand). Deleting it
   *   would destroy committed data, so we refuse up front (scanning every table before deleting
   *   anything) and leave the operator a decision.
   *
   * With no persisted coverage (state predating coverage naming) nothing explains a straddle: a
   * row-min/max name's `from` is its first row, so committed rows really are inside. Refuse.
   */
  async #deleteFilesAboveCursor(cursorNumber: number): Promise<void> {
    const overCursor: { path: string; from: number; to: number; table: string }[] = []

    for (const table of this.#tables) {
      const dir = path.join(this.#baseDir, table)
      const entries = await readdir(dir).catch(() => [] as string[])
      for (const name of entries) {
        const match = DATA_FILE_RE.exec(name)
        if (!match) continue

        const from = Number.parseInt(match[1], 10)
        const to = Number.parseInt(match[2], 10)
        if (to <= cursorNumber) continue

        if (from <= cursorNumber && from !== this.#coverage?.[table]) {
          // With no coverage recorded at all, the straddle can also be a pre-upgrade crash remnant
          // (a back-keyed row puts a legacy min/max name's `from` at or below the cursor) — a
          // shape the old recovery deleted and re-derived. We can't tell it apart from real state
          // divergence, but the remedy for it is one file, not the whole table.
          const legacyHint =
            this.#coverage === undefined
              ? ` If this is the first start after upgrading from a version that kept no coverage ` +
                `record and the previous run crashed, the file is an ordinary checkpoint remnant — ` +
                `deleting just this file and restarting is enough.`
              : ''

          throw new ParquetTargetError(
            PARQUET_ERROR_CODES.STATE_COVERAGE_INVALID,
            `Parquet file '${path.join(dir, name)}' covers block range ${from}-${to}, which straddles ` +
              `the committed cursor ${formatBlock(cursorNumber)} and does not start where table ` +
              `'${table}' was next due to publish from (${this.#coverage?.[table] ?? 'no coverage recorded'}): ` +
              `the data files were written by a run that committed further than the state file now ` +
              `records (a restored older state file, or a cursor rewound by hand). Refusing to delete ` +
              `it, since that would destroy committed data. Either restore the state file that matches ` +
              `the data files, or delete both the state file and '${table}/' to re-index.${legacyHint}`,
          )
        }

        overCursor.push({ path: path.join(dir, name), from, to, table })
      }
    }

    for (const file of overCursor) {
      try {
        await unlink(file.path)
      } catch (error) {
        throw new ParquetTargetError(
          PARQUET_ERROR_CODES.RECOVERY_DELETE_FAILED,
          `Crash recovery could not delete the over-cursor Parquet file '${file.path}' ` +
            `(its blocks exceed the committed cursor ${formatBlock(cursorNumber)}): ` +
            `${error instanceof Error ? error.message : String(error)}. Leaving it would duplicate or ` +
            `overlap re-fetched data — remove it manually and restart.`,
        )
      }
    }

    if (overCursor.length > 0) {
      this.#logger.warn(
        `Crash recovery: removed ${overCursor.length} Parquet file(s) above the committed cursor ` +
          `(block ${formatBlock(cursorNumber)}) before resuming.`,
      )
    }
  }
}

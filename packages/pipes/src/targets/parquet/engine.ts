import type { ParquetTable } from './schema.js'
import type { SegmentWriter } from './segment.js'

/** Per-table handle created once at target startup; creates one writer per segment file. */
export interface ParquetTableWriter {
  /**
   * Creates the writer for one segment file, writing to `tmpPath` — a target-chosen temp path
   * inside the table's output directory. Called once per file rotation. A segment may finish
   * with **zero rows appended** (tail closing claims a window the table produced nothing in),
   * so `finish()` must produce a valid schema-only Parquet file even if `append` never ran.
   */
  createSegment(tmpPath: string): SegmentWriter
}

/**
 * A pluggable segment-writer engine for `parquetTarget`.
 *
 * The target owns everything around the writer — staging, rotation triggers, coverage tracking,
 * temp-file naming, publication (fsync → collision check → atomic rename → dir fsync),
 * checkpointing, crash recovery and metrics. The source supplies finalized blocks. An engine owns
 * exactly one thing: writing those rows into a Parquet file at the temp path it is given.
 * It never names, renames, fsyncs or deletes files, and it never sees a block number or
 * coverage window — published files are named for the window the pipe processed, which only
 * the target knows — so naming, durability and recovery semantics cannot vary per engine. The
 * target also verifies the finished file's Parquet magic bytes before publishing it, so a
 * non-Parquet output fails loudly at the checkpoint instead of reaching downstream readers.
 *
 * **Encoding options belong to the engine, not the target.** Row-group size, compression and
 * any backend tuning are declared by each engine's own factory (`parquetjsEngine({...})`,
 * `duckdbEngine({...})`) and typed to what that engine can actually do — the target neither
 * consumes nor forwards them, so an engine can never be asked for an encoding it cannot
 * honor. The one capability gate left is {@link table}: the declared schema may carry
 * per-column `compression` overrides from the neutral model, and an engine that cannot honor
 * a declaration must throw there rather than silently ignore it.
 *
 * The SDK's declared schema model ({@link ParquetTable}) plus the plain-JS row contract (see
 * the `ParquetLeafType` JSDoc) is the complete input: `LIST` cells arrive as plain arrays,
 * `STRUCT` cells as plain objects, and any library-specific schema or row reshaping happens
 * inside the engine — there is no engine-specific schema mechanism at the API surface.
 */
export interface ParquetEngine {
  /** Engine name, used in logs and error messages. */
  readonly name: string
  /**
   * Called once per declared table at target construction, after the model-level schema
   * validation. Validate engine capability limits here (throw `ParquetTargetError` for
   * declarations this engine cannot honor — e.g. per-column `compression` overrides on a
   * backend with one file-level codec) and return the table's segment-writer factory.
   */
  table(table: ParquetTable): ParquetTableWriter
}

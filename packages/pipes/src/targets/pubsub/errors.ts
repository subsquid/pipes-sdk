import { PipeError, SdkErrorName } from '~/core/errors.js'

/**
 * Common error wrapper for everything thrown by the Pub/Sub target.
 *
 * All target-originated errors extend this class and carry an `E24xx` code so downstream
 * code can pattern-match on `instanceof PubsubTargetError` and react (alerting, retries,
 * etc.) without scraping `.message`. Code bands: E0xxx = source, E1xxx = fork handling,
 * E2xxx = targets (E20xx ClickHouse, E21xx Postgres, E22xx BigQuery, E23xx Parquet,
 * E24xx Pub/Sub).
 */
export class PubsubTargetError extends PipeError {
  constructor(code: string, message: string | string[]) {
    super(code, SdkErrorName.TargetConfiguration, message)
  }
}

// E24xx — Pub/Sub target codes
export const PUBSUB_ERROR_CODES = {
  /** A configured route names a topic that does not exist (`topicSetup: 'validate'`). */
  TOPIC_NOT_FOUND: 'E2401',
  /** A user attribute uses a reserved name (`_`-prefixed envelope space, or `goog…`). */
  RESERVED_ATTRIBUTE: 'E2402',
  /** The message exceeds Pub/Sub's per-message attribute count / key / value budget. */
  ATTRIBUTE_BUDGET: 'E2403',
  /** The encoded payload exceeds Pub/Sub's 10 MB message limit. */
  MESSAGE_TOO_LARGE: 'E2404',
  /** The canonical codec met a value it cannot represent (5.4's reject rows). */
  CODEC_UNSUPPORTED_VALUE: 'E2405',
  /** The canonical codec met a cycle. */
  CODEC_CYCLE: 'E2406',
  /**
   * The dataset never reports a finalized head, so no manifest can be kept and nothing
   * published could ever be compensated. Set `assumeNoForks: true` to publish anyway.
   */
  NO_FINALITY_HEAD: 'E2407',
  /**
   * A fork was reported on a pipe running with `assumeNoForks: true`. The assertion was
   * wrong and the data has already left — nothing can be retracted.
   */
  FORK_UNDER_ASSUME_NO_FORKS: 'E2408',
  /**
   * A fork-capable dataset delivered a block without a hash and the route derives ids
   * from it. Bare block numbers repeat after a fork, so the id would alias an orphaned
   * row with a canonical one — supply an explicit `MessageDraft.id`.
   */
  MISSING_BLOCK_HASH: 'E2409',
  /** Another process holds the single-writer lock on the state file. */
  STATE_LOCKED: 'E2410',
  /** The state file was written by an incompatible schema version. */
  STATE_SCHEMA_VERSION: 'E2411',
  /** A per-draft `orderingKey` was supplied under the `lww` profile, which has no keys. */
  ORDERING_KEY_NOT_SUPPORTED: 'E2412',
  /**
   * A materialized id changed its topic, ordering key or filter attributes between
   * revisions — a subscription filter would stop receiving the row mid-life.
   */
  MATERIALIZED_ID_MOVED: 'E2413',
  /** `windowTopic({ emptyWindows: 'upsert' })` declared without the required `emptyValues`. */
  MISSING_EMPTY_VALUES: 'E2414',
  /** A draft is missing the required `block`, or carries an unusable block number. */
  INVALID_DRAFT_BLOCK: 'E2415',
  /** `publishFrom: 'latest'` could not be resolved because the dataset reports no head. */
  UNRESOLVABLE_GO_LIVE: 'E2416',
  /** The configured `state` path could not be opened. */
  STATE_UNAVAILABLE: 'E2417',
  /** Two drafts of the same batch claim the same id on the same partition. */
  DUPLICATE_DRAFT_ID: 'E2418',
  /**
   * The state file belongs to a different producer: its stored cursor key is not the one
   * this pipe binds. Sequence counters, outbox and manifest are producer-wide, so adopting
   * another producer's file would hand its consumers this pipe's operations.
   */
  STATE_IDENTITY_MISMATCH: 'E2419',
  /**
   * The state file was written under the other delivery profile. The two scope `_seq`
   * differently (one producer-wide counter vs one dense counter per partition), so reusing
   * the file would re-issue sequence numbers consumers already hold.
   */
  STATE_PROFILE_MISMATCH: 'E2420',
} as const

import { DefaultPipeIdError } from './errors.js'

/**
 * The static id every target keyed its persisted cursor by before the SDK started keying cursors
 * by the pipe's source `id`. Also the key a state falls back to when `bind` never runs (e.g. unit
 * tests that drive a state class directly). Targets that support it migrate a cursor left under
 * this id by an older SDK to the pipe's own key automatically on first resume.
 */
export const LEGACY_DEFAULT_CURSOR_ID = 'stream'

/**
 * Resolves the id a target keys its persisted state by. One rule, shared by every target:
 * an explicit per-target id always wins, else the pipe's non-empty source id. The legacy default
 * is visible only before binding; a target write without either id is refused with E0001.
 *
 * `bind` is called once by the target's `write()` before any read, so getCursor, the cursor
 * writes, fork resolution and cleanup all key by the same value.
 */
export class CursorKey {
  #value: string
  readonly #explicit: boolean

  constructor(explicitId: string | undefined, defaultId: string = LEGACY_DEFAULT_CURSOR_ID) {
    this.#explicit = explicitId !== undefined
    this.#value = explicitId ?? defaultId
  }

  /** Adopt the pipe's source id, unless an explicit id was given (explicit always wins). */
  bind(sourceId: string | undefined): void {
    if (this.#explicit) return
    if (!sourceId?.trim()) throw new DefaultPipeIdError()

    this.#value = sourceId
  }

  /** The resolved id every persisted row is keyed by. */
  get value(): string {
    return this.#value
  }

  /** Whether an explicit per-target id was given (blocks source-id binding and legacy migration). */
  get isExplicit(): boolean {
    return this.#explicit
  }
}

import { Metrics } from '~/core/metrics-server.js'
import { BlockStreamClient } from '~/portal-client/client.js'

import { Logger } from './logger.js'
import { BatchContext } from './portal-source.js'
import { ProfilerOptions } from './profiling.js'
import { QueryBuilder } from './query-builder.js'
import { BlockCursor, HookContext } from './types.js'

export type StartContext = {
  id: string
  state: { current?: BlockCursor; initial: number }
  logger: Logger
  metrics: Metrics
  portal: BlockStreamClient
}
export type StopContext = { logger: Logger }
type TransformerFn<In, Out> = (data: In, ctx: BatchContext) => Promise<Out> | Out

export type TransformerOptions<In, Out> = {
  profiler?: ProfilerOptions
  start?: (ctx: StartContext) => Promise<void> | void
  transform: TransformerFn<In, Out>
  /**
   * Called after a chain fork has been resolved. Receives the already-resolved safe
   * cursor; undo any internal state above it.
   */
  rollback?: (cursor: BlockCursor, ctx: HookContext) => Promise<void> | void
  stop?: (ctx: StopContext) => Promise<void> | void
}

export type TransformerArgs<In, Out> = Transformer<In, Out> | TransformerOptions<In, Out> | TransformerFn<In, Out>

export class Transformer<In, Out> {
  options: TransformerOptions<In, Out>

  constructor(options: TransformerOptions<In, Out> | TransformerFn<In, Out>) {
    if (typeof options === 'function') {
      this.options = { transform: options }
    } else {
      this.options = options
    }
  }

  children: Transformer<any, any>[] = []

  /**
   * @internal
   */
  id() {
    return this.options.profiler?.name || 'anonymous'
  }

  /**
   * @internal
   */
  setId(profilerId: string) {
    this.options.profiler = {
      name: profilerId,
      hidden: this.options.profiler?.hidden,
    }
  }

  /**
   * @internal
   */
  async start(ctx: StartContext) {
    await this.options.start?.(ctx)

    if (this.children.length === 0) return
    await Promise.all(this.children.map((t) => t.start(ctx)))
  }

  /**
   * @internal
   */
  async stop(ctx: { logger: Logger }) {
    await this.options.stop?.(ctx)

    if (this.children.length === 0) return
    await Promise.all(this.children.map((t) => t.stop(ctx)))
  }

  /**
   * @internal
   */
  async rollback(cursor: BlockCursor, ctx: HookContext) {
    await this.options.rollback?.(cursor, ctx)

    if (this.children.length === 0) return
    await Promise.all(this.children.map((t) => t.rollback(cursor, ctx)))
  }

  /**
   * @internal
   */
  async run(data: In, ctx: BatchContext): Promise<Out> {
    const span = ctx.profiler.start(this.options.profiler)

    try {
      let res = await this.options.transform(data, { ...ctx, profiler: span })
      span.data = res

      for (const child of this.children) {
        res = await child.run(res, { ...ctx, profiler: span })
      }

      return res
    } finally {
      span.end()
    }
  }

  /**
   * Chains this transformer with another one.
   *
   * Type parameters:
   * - `In` – the input type of the first transformer in the chain.
   * - `Out` – the output type of this transformer (and the input of the next).
   * - `Res` – the output type of the next transformer in the chain.
   *
   * Why `In` must be inferred from the parent transformer:
   * -----------------------------------------------------
   * The second transformer only knows how to map its *input* (`Out`) to its *output* (`Res`).
   * But in the full chain, the *ultimate* input type is determined by the first transformer.
   *
   * Example:
   *   const t1 = createTransformer<string[], number[]>({ ... })
   *   const t2 = createTransformer<number[], boolean>({ ... })
   *
   *   const piped = t1.pipe(t2)
   *   // piped has type Transformer<string[], boolean>
   *
   * Notice: `t2` does not know anything about `string[]` (the original `In`).
   * Only the parent (`t1`) knows it. Therefore, when piping, we must "lift"
   * the parent's `In` into the resulting type:
   *
   *   Transformer<In, Res> instead of Transformer<Out, Res>
   *
   * Otherwise, type information about the very first input would be lost,
   * and downstream code would see only the immediate `Out` type.
   */
  pipe<Res>(transformer: TransformerArgs<Out, Res>): Transformer<In, Res> {
    this.children.push(transformer instanceof Transformer ? transformer : new Transformer(transformer))

    return this as unknown as Transformer<In, Res>
  }
}

export function createTransformer<In, Out>(options: TransformerOptions<In, Out>) {
  return new Transformer<In, Out>(options)
}

export type SetupQueryFn<Query> = (ctx: { query: Query; logger: Logger }) => void | any | Promise<void | any>

// FIXME STREAMS write docs
export class QueryAwareTransformer<
  In = any,
  Out = any,
  Query extends QueryBuilder<any> = QueryBuilder<any>,
> extends Transformer<In, Out> {
  /**
   * @internal
   */
  setupQuery: SetupQueryFn<Query>

  constructor(setupQuery: SetupQueryFn<Query>, options: TransformerOptions<In, Out>) {
    super(options)

    this.setupQuery = setupQuery
  }

  /**
   * We need to override the return type
   */
  override pipe<Res>(transformer: TransformerArgs<Out, Res>): QueryAwareTransformer<In, Res, Query> {
    return super.pipe(transformer) as unknown as QueryAwareTransformer<In, Res, Query>
  }
}

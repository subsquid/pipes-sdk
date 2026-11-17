import {
  ANY,
  ANY_OBJECT,
  BYTES,
  NAT,
  Validator,
  array,
  constant,
  nullable,
  object,
  oneOf,
  withDefault,
} from '@subsquid/util-internal-validation'

import {
  type Hex,
  type ObjectValidatorShape,
  type PortalQuery,
  type Select,
  type Selected,
  type Selector,
  type Simplify,
  type Trues,
  project,
} from './common.js'

/** Hard fork marker of a block. Raw JSON passthrough; may be null. */
export type Hardfork = {
  version: number
  round: number
}

/** The L1 action as submitted; fields besides `type` depend on it. Raw JSON passthrough. */
export type ActionPayload = {
  type: string
  [key: string]: any
}

export type ActionSignature = {
  r: Hex
  s: Hex
  v: number
}

export type ActionStatus = 'ok' | 'err'

/** The node's reply: an object, or a plain string on some errors. Raw JSON passthrough. */
export type ActionResponse = Record<string, any> | string

export type BlockHeaderFields = {
  number: number
  hash: Hex
  parentHash: Hex
  round: number
  parentRound: number
  proposer: Hex
  /** Unix timestamp in milliseconds. */
  timestamp: number
  hardfork: Hardfork | null
}

export type ActionFields = {
  actionIndex: number
  /** Null on some rejected commands. */
  user: Hex | null
  action: ActionPayload
  signature: ActionSignature
  nonce: number | null
  vaultAddress: Hex | null
  status: ActionStatus
  response: ActionResponse
}

export type BlockHeaderFieldSelection = Selector<keyof BlockHeaderFields>
export type BlockHeader<F extends BlockHeaderFieldSelection = Trues<BlockHeaderFieldSelection>> = Select<
  BlockHeaderFields,
  F
>

export type ActionFieldSelection = Selector<keyof ActionFields>
export type Action<F extends ActionFieldSelection = Trues<ActionFieldSelection>> = Select<ActionFields, F>

export type FieldSelection = {
  block?: BlockHeaderFieldSelection
  action?: ActionFieldSelection
}

export type ActionRequest = {
  /** Values of `action.type`, such as `order` or `cancelByCloid`. */
  actionType?: string[]
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

export type OrderActionRequest = {
  /** Asset ids, not coin names. */
  containsAsset?: number[]
  /** Client order ids. */
  containsCloid?: Hex[]
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

export type CancelActionRequest = {
  /** Asset ids, not coin names. */
  containsAsset?: number[]
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

export type CancelByCloidActionRequest = {
  /** Asset ids, not coin names. */
  containsAsset?: number[]
  /** Client order ids. */
  containsCloid?: Hex[]
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

export type BatchModifyActionRequest = {
  /** Asset ids, not coin names. */
  containsAsset?: number[]
  /** Client order ids. */
  containsCloid?: Hex[]
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

export type DataRequest = {
  includeAllBlocks?: boolean
  actions?: ActionRequest[]
  orderActions?: OrderActionRequest[]
  cancelActions?: CancelActionRequest[]
  cancelByCloidActions?: CancelByCloidActionRequest[]
  batchModifyActions?: BatchModifyActionRequest[]
}

export type Query<F extends FieldSelection = FieldSelection> = Simplify<
  PortalQuery & {
    type: 'hyperliquidReplicaCmds'
    fields: F
  } & DataRequest
>

export type Block<F extends FieldSelection> = Simplify<{
  header: BlockHeader<Selected<F, 'block'>>
  actions: Action<Selected<F, 'action'>>[]
}>

export function getBlockSchema<F extends FieldSelection>(fields: F): Validator<Block<F>, unknown> {
  let header = object(project(BlockHeaderShape, fields.block))
  let action = object(project(ActionShape, fields.action))

  return object({
    header,
    actions: withDefault([], array(action)),
  }) as Validator<Block<F>, unknown>
}

const BlockHeaderShape: ObjectValidatorShape<BlockHeaderFields> = {
  number: NAT,
  hash: BYTES,
  parentHash: BYTES,
  round: NAT,
  parentRound: NAT,
  proposer: BYTES,
  timestamp: NAT,
  hardfork: nullable(ANY_OBJECT) as Validator<Hardfork | null, unknown>,
}

const ActionShape: ObjectValidatorShape<ActionFields> = {
  actionIndex: NAT,
  user: nullable(BYTES),
  action: ANY_OBJECT as Validator<ActionPayload, unknown>,
  signature: ANY_OBJECT as Validator<ActionSignature, unknown>,
  nonce: nullable(NAT),
  vaultAddress: nullable(BYTES),
  status: oneOf({
    ok: constant('ok'),
    err: constant('err'),
  }),
  response: ANY,
}

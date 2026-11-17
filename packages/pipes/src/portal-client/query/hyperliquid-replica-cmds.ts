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

/** Hard fork marker of a block; may be null. */
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

type ActionFilter = {
  user?: Hex[]
  vaultAddress?: Hex[]
  status?: ActionStatus
}

type AssetFilter = ActionFilter & {
  /** Asset ids, not coin names. */
  containsAsset?: number[]
}

type CloidFilter = AssetFilter & {
  /** Client order ids. */
  containsCloid?: Hex[]
}

export type ActionRequest = Simplify<
  ActionFilter & {
    /** Values of `action.type`, such as `order` or `cancelByCloid`. */
    actionType?: string[]
  }
>

export type OrderActionRequest = Simplify<CloidFilter>
export type CancelActionRequest = Simplify<AssetFilter>
export type CancelByCloidActionRequest = Simplify<CloidFilter>
export type BatchModifyActionRequest = Simplify<CloidFilter>

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
  hardfork: nullable(object({ version: NAT, round: NAT })),
}

const ActionShape: ObjectValidatorShape<ActionFields> = {
  actionIndex: NAT,
  user: nullable(BYTES),
  action: ANY_OBJECT as Validator<ActionPayload, unknown>,
  signature: object({ r: BYTES, s: BYTES, v: NAT }),
  nonce: nullable(NAT),
  vaultAddress: nullable(BYTES),
  status: oneOf({
    ok: constant('ok'),
    err: constant('err'),
  }),
  response: ANY,
}

import Mustache from 'mustache'

import type { Deployment } from '../../../../contract-params.js'
import { Erc20TransfersPipeTemplateParams } from '../template.config.js'

const template = `import { commonAbis, evmEventDecoder } from '@subsquid/pipes/evm'
{{#decoderGroups}}

const {{{decoderId}}} = evmEventDecoder({
  profiler: { name: '{{{profilerName}}}' }, // Optional: add a profiler to measure the performance of the transformer
  range: { from: '{{{range.from}}}'{{#range.to}}, to: '{{{range.to}}}'{{/range.to}} },
  contracts: [
    {{#addresses}}
    '{{.}}',
    {{/addresses}}
  ],
  events: {
    transfers: commonAbis.erc20.events.Transfer,
  },
}).pipe(({ transfers }) =>
  transfers.map((transfer) => ({
    blockNumber: transfer.block.number,
    txHash: transfer.rawEvent.transactionHash,
    logIndex: transfer.rawEvent.logIndex,
    timestamp: transfer.timestamp.getTime() / 1000,
    from: transfer.event.from,
    to: transfer.event.to,
    value: transfer.event.value,
    tokenAddress: transfer.contract,
  })),
)
{{/decoderGroups}}
`

interface DecoderGroup {
  decoderId: string
  profilerName: string
  range: { from: string; to?: string }
  addresses: string[]
}

/**
 * Ranges are deployment-level, but one decoder scans one range — deployments
 * sharing a range share a decoder; divergent ranges get their own (suffixed)
 * decoder rather than silently widening the shared scan.
 */
export function erc20DecoderGroups(params: Erc20TransfersPipeTemplateParams): DecoderGroup[] {
  const byRange = new Map<string, { range: Deployment['range']; addresses: string[] }>()
  for (const deployment of params.deployments) {
    const key = `${deployment.range.from} ${deployment.range.to ?? ''}`
    const group = byRange.get(key) ?? { range: deployment.range, addresses: [] }
    group.addresses.push(deployment.address)
    byRange.set(key, group)
  }

  return [...byRange.values()].map((group, i) => ({
    decoderId: i === 0 ? 'erc20Transfers' : `erc20Transfers${i + 1}`,
    profilerName: i === 0 ? 'erc20-transfers' : `erc20-transfers-${i + 1}`,
    range: group.range,
    addresses: group.addresses,
  }))
}

export function renderTransformer(params: Erc20TransfersPipeTemplateParams) {
  return Mustache.render(template, { decoderGroups: erc20DecoderGroups(params) })
}

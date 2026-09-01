import { describe, expect, it } from 'vitest'

import type { RpcObservation as BitcoinObservation, LatencySample as BitcoinSample } from '~/bitcoin/index.js'
import type { RpcObservation as EvmObservation, LatencySample as EvmSample } from '~/evm/index.js'
import type { RpcObservation as SolanaObservation, LatencySample as SolanaSample } from '~/solana/index.js'

import type { LatencySample, RpcObservation } from './rpc-latency-watcher.js'

/**
 * The watchers emit these types, so consumers must be able to name them from the same
 * entrypoint they import the watcher from. `~/{chain}/index.js` is what the package's
 * `./{chain}` export maps to — a type that stops resolving here stops resolving for them.
 */
describe('rpc latency public types', () => {
  it('re-exports LatencySample and RpcObservation from every chain entrypoint', () => {
    const sample: LatencySample = {
      number: 1,
      timestamp: new Date(),
      portal: { receivedAt: new Date() },
      rpc: [{ url: 'https://rpc.example', unresolved: 'rpc-behind' }],
    }

    const evm: EvmSample = sample
    const solana: SolanaSample = sample
    const bitcoin: BitcoinSample = sample

    const observation: RpcObservation = sample.rpc[0]
    const evmObservation: EvmObservation = observation
    const solanaObservation: SolanaObservation = observation
    const bitcoinObservation: BitcoinObservation = observation

    expect([evm, solana, bitcoin]).toHaveLength(3)
    expect([evmObservation, solanaObservation, bitcoinObservation]).toHaveLength(3)
  })
})

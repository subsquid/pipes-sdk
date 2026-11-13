import { execSync } from 'node:child_process'

import { Address, address as toSvmAddress } from '@solana/addresses'
import { RpcClient } from '@subsquid/rpc-client'
import { fetchIdl } from '@subsquid/solana-typegen/lib/util/fetch.js'
import { toCamelCase } from 'drizzle-orm/casing'

import { getEvmChainId, getNetworkFromChainId } from '~/commands/init/config/networks.js'
import { NetworkType } from '~/types/init.js'

export interface ContractMetadata {
  contractAddress: string
  contractName: string
  contractEvents: RawAbiEvent[]
}

export interface BaseProxyRes<T> {
  status: string
  message: string
  result: T
}

export interface RawAbiItem {
  name: string
  type: string
}

export type RawAbiEvent = RawAbiItem & {
  inputs: RawAbiItem[]
}

/**
 * Simplified version of the ABI only including the fields
 * we care about
 */
type RawAbi = RawAbiEvent[]

/**
 * Simplified version of instructions only including the fields
 * we care about
 */
export interface SvmInstruction {
  name: string
  description?: string
  accounts: { name: string }[]
  args: { name: string; type: string }[]
}

export interface EtherscanResponse {
  ContractName: string
  ABI: string
  Proxy: '0' | '1'
  Implementation: string
}

abstract class AbiService {
  abstract generateTypes(projectPath: string, contractAddresses: string[], chainId?: string): void

  abstract getContractData(contractAddresses: string[], chainId?: string): Promise<ContractMetadata[]>
}

export class SqdAbiService {
  // Both remote lookups are pure per (network, address), and one init run asks for the
  // same contract from several places (prompt, range prompt, typegen planning) — cache
  // the promises so each contract is fetched at most once per service instance.
  readonly #contractData = new Map<string, Promise<ContractMetadata>>()
  readonly #creationBlocks = new Map<string, Promise<string>>()

  generateTypes(networkType: NetworkType, network: string, projectPath: string, contractAddresses: string[]) {
    return this.getService(networkType).generateTypes(
      projectPath,
      contractAddresses,
      networkType === 'evm' ? getEvmChainId(network) : undefined,
    )
  }

  getContractData(networkType: NetworkType, network: string, contractAddresses: string[]): Promise<ContractMetadata[]> {
    const service = this.getService(networkType)
    const chainId = networkType === 'evm' ? getEvmChainId(network) : undefined

    return Promise.all(
      contractAddresses.map((address) => {
        // EVM addresses are case-insensitive hex; SVM addresses are case-sensitive base58.
        const normalized = networkType === 'evm' ? address.toLowerCase() : address
        const key = `${networkType}:${network}:${normalized}`
        let cached = this.#contractData.get(key)
        if (!cached) {
          cached = service.getContractData([address], chainId).then(([metadata]) => metadata!)
          // A failed fetch must not poison the cache — the user may retry with a fixed address.
          cached.catch(() => this.#contractData.delete(key))
          this.#contractData.set(key, cached)
        }

        return cached
      }),
    )
  }

  async getContractCreationBlock(network: string, address: string): Promise<string> {
    const key = `${network}:${address.toLowerCase()}`
    let cached = this.#creationBlocks.get(key)
    if (!cached) {
      cached = this.#evm.getContractCreationBlock(address, getEvmChainId(network))
      cached.catch(() => this.#creationBlocks.delete(key))
      this.#creationBlocks.set(key, cached)
    }

    return cached
  }

  readonly #evm = new EvmAbiService()
  readonly #svm = new SvmAbiService()

  private getService(networkType: NetworkType): AbiService {
    return networkType === 'evm' ? this.#evm : this.#svm
  }
}

class EvmAbiService extends AbiService {
  private static SERVICE_URL = 'https://cloud.sqd.dev/chains/api/v1/evm/abi'

  override generateTypes(projectPath: string, contractAddresses: string[], chainId?: string): void {
    const outputDir = `${projectPath}/src/contracts/`

    for (const address of contractAddresses) {
      // Pinned to 4.x: typegen 5 emits a directory per contract against evm-abi 1.x,
      // while the templates import `./contracts/<address>.js` and the project depends
      // on evm-abi 0.3.x (the version @subsquid/pipes is built against).
      const cmd = `npx --yes @subsquid/evm-typegen@^4 ${outputDir} ${address} ${chainId ? `--chain-id ${chainId}` : ''}`

      execSync(cmd, { stdio: 'ignore' })
    }
  }

  private static ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

  override getContractData(contractAddresses: string[], chainid?: string) {
    if (!chainid) throw new Error('Missing chain id')

    return Promise.all(contractAddresses.map(async (address) => this.fetchEvmContractData(address, chainid)))
  }

  /**
   * Resolves the ABI events for a (possibly proxied) contract.
   *
   * `getsourcecode` reports `Proxy: '1'` on implementations too — USDC's
   * FiatTokenV2_2 points at a linked library — and proxy shells declare their own
   * admin events (`AdminChanged`/`Upgraded`). So neither "stop at `Proxy: '0'`"
   * nor "stop at the first hop with events" is correct: we walk the whole proxy
   * chain and keep the deepest hop that actually exposes events. USDC thus lands
   * on FiatTokenV2_2's `Transfer`/`Approval` rather than the proxy shell or the
   * trailing event-less library.
   */
  private async fetchEvmContractData(address: string, chainid: string): Promise<ContractMetadata> {
    const visited = new Set<string>()
    let target = address
    let best: ContractMetadata | undefined

    for (let depth = 0; depth <= 5; depth++) {
      const key = target.toLowerCase()
      if (visited.has(key)) break
      visited.add(key)

      const { metadata, proxyTarget } = await this.fetchHop(target, chainid)
      if (metadata.contractEvents.length > 0) best = metadata
      if (!proxyTarget) return best ?? metadata

      target = proxyTarget
    }

    if (best) return best

    throw new Error(
      'Unsupported deeply nested Proxy contract: this contract has more than 5 levels to the implementation contract',
    )
  }

  /** Fetches and parses a single contract, reporting its proxy target (if any). */
  private async fetchHop(
    address: string,
    chainid: string,
  ): Promise<{ metadata: ContractMetadata; proxyTarget?: string }> {
    try {
      const params = new URLSearchParams({
        chainid,
        address,
        module: 'contract',
        action: 'getsourcecode',
      })

      const url = new URL(EvmAbiService.SERVICE_URL)
      url.search = params.toString()

      const res = await fetch(url)
      const data = (await res.json()) as BaseProxyRes<EtherscanResponse[]>
      const [contractData] = data.result

      if (!this.isContractVerified(contractData.ABI)) throw new ContractCodeNotVerifiedError(address, chainid)

      const metadata: ContractMetadata = {
        contractAddress: address,
        contractName: contractData.ContractName,
        contractEvents: this.parseEvents(JSON.parse(contractData.ABI)),
      }

      const implementation = contractData.Implementation?.trim()
      const implementationKey = implementation?.toLowerCase()
      const proxyTarget =
        contractData.Proxy === '1' &&
        implementation &&
        implementationKey !== EvmAbiService.ZERO_ADDRESS &&
        implementationKey !== address.toLowerCase()
          ? implementation
          : undefined

      return { metadata, proxyTarget }
    } catch (e) {
      // Domain errors already carry user-facing context; wrap raw fetch/parse
      // failures (network down, API error body, malformed ABI JSON) with the
      // address and network so the user knows which input to fix.
      if (e instanceof ContractCodeNotVerifiedError) throw e

      const network = getNetworkFromChainId(chainid)
      const cause = e instanceof Error ? e.message : String(e)
      throw new Error(`Failed to fetch the ABI for ${address} on ${network.name}: ${cause}`, { cause: e })
    }
  }

  async getContractCreationBlock(address: string, chainid: string): Promise<string> {
    const params = new URLSearchParams({
      chainid,
      module: 'contract',
      action: 'getcontractcreation',
      contractaddresses: address,
    })

    const url = new URL(EvmAbiService.SERVICE_URL)
    url.search = params.toString()

    const res = await fetch(url)
    const data = (await res.json()) as BaseProxyRes<[{ blockNumber: string }]>

    if (!data.result?.[0]?.blockNumber) {
      throw new Error(`Could not fetch deployment block for ${address}`)
    }

    return data.result[0].blockNumber
  }

  private isContractVerified(abiRes: string) {
    return !abiRes.includes('Contract source code not verified')
  }

  private parseEvents(abi: RawAbi): RawAbiEvent[] {
    return abi
      .filter((item): item is RawAbiEvent => item.type === 'event')
      .map((item) => ({
        ...item,
        inputs: item.inputs.map((input) => ({
          ...input,
        })),
      }))
  }
}

class SvmAbiService extends AbiService {
  private static SERVICE_URL = 'https://api.mainnet-beta.solana.com'

  override generateTypes(projectPath: string, contractAddresses: string[]): void {
    const outputDir = `${projectPath}/src/contracts/`

    for (const address of contractAddresses) {
      execSync(`npx --yes @subsquid/solana-typegen@latest ${outputDir} ${address}`, {
        stdio: 'ignore',
      })
    }
  }

  override getContractData(contractAddresses: string[]): Promise<ContractMetadata[]> {
    return Promise.all(contractAddresses.map(async (address) => this.fetchSvmContractData(address)))
  }

  private async fetchSvmContractData(address: string) {
    const checksumAddress = toSvmAddress(address)
    const idl = await this.fetchContractIdl(checksumAddress)

    return {
      contractName: toCamelCase(idl?.name || idl.metadata?.name || `contract_${idl.address}`),
      contractAddress: checksumAddress,

      contractEvents: idl.instructions.map((i: SvmInstruction) => ({
        name: toCamelCase(i.name),
        type: 'event',
        inputs: [
          ...i.args
            .map<RawAbiItem>(({ name, type }) => ({
              name: toCamelCase(name),
              type,
            }))
            .filter((a) => typeof a.type !== 'object'),
          ...i.accounts.map<RawAbiItem>(({ name }) => ({
            name: toCamelCase(name),
            type: 'publicKey',
          })),
        ],
      })),
    }
  }

  private async fetchContractIdl(address: Address): Promise<any> {
    const client = new RpcClient({ url: SvmAbiService.SERVICE_URL })
    return await fetchIdl(client, address)
  }
}

export class ContractCodeNotVerifiedError extends Error {
  constructor(contractAddress: string, chainId: string) {
    const network = getNetworkFromChainId(chainId)
    super(
      `The contract code for ${contractAddress} is not verified on ${network.name}. Does this contract exist on ${network.name}?`,
    )
  }
}

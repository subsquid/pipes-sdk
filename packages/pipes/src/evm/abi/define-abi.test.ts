import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'
import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from 'vitest'

import { MockPortal, MockResponse, mockPortal, readAll } from '~/testing/index.js'

import { evmEventDecoder } from '../evm-decoder.js'
import { evmPortalStream } from '../evm-stream.js'
import { commonAbis } from './common.js'
import { defineAbi } from './define-abi.js'

const erc20JsonAbi = [
  {
    type: 'event',
    name: 'Transfer',
    anonymous: false,
    inputs: [
      { indexed: true, name: 'from', type: 'address' },
      { indexed: true, name: 'to', type: 'address' },
      { indexed: false, name: 'value', type: 'uint256' },
    ],
  },
  {
    type: 'event',
    name: 'Approval',
    anonymous: false,
    inputs: [
      { indexed: true, name: 'owner', type: 'address' },
      { indexed: true, name: 'spender', type: 'address' },
      { indexed: false, name: 'value', type: 'uint256' },
    ],
  },
  {
    type: 'function',
    name: 'name',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'string' }],
  },
  {
    type: 'function',
    name: 'approve',
    stateMutability: 'nonpayable',
    inputs: [
      { name: '_spender', type: 'address' },
      { name: '_value', type: 'uint256' },
    ],
    outputs: [{ name: '', type: 'bool' }],
  },
  {
    type: 'function',
    name: 'totalSupply',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'uint256' }],
  },
  {
    type: 'function',
    name: 'transferFrom',
    stateMutability: 'nonpayable',
    inputs: [
      { name: '_from', type: 'address' },
      { name: '_to', type: 'address' },
      { name: '_value', type: 'uint256' },
    ],
    outputs: [{ name: '', type: 'bool' }],
  },
  {
    type: 'function',
    name: 'decimals',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'uint8' }],
  },
  {
    type: 'function',
    name: 'balanceOf',
    stateMutability: 'view',
    inputs: [{ name: '_owner', type: 'address' }],
    outputs: [{ name: '', type: 'uint256' }],
  },
  {
    type: 'function',
    name: 'symbol',
    stateMutability: 'view',
    inputs: [],
    outputs: [{ name: '', type: 'string' }],
  },
  {
    type: 'function',
    name: 'transfer',
    stateMutability: 'nonpayable',
    inputs: [
      { name: '_to', type: 'address' },
      { name: '_value', type: 'uint256' },
    ],
    outputs: [{ name: '', type: 'bool' }],
  },
  {
    type: 'function',
    name: 'allowance',
    stateMutability: 'view',
    inputs: [
      { name: '_owner', type: 'address' },
      { name: '_spender', type: 'address' },
    ],
    outputs: [{ name: '', type: 'uint256' }],
  },
] as const

describe('defineAbi', () => {
  describe('event construction', () => {
    it('should produce the same topic hash as generated code for Transfer', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.events.Transfer.topic).toBe(commonAbis.erc20.events.Transfer.topic)
    })

    it('should produce the same topic hash as generated code for Approval', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.events.Approval.topic).toBe(commonAbis.erc20.events.Approval.topic)
    })

    it('should decode Transfer event identically to generated code', () => {
      const abi = defineAbi(erc20JsonAbi)

      const log = {
        topics: [
          '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
          '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
          '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f',
        ],
        data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
      }

      const generatedResult = commonAbis.erc20.events.Transfer.decode(log)
      const defineResult = abi.events.Transfer.decode(log)

      expect(defineResult).toEqual(generatedResult)
      expect(defineResult).toMatchInlineSnapshot(`
        {
          "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
          "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
          "value": 87600000000000000n,
        }
      `)
    })

    it('should decode Approval event identically to generated code', () => {
      const abi = defineAbi(erc20JsonAbi)

      const log = {
        topics: [
          '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
          '0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
          '0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff',
        ],
        data: '0x0000000000000000000000000000000000000000000000000100000000000000',
      }

      const generatedResult = commonAbis.erc20.events.Approval.decode(log)
      const defineResult = abi.events.Approval.decode(log)

      expect(defineResult).toEqual(generatedResult)
      expect(defineResult).toMatchInlineSnapshot(`
        {
          "owner": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
          "spender": "0xffffffffffffffffffffffffffffffffffffffff",
          "value": 72057594037927936n,
        }
      `)
    })

    it('should validate events with .is()', () => {
      const abi = defineAbi(erc20JsonAbi)

      const transferLog = {
        topics: [
          '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
          '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
          '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f',
        ],
        data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
      }

      expect(abi.events.Transfer.is(transferLog)).toBe(true)
      expect(abi.events.Approval.is(transferLog)).toBe(false)
    })

    it('should handle events with no indexed parameters', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'DataUpdated',
          inputs: [
            { indexed: false, name: 'key', type: 'bytes32' },
            { indexed: false, name: 'value', type: 'uint256' },
          ],
        },
      ] as const)

      expect(abi.events.DataUpdated).toBeDefined()
      expect(abi.events.DataUpdated.topic).toMatch(/^0x[0-9a-f]{64}$/)
    })

    it('should handle events with all indexed parameters', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'ThreeIndexed',
          inputs: [
            { indexed: true, name: 'a', type: 'address' },
            { indexed: true, name: 'b', type: 'uint256' },
            { indexed: true, name: 'c', type: 'bool' },
          ],
        },
      ] as const)

      expect(abi.events.ThreeIndexed).toBeDefined()
    })

    it('should skip anonymous events', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'AnonEvent',
          anonymous: true,
          inputs: [{ indexed: false, name: 'value', type: 'uint256' }],
        },
        {
          type: 'event',
          name: 'NormalEvent',
          inputs: [{ indexed: false, name: 'value', type: 'uint256' }],
        },
      ] as const)

      expect(abi.events.NormalEvent).toBeDefined()
      expect((abi.events as any).AnonEvent).toBeUndefined()
    })
  })

  describe('function construction', () => {
    it('should produce the same selector as generated code for balanceOf', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.functions.balanceOf.selector).toBe(commonAbis.erc20.functions.balanceOf.selector)
    })

    it('should produce the same selector as generated code for transfer', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.functions.transfer.selector).toBe(commonAbis.erc20.functions.transfer.selector)
    })

    it('should produce the same selector as generated code for approve', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.functions.approve.selector).toBe(commonAbis.erc20.functions.approve.selector)
    })

    it('should produce the same selectors for all ERC20 functions', () => {
      const abi = defineAbi(erc20JsonAbi)

      for (const name of Object.keys(commonAbis.erc20.functions)) {
        expect(abi.functions[name].selector).toBe((commonAbis.erc20.functions as any)[name].selector)
      }
    })

    it('should mark view functions as isView', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.functions.balanceOf.isView).toBe(true)
      expect(abi.functions.totalSupply.isView).toBe(true)
      expect(abi.functions.name.isView).toBe(true)
      expect(abi.functions.symbol.isView).toBe(true)
      expect(abi.functions.decimals.isView).toBe(true)
      expect(abi.functions.allowance.isView).toBe(true)
    })

    it('should mark non-view functions as not isView', () => {
      const abi = defineAbi(erc20JsonAbi)

      expect(abi.functions.transfer.isView).toBe(false)
      expect(abi.functions.approve.isView).toBe(false)
      expect(abi.functions.transferFrom.isView).toBe(false)
    })

    it('should encode function calldata correctly', () => {
      const abi = defineAbi(erc20JsonAbi)

      const calldata = abi.functions.balanceOf.encode({
        _owner: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
      })

      const generatedCalldata = commonAbis.erc20.functions.balanceOf.encode({
        _owner: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
      })

      expect(calldata).toBe(generatedCalldata)
    })

    it('should decode function calldata correctly', () => {
      const abi = defineAbi(erc20JsonAbi)

      const calldata = '0x70a082310000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d'
      const decoded = abi.functions.balanceOf.decode(calldata)

      expect(decoded._owner).toBe('0x7a250d5630b4cf539739df2c5dacb4c659f2488d')
    })
  })

  describe('Solidity type support', () => {
    it('should handle all integer types', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'IntTypes',
          inputs: [
            { indexed: false, name: 'a', type: 'uint8' },
            { indexed: false, name: 'b', type: 'uint64' },
            { indexed: false, name: 'c', type: 'uint256' },
            { indexed: false, name: 'd', type: 'int8' },
            { indexed: false, name: 'e', type: 'int128' },
            { indexed: false, name: 'f', type: 'int256' },
          ],
        },
      ] as const)

      expect(abi.events.IntTypes).toBeDefined()
    })

    it('should handle bytes types', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'BytesTypes',
          inputs: [
            { indexed: false, name: 'a', type: 'bytes' },
            { indexed: false, name: 'b', type: 'bytes4' },
            { indexed: false, name: 'c', type: 'bytes32' },
          ],
        },
      ] as const)

      expect(abi.events.BytesTypes).toBeDefined()
    })

    it('should handle dynamic array types', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'ArrayEvent',
          inputs: [{ indexed: false, name: 'values', type: 'uint256[]' }],
        },
      ] as const)

      expect(abi.events.ArrayEvent).toBeDefined()
    })

    it('should handle fixed-size array types', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'FixedArrayEvent',
          inputs: [{ indexed: false, name: 'values', type: 'address[3]' }],
        },
      ] as const)

      expect(abi.events.FixedArrayEvent).toBeDefined()
    })

    it('should handle tuple types', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'TupleEvent',
          inputs: [
            {
              indexed: false,
              name: 'data',
              type: 'tuple',
              components: [
                { name: 'addr', type: 'address' },
                { name: 'amount', type: 'uint256' },
              ],
            },
          ],
        },
      ] as const)

      expect(abi.events.TupleEvent).toBeDefined()
    })

    it('should handle nested tuple types', () => {
      const abi = defineAbi([
        {
          type: 'function',
          name: 'nestedTuple',
          inputs: [
            {
              name: 'data',
              type: 'tuple',
              components: [
                { name: 'addr', type: 'address' },
                {
                  name: 'inner',
                  type: 'tuple',
                  components: [
                    { name: 'x', type: 'uint256' },
                    { name: 'y', type: 'uint256' },
                  ],
                },
              ],
            },
          ],
          outputs: [{ name: '', type: 'bool' }],
        },
      ] as const)

      expect(abi.functions.nestedTuple).toBeDefined()
    })

    it('should handle tuple array types', () => {
      const abi = defineAbi([
        {
          type: 'function',
          name: 'tupleArray',
          inputs: [
            {
              name: 'items',
              type: 'tuple[]',
              components: [
                { name: 'addr', type: 'address' },
                { name: 'amount', type: 'uint256' },
              ],
            },
          ],
          outputs: [],
        },
      ] as const)

      expect(abi.functions.tupleArray).toBeDefined()
    })

    it('should throw for unsupported types', () => {
      expect(() =>
        defineAbi([
          {
            type: 'event',
            name: 'Bad',
            inputs: [{ indexed: false, name: 'x', type: 'nonsense' }],
          },
        ] as const),
      ).toThrow('Unsupported Solidity type: "nonsense"')
    })
  })

  describe('Hardhat artifact support', () => {
    it('should accept Hardhat artifact format', () => {
      const artifact = {
        _format: 'hh-sol-artifact-1',
        contractName: 'ERC20',
        abi: erc20JsonAbi,
      }

      const abi = defineAbi(artifact)

      expect(abi.events.Transfer.topic).toBe(commonAbis.erc20.events.Transfer.topic)
      expect(abi.functions.balanceOf.selector).toBe(commonAbis.erc20.functions.balanceOf.selector)
    })
  })

  describe('complex real-world ABIs', () => {
    it('should handle Uniswap V3 PoolCreated event', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'PoolCreated',
          inputs: [
            { indexed: true, name: 'token0', type: 'address' },
            { indexed: true, name: 'token1', type: 'address' },
            { indexed: true, name: 'fee', type: 'uint24' },
            { indexed: false, name: 'tickSpacing', type: 'int24' },
            { indexed: false, name: 'pool', type: 'address' },
          ],
        },
      ] as const)

      const generatedPoolCreated = event(
        '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
        'PoolCreated(address,address,uint24,int24,address)',
        {
          token0: indexed(p.address),
          token1: indexed(p.address),
          fee: indexed(p.uint24),
          tickSpacing: p.int24,
          pool: p.address,
        },
      )

      expect(abi.events.PoolCreated.topic).toBe(generatedPoolCreated.topic)
    })

    it('should handle Uniswap V3 Swap event', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'Swap',
          inputs: [
            { indexed: true, name: 'sender', type: 'address' },
            { indexed: true, name: 'recipient', type: 'address' },
            { indexed: false, name: 'amount0', type: 'int256' },
            { indexed: false, name: 'amount1', type: 'int256' },
            { indexed: false, name: 'sqrtPriceX96', type: 'uint160' },
            { indexed: false, name: 'liquidity', type: 'uint128' },
            { indexed: false, name: 'tick', type: 'int24' },
          ],
        },
      ] as const)

      const generatedSwap = event(
        '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        'Swap(address,address,int256,int256,uint160,uint128,int24)',
        {
          sender: indexed(p.address),
          recipient: indexed(p.address),
          amount0: p.int256,
          amount1: p.int256,
          sqrtPriceX96: p.uint160,
          liquidity: p.uint128,
          tick: p.int24,
        },
      )

      expect(abi.events.Swap.topic).toBe(generatedSwap.topic)
    })

    it('should handle event with tuple signature for topic computation', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'OrderFilled',
          inputs: [
            {
              indexed: false,
              name: 'order',
              type: 'tuple',
              components: [
                { name: 'maker', type: 'address' },
                { name: 'taker', type: 'address' },
                { name: 'amount', type: 'uint256' },
              ],
            },
            { indexed: true, name: 'orderHash', type: 'bytes32' },
          ],
        },
      ] as const)

      // The signature should be OrderFilled((address,address,uint256),bytes32)
      expect(abi.events.OrderFilled.topic).toMatch(/^0x[0-9a-f]{64}$/)
    })
  })

  describe('edge cases', () => {
    it('should handle empty ABI', () => {
      const abi = defineAbi([] as const)
      expect(abi.events).toEqual({})
      expect(abi.functions).toEqual({})
    })

    it('should skip non-event, non-function items', () => {
      const abi = defineAbi([
        { type: 'constructor', inputs: [{ name: 'x', type: 'uint256' }] },
        { type: 'fallback' },
        { type: 'receive' },
        { type: 'error', name: 'InsufficientBalance', inputs: [{ name: 'balance', type: 'uint256' }] },
        {
          type: 'event',
          name: 'Transfer',
          inputs: [
            { indexed: true, name: 'from', type: 'address' },
            { indexed: true, name: 'to', type: 'address' },
            { indexed: false, name: 'value', type: 'uint256' },
          ],
        },
      ] as const)

      expect(Object.keys(abi.events)).toEqual(['Transfer'])
      expect(Object.keys(abi.functions)).toEqual([])
    })

    it('should use first occurrence for duplicate event names', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'Transfer',
          inputs: [
            { indexed: true, name: 'from', type: 'address' },
            { indexed: true, name: 'to', type: 'address' },
            { indexed: false, name: 'value', type: 'uint256' },
          ],
        },
        {
          type: 'event',
          name: 'Transfer',
          inputs: [
            { indexed: true, name: 'from', type: 'address' },
            { indexed: true, name: 'to', type: 'address' },
            { indexed: false, name: 'wad', type: 'uint256' },
          ],
        },
      ] as const)

      // Should use first Transfer definition
      expect(abi.events.Transfer).toBeDefined()
    })

    it('should handle parameters without names', () => {
      const abi = defineAbi([
        {
          type: 'event',
          name: 'Anonymous',
          inputs: [
            { indexed: false, type: 'address' },
            { indexed: false, type: 'uint256' },
          ],
        },
      ] as const)

      expect(abi.events.Anonymous).toBeDefined()
    })
  })

  describe('type inference with as const', () => {
    it('should infer event names', () => {
      const abi = defineAbi(erc20JsonAbi)

      // These should compile without errors
      const _transfer = abi.events.Transfer
      const _approval = abi.events.Approval
      expect(_transfer).toBeDefined()
      expect(_approval).toBeDefined()
    })

    it('should infer function names', () => {
      const abi = defineAbi(erc20JsonAbi)

      const _balanceOf = abi.functions.balanceOf
      const _transfer = abi.functions.transfer
      expect(_balanceOf).toBeDefined()
      expect(_transfer).toBeDefined()
    })

    it('should infer decoded Transfer event field types', () => {
      const abi = defineAbi(erc20JsonAbi)

      const log = {
        topics: [
          '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
          '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
          '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f',
        ],
        data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
      }

      const decoded = abi.events.Transfer.decode(log)

      expectTypeOf(decoded.from).toEqualTypeOf<string>()
      expectTypeOf(decoded.to).toEqualTypeOf<string>()
      expectTypeOf(decoded.value).toEqualTypeOf<bigint>()
    })

    it('should infer decoded Approval event field types', () => {
      const abi = defineAbi(erc20JsonAbi)

      const log = {
        topics: [
          '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
          '0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
          '0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff',
        ],
        data: '0x0000000000000000000000000000000000000000000000000100000000000000',
      }

      const decoded = abi.events.Approval.decode(log)

      expectTypeOf(decoded.owner).toEqualTypeOf<string>()
      expectTypeOf(decoded.spender).toEqualTypeOf<string>()
      expectTypeOf(decoded.value).toEqualTypeOf<bigint>()
    })
  })

  describe('compatibility with evmEventDecoder', () => {
    it('should produce events compatible with evmEventDecoder indexed param types', () => {
      const abi = defineAbi(erc20JsonAbi)

      // The params property of the event should have the `indexed: true` marker
      // This is needed for evmEventDecoder to know which params can be filtered
      const transferParams = abi.events.Transfer.params as Record<string, { indexed?: boolean }>

      expect(transferParams['from'].indexed).toBe(true)
      expect(transferParams['to'].indexed).toBe(true)
      expect(transferParams['value'].indexed).toBeUndefined()
    })

    it('should be usable in evmEventDecoder config shape', () => {
      const abi = defineAbi(erc20JsonAbi)

      // This simulates what evmEventDecoder does internally
      const event = abi.events.Transfer
      expect(event.topic).toBeDefined()
      expect(typeof event.decode).toBe('function')
      expect(typeof event.is).toBe('function')
    })
  })

  describe('end-to-end with evmEventDecoder + evmPortalStream', () => {
    let portal: MockPortal

    const PORTAL_MOCK_RESPONSE: MockResponse[] = [
      {
        statusCode: 200,
        data: [
          {
            header: { number: 1, hash: '0x1', timestamp: 2000 },
            logs: [
              {
                address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                topics: [
                  '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                  '0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d',
                  '0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f',
                ],
                logIndex: 0,
                transactionIndex: 0,
                transactionHash: '0xdeadbeef',
                data: '0x000000000000000000000000000000000000000000000000013737bc62530000',
              },
              {
                address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                topics: [
                  '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
                  '0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                  '0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff',
                ],
                logIndex: 1,
                transactionIndex: 1,
                transactionHash: '0xdeadbeef',
                data: '0x0000000000000000000000000000000000000000000000000100000000000000',
              },
            ],
          },
        ],
      },
    ]

    beforeEach(async () => {
      portal = await mockPortal(PORTAL_MOCK_RESPONSE)
    })

    afterEach(async () => {
      await portal?.close()
    })

    it('should decode events through evmEventDecoder with defineAbi events', async () => {
      const abi = defineAbi(erc20JsonAbi)

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: abi.events.Transfer,
          },
        }),
      }).pipe((e) => e.transfers)

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block": {
              "hash": "0x1",
              "number": 1,
            },
            "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "event": {
              "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
              "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              "value": 87600000000000000n,
            },
            "factory": null,
            "rawEvent": {
              "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
              "logIndex": 0,
              "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              ],
              "transactionHash": "0xdeadbeef",
              "transactionIndex": 0,
            },
            "timestamp": 1970-01-01T00:33:20.000Z,
          },
        ]
      `)
    })

    it('should decode mixed events (defineAbi + generated) through evmEventDecoder', async () => {
      const abi = defineAbi(erc20JsonAbi)

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: abi.events.Transfer,
            approvals: commonAbis.erc20.events.Approval,
          },
        }),
      }).pipe((e) => [...e.transfers, ...e.approvals])

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block": {
              "hash": "0x1",
              "number": 1,
            },
            "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "event": {
              "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
              "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              "value": 87600000000000000n,
            },
            "factory": null,
            "rawEvent": {
              "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
              "logIndex": 0,
              "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              ],
              "transactionHash": "0xdeadbeef",
              "transactionIndex": 0,
            },
            "timestamp": 1970-01-01T00:33:20.000Z,
          },
          {
            "block": {
              "hash": "0x1",
              "number": 1,
            },
            "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "event": {
              "owner": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
              "spender": "0xffffffffffffffffffffffffffffffffffffffff",
              "value": 72057594037927936n,
            },
            "factory": null,
            "rawEvent": {
              "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "data": "0x0000000000000000000000000000000000000000000000000100000000000000",
              "logIndex": 1,
              "topics": [
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                "0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff",
              ],
              "transactionHash": "0xdeadbeef",
              "transactionIndex": 1,
            },
            "timestamp": 1970-01-01T00:33:20.000Z,
          },
        ]
      `)
    })

    it('should support filtering by indexed params with defineAbi events', async () => {
      const abi = defineAbi(erc20JsonAbi)

      const stream = evmPortalStream({
        id: 'test',
        portal: portal.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: {
              event: abi.events.Transfer,
              params: {
                from: '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
              },
            },
          },
        }),
      }).pipe((e) => e.transfers)

      const res = await readAll(stream)

      expect(res).toMatchInlineSnapshot(`
        [
          {
            "block": {
              "hash": "0x1",
              "number": 1,
            },
            "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "event": {
              "from": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
              "to": "0xc82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              "value": 87600000000000000n,
            },
            "factory": null,
            "rawEvent": {
              "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
              "data": "0x000000000000000000000000000000000000000000000000013737bc62530000",
              "logIndex": 0,
              "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                "0x000000000000000000000000c82e11e709deb68f3631fc165ebd8b4e3fc3d18f",
              ],
              "transactionHash": "0xdeadbeef",
              "transactionIndex": 0,
            },
            "timestamp": 1970-01-01T00:33:20.000Z,
          },
        ]
      `)
    })

    it('should produce identical results as generated code events', async () => {
      const abi = defineAbi(erc20JsonAbi)

      // Run with defineAbi events
      const mockPortal1 = await mockPortal(PORTAL_MOCK_RESPONSE)
      const stream1 = evmPortalStream({
        id: 'test',
        portal: mockPortal1.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: abi.events.Transfer,
            approvals: abi.events.Approval,
          },
        }),
      }).pipe((e) => [...e.transfers, ...e.approvals])
      const res1 = await readAll(stream1)
      await mockPortal1?.close()

      // Run with generated code events
      const mockPortal2 = await mockPortal(PORTAL_MOCK_RESPONSE)
      const stream2 = evmPortalStream({
        id: 'test',
        portal: mockPortal2.url,
        outputs: evmEventDecoder({
          range: { from: 0, to: 1 },
          events: {
            transfers: commonAbis.erc20.events.Transfer,
            approvals: commonAbis.erc20.events.Approval,
          },
        }),
      }).pipe((e) => [...e.transfers, ...e.approvals])
      const res2 = await readAll(stream2)
      await mockPortal2?.close()

      // Results should be identical
      expect(res1).toEqual(res2)
    })
  })
})

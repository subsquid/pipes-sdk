import { describe, expect, it } from 'vitest'

import { shimWireBlock } from './shim.js'

/**
 * `shimWireBlock` reconciles the two enumerable trace-level differences between the normalized model
 * (`@subsquid/evm-normalization` `toJSON`) and the Portal wire schema. Mirrors the Squid
 * evm-rpc-stream shim tests.
 */

describe('shimWireBlock', () => {
  it('rewrites the selfdestruct trace tag to suicide', () => {
    const block = { traces: [{ type: 'selfdestruct', action: { refundAddress: '0xa' } }] }
    shimWireBlock(block)
    expect(block.traces[0].type).toBe('suicide')
  })

  it('renames a reward action rewardType to type', () => {
    const block = { traces: [{ type: 'reward', action: { author: '0xa', rewardType: 'block' } }] }
    shimWireBlock(block)
    expect(block.traces[0].action).toEqual({ author: '0xa', type: 'block' })
    expect('rewardType' in block.traces[0].action).toBe(false)
  })

  it('leaves create / call traces and trace-less blocks untouched', () => {
    const block = {
      traces: [
        { type: 'call', action: { to: '0xa', from: '0xb' } },
        { type: 'create', action: { from: '0xc' } },
      ],
    }
    shimWireBlock(block)
    expect(block.traces[0]).toEqual({ type: 'call', action: { to: '0xa', from: '0xb' } })
    expect(block.traces[1]).toEqual({ type: 'create', action: { from: '0xc' } })

    // no traces at all — must not throw
    expect(() => shimWireBlock({ header: {} })).not.toThrow()
    expect(() => shimWireBlock({})).not.toThrow()
  })

  it('does not throw on malformed wire JSON (non-object action) — leaves it for schema validation', () => {
    // A reward trace whose `action` is a truthy non-object must not make the `in` check throw before
    // the downstream `cast()` can report a proper validation error.
    const block = { traces: [{ type: 'reward', action: 'not-an-object' }] }
    expect(() => shimWireBlock(block)).not.toThrow()
    expect(block.traces[0].action).toBe('not-an-object') // left untouched for the schema to reject
  })
})

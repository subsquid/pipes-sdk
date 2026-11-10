import { describe, expect, it } from 'vitest'

import { FALLBACK_OPTIONS } from './fallback.js'
import { type FullStreamScenario, childSourceOf, onePortalOf, s1FullStream, sourceOf } from './full-stream.js'

type Sim = { chain: { url: string; requests: [] } }

const simulator: Sim = { chain: { url: 'http://primary.invalid', requests: [] } }
const clock = () => 0

const onePortal = s1FullStream('T') as unknown as FullStreamScenario<Sim>
const sourceList: FullStreamScenario<Sim> = {
  ...onePortal,
  source: () => ({
    source: ['http://primary.invalid', 'http://standby.invalid'],
    fallback: { ...FALLBACK_OPTIONS, detection: { ...FALLBACK_OPTIONS.detection, clock } },
  }),
}

describe('the source a run reads', () => {
  it('is the simulator portal, or the scenario source list when it has one', () => {
    expect(sourceOf(onePortal, simulator)).toEqual({ source: 'http://primary.invalid' })
    expect(sourceOf(sourceList, simulator).source).toEqual(['http://primary.invalid', 'http://standby.invalid'])
  })

  it('leaves the fallback clock behind for a child process, and keeps the other options', () => {
    const { fallback } = childSourceOf(sourceList, simulator)

    expect(fallback?.detection).not.toHaveProperty('clock')
    expect(fallback?.detection?.capabilityProbe).toBe(false)
    expect(fallback?.sourceRetries).toBe(0)
  })

  it('is refused by a store that reads one portal when the scenario gives a list', () => {
    expect(onePortalOf(onePortal, simulator)).toBe('http://primary.invalid')
    expect(() => onePortalOf(sourceList, simulator)).toThrow('runs no source-list scenario')
  })
})

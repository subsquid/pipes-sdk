import { describe, expect } from 'vitest'

import type { BlockCursor } from '~/core/index.js'

import {
  FORK_HISTORIES,
  HISTORY_QUERY_FIELDS,
  HISTORY_REQUESTS,
  HISTORY_ROLLBACKS,
  forkHistory,
} from '../scenarios/fork-histories.js'
import { it, runToEnd } from './harness.js'

describe('clickhouse · partial canonical windows (CT-1)', () => {
  for (const history of FORK_HISTORIES) {
    it(`${history}: rolls back through block 3 to block 1 and preserves the canonical rows`, {
      timeout: 300_000,
    }, async ({ simulateForkHistory, database }) => {
      const simulator = await simulateForkHistory(history)
      const rollbacks: BlockCursor[] = []

      await runToEnd({
        scenario: forkHistory('A', history),
        simulator,
        database,
        repair: 'predicate',
        onRollback: (cursor) => rollbacks.push(cursor),
      })

      for (const { query, answer } of simulator.chain.requests) {
        expect(query.type).toBe('evm')
        expect(query['fields']).toEqual(HISTORY_QUERY_FIELDS)

        if (answer?.kind === 'blocks') {
          for (let i = 1; i < answer.blocks.length; i++) {
            expect(answer.blocks[i].header.parentHash).toBe(answer.blocks[i - 1].header.hash)
          }
        }
      }

      expect(rollbacks.map(({ number, hash }) => ({ number, hash }))).toEqual(HISTORY_ROLLBACKS)
      expect(
        simulator.chain.requests.map(({ query }) => ({
          fromBlock: query.fromBlock,
          toBlock: query.toBlock,
          ...(query.parentBlockHash === undefined ? {} : { parentBlockHash: query.parentBlockHash }),
        })),
      ).toEqual(HISTORY_REQUESTS)
    })
  }
})

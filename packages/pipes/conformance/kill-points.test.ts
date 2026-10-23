import { mkdir, writeFile } from 'node:fs/promises'
import path from 'node:path'

import { ParquetSchema, ParquetWriter } from '@dsnp/parquetjs'
import { describe, expect } from 'vitest'

import { runKilled } from './kill-points.js'
import type { KillPoint } from './kill-switch.js'
import { PIPE_ID, cursorAt, s1Oracle } from './s1.js'
import { it } from './s1-harness.js'

const STATE_RENAME: KillPoint = {
  name: 'never',
  op: 'rename',
  path: String.raw`^_sqd_parquet_state\.${PIPE_ID}\.json$`,
  at: 1_000,
  when: 'before',
}

describe('runKilled', () => {
  it('refuses a run that ends before its kill point fires', async ({ simulate, dir }) => {
    const simulator = await simulate()

    const run = runKilled({ simulator, dir, model: s1Oracle(), point: STATE_RENAME })

    await expect(run).rejects.toThrow("the run ended before the kill point 'never' fired")
  })

  it('refuses a run that fails before its kill point fires', async ({ simulate, dir }) => {
    const simulator = await simulate()
    // A directory where the pipe writes its state fails its first checkpoint.
    await mkdir(path.join(dir, `.tmp-state-_sqd_parquet_state.${PIPE_ID}.json`))

    const run = runKilled({ simulator, dir, model: s1Oracle(), point: STATE_RENAME })

    await expect(run).rejects.toThrow(/the run failed \(exit 1, signal null\) before the kill point 'never' fired/)
  })

  it('refuses a run that fails before its first batch, which leaves no journal', async ({ simulate, dir }) => {
    const simulator = await simulate()
    // A unit that straddles the cursor and starts where no coverage says: recovery refuses it (E2317).
    const schema = new ParquetSchema({ blockNumber: { type: 'INT64' }, value: { type: 'UTF8' } })
    await mkdir(path.join(dir, 'transfers'))
    const writer = await ParquetWriter.openFile(
      schema,
      path.join(dir, 'transfers', '000000000000-000000000009.parquet'),
    )
    await writer.close()
    const state = { id: PIPE_ID, cursor: cursorAt(5), coverage: { transfers: 10, approvals: 6 } }
    await writeFile(path.join(dir, `_sqd_parquet_state.${PIPE_ID}.json`), JSON.stringify(state))
    const model = s1Oracle()

    const run = runKilled({ simulator, dir, model, point: STATE_RENAME })

    await expect(run).rejects.toThrow(/the run failed \(exit 1, signal null\)[^]*E2317/)
  })
})

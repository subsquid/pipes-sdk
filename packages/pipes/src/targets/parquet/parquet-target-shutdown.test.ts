import { readdir } from 'node:fs/promises'
import path from 'node:path'

import { ParquetReader } from '@dsnp/parquetjs'
import { describe, expect } from 'vitest'

import { PipelineStoppedError } from '~/core/errors.js'
import { evmPortalStream } from '~/evm/index.js'
import { withDir as it } from '~/testing/dir-fixture.js'
import { mockBlock } from '~/testing/evm/index.js'
import { blockDecoder } from '~/testing/index.js'

import type { ParquetStore } from './parquet-store.js'
import { parquetTarget } from './parquet-target.js'
import type { ParquetTable } from './schema.js'

const TABLE: ParquetTable = { table: 'blocks', schema: { blockNumber: { type: 'INT64' } } }

const chainOf = (count: number) => Array.from({ length: count }, (_, i) => mockBlock({ number: i + 1 }))

async function storedBlocks(dir: string) {
  const files = (await readdir(path.join(dir, TABLE.table))).filter((file) => file.endsWith('.parquet')).sort()
  const blocks: number[] = []

  for (const file of files) {
    const reader = await ParquetReader.openFile(path.join(dir, TABLE.table, file))
    const cursor = reader.getCursor()

    let row = (await cursor.next()) as { blockNumber: bigint } | null
    while (row) {
      blocks.push(Number(row.blockNumber))
      row = (await cursor.next()) as { blockNumber: bigint } | null
    }

    await reader.close()
  }

  return blocks
}

function pipe(url: string, dir: string, onBlock: (block: number) => void = () => {}) {
  const onData = ({ store, data }: { store: ParquetStore; data: { number: number }[] }) => {
    store.insert(
      TABLE.table,
      data.map((block) => ({ blockNumber: block.number })),
    )

    onBlock(data[0].number)
  }

  return evmPortalStream({
    id: 'parquet-stop',
    portal: { url, maxBytes: 1, finalized: true },
    logger: 'silent',
    outputs: blockDecoder({ from: 1, to: 5 }),
  }).pipeTo(parquetTarget({ dir, tables: [TABLE], onData }))
}

describe('parquetTarget — graceful shutdown', () => {
  it('publishes the batches it wrote, and a restart goes on after them with no gap or repeat', async ({
    portal,
    dir,
  }) => {
    const chain = await portal({ blocks: chainOf(5), blocksPerResponse: 1 })

    const stopAtSecondBlock = (block: number) => {
      if (block === 2) {
        process.emit('SIGTERM', 'SIGTERM')
      }
    }

    await expect(pipe(chain.url, dir, stopAtSecondBlock)).rejects.toBeInstanceOf(PipelineStoppedError)

    expect(await storedBlocks(dir)).toEqual([1, 2])

    await pipe(chain.url, dir)

    expect(await storedBlocks(dir)).toEqual([1, 2, 3, 4, 5])
  })
})

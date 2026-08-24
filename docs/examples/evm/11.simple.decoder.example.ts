import { OutputOf, Transformer, createTransformer } from '@subsquid/pipes'
import { evmQuery, evmStream } from '@subsquid/pipes/evm'

function myDecoder() {
  return evmQuery()
    .addFields({
      block: { timestamp: true },
      log: { address: true, transactionHash: true },
    })
    .build()
    .pipe((data) => {
      return data.map((d) => ({
        block_number: d.header.timestamp,
        d,
      }))
    })
}

type MyDecoderOut = OutputOf<typeof myDecoder>

function myTransformation(): (data: MyDecoderOut) => { block_number_renamed: number }[] {
  return (data) =>
    data.map((i) => ({
      block_number_renamed: i.block_number,
    }))
}

type MyTransformationOut = OutputOf<typeof myTransformation> // { block_number_renamed: number }[]

const outputs = {
  v1: myDecoder(),
  v2: myDecoder().pipe(myTransformation()),
}

type FullOutputs = OutputOf<typeof outputs> // { v1: { block_number: number }[], v2: { block_number_renamed: number }[] }

function wholePipeTransform(): Transformer<FullOutputs, FullOutputs> {
  return createTransformer({
    transform: (data) => data,
  })
}

async function cli() {
  const stream = evmStream({
    id: 'simple-decoder',
    source: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    outputs,
  }).pipe(wholePipeTransform())

  for await (const { data } of stream) {
    console.log(data.v1.map((d) => d.block_number))
    console.log(data.v2.map((d) => d.block_number_renamed))
  }
}

void cli()

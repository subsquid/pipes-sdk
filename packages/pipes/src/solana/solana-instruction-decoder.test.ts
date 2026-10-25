import { type MockInstance, describe, expect, it, vi } from 'vitest'

import { InstructionDecoderConfigurationError, defaultLogger } from '~/core/index.js'
import { Instructions, SolanaProgram, solanaInstructionDecoder } from '~/solana/solana-instruction-decoder.js'
import { solanaPortalStream } from '~/solana/solana-portal-source.js'
import { MockResponse, mockMetricsServer, readAll } from '~/testing/index.js'
import { withPortal } from '~/testing/portal-fixture.js'

import * as tokenProgram from './abi/tokenProgram/index.js'

const TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
const TOKEN_2022 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
const ACCOUNTS = [
  '98vhGWL5CtK61KSKCJjJn2PVfkjzaw9QF6sRLptBWZvZ',
  '49gyyvxzf61PknHoTg2cFGYQCJRnUrC7Web8h8go7ceM',
  'EDMGEpKKGKS7nxpu1gjLmuHHWAmvLNy3BZWDxNC3nhAt',
]

/** A Token `transfer` of 50000000000 (`'3DXy58UDhJuu'`) at `programId`. */
function transfer(programId: string, transactionIndex: number, data = '3DXy58UDhJuu') {
  return { transactionIndex, instructionAddress: [transactionIndex, 0, 0], programId, accounts: ACCOUNTS, data }
}

function blockWith(instructions: object[], validateRequest?: (body: any) => void): MockResponse[] {
  return [
    {
      statusCode: 200,
      validateRequest,
      data: [
        { header: { number: 1, hash: 'ooooooooooooooooooooooooooooooooooooooooooo1', timestamp: 2000 }, instructions },
      ],
    },
  ]
}

describe('solanaInstructionDecoder transform', () => {
  withPortal('decodes the instructions of its program', async ({ portal: open }) => {
    const portal = await open(blockWith([transfer(TOKEN, 85)]))

    const stream = solanaPortalStream({
      id: 'test',
      portal: portal.url,
      logger: false,
      outputs: solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program: tokenProgram,
        instructions: {
          transfers: tokenProgram.instructions.transfer,
        },
      }).pipe((e) => e.transfers),
    })

    const res = await readAll(stream)

    expect(res).toHaveLength(1)

    const [tx] = res

    expect(tx.programId).toBe(TOKEN)
    expect(tx.instruction).toMatchObject({
      accounts: {
        authority: 'EDMGEpKKGKS7nxpu1gjLmuHHWAmvLNy3BZWDxNC3nhAt',
        destination: '49gyyvxzf61PknHoTg2cFGYQCJRnUrC7Web8h8go7ceM',
        source: '98vhGWL5CtK61KSKCJjJn2PVfkjzaw9QF6sRLptBWZvZ',
      },
      data: {
        amount: 50000000000n,
      },
    })
  })

  withPortal('requests and emits only the address of its program', async ({ portal: open }) => {
    let requestBody: any
    // The portal answers the union of every output's requests, so a foreign transfer can arrive.
    const portal = await open(
      blockWith([transfer(TOKEN, 85), transfer(TOKEN_2022, 86)], (body) => {
        requestBody = body
      }),
    )

    const stream = solanaPortalStream({
      id: 'test',
      portal: portal.url,
      logger: false,
      outputs: solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program: tokenProgram,
        instructions: { transfers: tokenProgram.instructions.transfer },
      }).pipe((e) => e.transfers),
    })

    const res = await readAll(stream)

    expect(res.map((r) => r.programId)).toEqual([TOKEN])
    expect(requestBody.instructions).toHaveLength(1)
    expect(requestBody.instructions[0]).toMatchObject({ programId: [TOKEN], d1: ['0x03'] })
  })

  withPortal('emits only the instructions of its own address across decoders', async ({ portal: open }) => {
    const portal = await open(blockWith([transfer(TOKEN, 85)]))

    const stream = solanaPortalStream({
      id: 'test',
      portal: portal.url,
      logger: false,
      outputs: {
        validProgramId: solanaInstructionDecoder({
          range: { from: 0, to: 1 },
          program: tokenProgram,
          instructions: {
            transfers: tokenProgram.instructions.transfer,
          },
        }),
        unknownProgramId: solanaInstructionDecoder({
          range: { from: 0, to: 1 },
          program: { ...tokenProgram, programId: 'xxxx' },
          instructions: {
            transfers: tokenProgram.instructions.transfer,
          },
        }),
      },
    })

    const valid = []
    const invalid = []
    for await (const { data } of stream) {
      valid.push(...data.validProgramId.transfers)
      invalid.push(...data.unknownProgramId.transfers)
    }

    expect(valid).toHaveLength(1)
    expect(invalid).toHaveLength(0)
  })
})

describe('solanaInstructionDecoder program binding', () => {
  const decode = () => ({ accounts: {}, data: {} })
  // By default, Anchor derives a discriminator from the instruction name alone:
  // sha256("global:swap")[..8] is the `swap` of every program that keeps the default.
  const SWAP = '0xf8c69e91e17587c8'
  const jupiter = {
    programId: 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
    instructions: { swap: { d8: SWAP, decode } },
  }
  const raydium = {
    programId: 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
    instructions: { swap: { d8: SWAP, decode }, deposit: { d8: '0xf223c68952e1f2b6', decode } },
  }

  it.each<{ case: string; instructions: Instructions; offending: string }>([
    {
      case: "another program's instruction beside its own",
      instructions: { jupSwap: jupiter.instructions.swap, rayDeposit: raydium.instructions.deposit },
      offending: 'rayDeposit',
    },
    {
      case: 'a same-named instruction of another program',
      instructions: { swap: raydium.instructions.swap },
      offending: 'swap',
    },
  ])('refuses $case', ({ instructions, offending }) => {
    // Widened to the base type, the way an untyped caller passes it: the types no longer tie the
    // entries to the program, so the check at construction must.
    const program: SolanaProgram = jupiter
    const build = () =>
      solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program,
        instructions,
      })

    expect(build).toThrow(InstructionDecoderConfigurationError)
    expect(build).toThrow(`Instruction "${offending}" is not one of the program's instructions.`)
  })

  it('refuses a program with an empty address', () => {
    expect(() =>
      solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program: { ...tokenProgram, programId: '' },
        instructions: { transfers: tokenProgram.instructions.transfer },
      }),
    ).toThrow(/no program address/)
  })

  it('does not compile with an object that is not a program, or with an instruction of another type', () => {
    expect(() =>
      solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        // @ts-expect-error a program has an address and instructions
        program: {},
        instructions: { transfers: tokenProgram.instructions.transfer },
      }),
    ).toThrow(InstructionDecoderConfigurationError)

    expect(() =>
      // @ts-expect-error an Anchor `swap` is not one of the Token program's instructions
      solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program: tokenProgram,
        instructions: { swap: raydium.instructions.swap },
      }),
    ).toThrow(InstructionDecoderConfigurationError)
  })
})

describe('solanaInstructionDecoder deprecated programId', () => {
  function programIdWarnings(warn: MockInstance) {
    return warn.mock.calls.filter(([message]) => String(message).includes('without `program`'))
  }

  withPortal('still decodes, and does not warn for a single address', async ({ portal: open }) => {
    const portal = await open(blockWith([transfer(TOKEN, 85)]))
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')

    const stream = solanaPortalStream({
      id: 'test',
      portal: portal.url,
      logger,
      outputs: solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        programId: TOKEN,
        instructions: { transfers: tokenProgram.instructions.transfer },
      }).pipe((e) => e.transfers),
    })

    const res = await readAll(stream)

    expect(res).toHaveLength(1)
    expect(programIdWarnings(warn)).toHaveLength(0)
  })

  withPortal('warns once when it lists several addresses', async ({ portal: open }) => {
    const portal = await open(blockWith([transfer(TOKEN, 85), transfer(TOKEN_2022, 86)]))
    const logger = defaultLogger({ level: 'silent' })
    const warn = vi.spyOn(logger, 'warn')

    const stream = solanaPortalStream({
      id: 'test',
      portal: portal.url,
      logger,
      outputs: solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        programId: [TOKEN, TOKEN_2022],
        instructions: { transfers: tokenProgram.instructions.transfer },
      }).pipe((e) => e.transfers),
    })

    const res = await readAll(stream)

    expect(res).toHaveLength(2)
    expect(programIdWarnings(warn)).toHaveLength(1)
  })
})

describe('solanaInstructionDecoder decode errors', () => {
  // A decodable transfer followed by one whose data is the discriminator byte only
  // (`'4'` = 0x03) — it matches d1 routing but `amount` (u64) cannot be read, so
  // `decode` throws.
  const RESPONSES = blockWith([transfer(TOKEN, 85), transfer(TOKEN, 86, '4')])

  function stream(
    portalUrl: string,
    metrics: ReturnType<typeof mockMetricsServer>,
    onError?: (ctx: any, error: any) => unknown,
  ) {
    return solanaPortalStream({
      id: 'test',
      portal: portalUrl,
      logger: false,
      metrics: metrics.server,
      outputs: solanaInstructionDecoder({
        range: { from: 0, to: 1 },
        program: tokenProgram,
        instructions: { transfers: tokenProgram.instructions.transfer },
        onError,
      }).pipe((e) => e.transfers),
    })
  }

  withPortal('is fatal by default — no hook re-throws the decode error', async ({ portal: open }) => {
    const portal = await open(RESPONSES)
    const metrics = mockMetricsServer()

    await expect(readAll(stream(portal.url, metrics))).rejects.toThrow()
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })

  withPortal('a returning hook suppresses the record and counts the skip', async ({ portal: open }) => {
    const portal = await open(RESPONSES)
    const metrics = mockMetricsServer()
    const seen: any[] = []

    const res = await readAll(stream(portal.url, metrics, (_ctx, error) => seen.push(error)))

    expect(res).toHaveLength(1)
    expect(res[0].instruction.data.amount).toBe(50000000000n)
    expect(seen).toHaveLength(1)

    const skipped = metrics.counter('sqd_decode_errors_skipped_total')
    expect(skipped.total).toBe(1)
    expect(skipped.calls[0].labels).toEqual({ id: 'test' })
  })

  withPortal('a re-throwing hook stays fatal and records no skip', async ({ portal: open }) => {
    const portal = await open(RESPONSES)
    const metrics = mockMetricsServer()

    await expect(
      readAll(
        stream(portal.url, metrics, (_ctx, error) => {
          throw error
        }),
      ),
    ).rejects.toThrow()
    expect(metrics.counter('sqd_decode_errors_skipped_total')).toBeUndefined()
  })
})

describe('solanaInstructionDecoder configuration guards', () => {
  const anchorSwap = { d8: '0xf8c69e91e17587c8', decode: () => ({ accounts: {}, data: {} }) }

  /** A decoder over a program whose instructions are exactly `instructions`, so only the guard under test can refuse it. */
  function decoderOver(instructions: Instructions) {
    return solanaInstructionDecoder({
      range: { from: 0, to: 1 },
      program: { programId: TOKEN, instructions },
      instructions,
    })
  }

  it('rejects mixing discriminator widths in one decoder', () => {
    expect(() => decoderOver({ transfers: tokenProgram.instructions.transfer, swap: anchorSwap })).toThrow(
      InstructionDecoderConfigurationError,
    )
  })

  it('rejects two instructions sharing a discriminator', () => {
    expect(() =>
      decoderOver({ transfers: tokenProgram.instructions.transfer, alias: tokenProgram.instructions.transfer }),
    ).toThrow(InstructionDecoderConfigurationError)
  })

  it('rejects a decoder with no discriminators', () => {
    expect(() => decoderOver({ noop: { decode: () => ({ accounts: {}, data: {} }) } })).toThrow(
      InstructionDecoderConfigurationError,
    )
  })

  it('rejects an instruction that sets multiple discriminator widths', () => {
    expect(() =>
      decoderOver({
        transfers: { d1: '0x03', d8: '0xf8c69e91e17587c8', decode: () => ({ accounts: {}, data: {} }) },
      }),
    ).toThrow(InstructionDecoderConfigurationError)
  })

  it('names the offending instructions when every entry lacks a discriminator', () => {
    expect(() =>
      decoderOver({
        swap: { decode: () => ({ accounts: {}, data: {} }) },
        claim: { decode: () => ({ accounts: {}, data: {} }) },
      }),
    ).toThrow(/"swap", "claim"/)
  })

  it('rejects a discriminator-less instruction alongside a valid one', () => {
    expect(() =>
      decoderOver({
        transfers: tokenProgram.instructions.transfer,
        noop: { decode: () => ({ accounts: {}, data: {} }) },
      }),
    ).toThrow(InstructionDecoderConfigurationError)
  })
})

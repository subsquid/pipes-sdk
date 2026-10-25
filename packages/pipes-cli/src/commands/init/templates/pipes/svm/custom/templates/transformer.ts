import Mustache from 'mustache'

import { flattenContracts } from '../../../../contract-params.js'
import { CustomTemplateParams } from '../template.config.js'
import { programIdentifiers } from './naming.js'

interface Program {
  contractName: string
  contractAddress: string
  /** Reference-deployment address for the typegen import path. */
  typegenAddress: string
  contractEvents: Array<{ name: string }>
  range: { from: string; to?: string }
}

interface DecoderGroup {
  decoderId: string
  rangeFrom: string
  rangeTo?: string
  /** The typegen import the decoder binds its instructions to. */
  programAlias: string
  /** `program:` as rendered: the typegen import, with `programId` set to the configured address. */
  program: string
  programs: Program[]
  instructions: Array<{ contractName: string; name: string; uniqueKey: string }>
}

export const customContractTemplate = `import { solanaInstructionDecoder } from '@subsquid/pipes/solana'
{{#imports}}
import * as {{{alias}}}Program from "./contracts/{{{address}}}/index.js"
{{/imports}}
import { enrichEvents } from './utils/index.js'

{{#decoderGroups}}
const {{{decoderId}}} = solanaInstructionDecoder({
  range: { from: '{{{rangeFrom}}}'{{#rangeTo}}, to: '{{{rangeTo}}}'{{/rangeTo}} },
  program: {{{program}}},
  /**
   * Or pass every instruction of the program:
   * \`\`\`ts
   * instructions: {{{programAlias}}}Program.instructions,
   * \`\`\`
   */
  instructions: {
    {{#instructions}}
    {{uniqueKey}}: {{contractName}}Program.instructions.{{name}},
    {{/instructions}}
  },
}).pipe(enrichEvents)
{{/decoderGroups}}
`

export function buildDecoderGroups(params: CustomTemplateParams): DecoderGroup[] {
  const flat = flattenContracts(params.contracts)
  if (flat.length === 0) {
    return []
  }

  const identifiers = programIdentifiers(flat)

  const programs: Program[] = flat.map((c) => ({
    contractName: identifiers.get(c.typegenAddress)!,
    contractAddress: c.contractAddress,
    typegenAddress: c.typegenAddress,
    contractEvents: c.contractEvents,
    range: c.range ?? { from: 'latest' },
  }))

  // One decoder per (deployment, range): a decoder reads one address, its `program`'s. Different
  // programs must never share a decoder: Solana discriminators are program-independent, so
  // two programs with a same-named instruction would decode each other's data (the decoder
  // refuses instructions from outside its `program`). Keyed by typegen address so name
  // normalization can't merge distinct programs.
  const groups = new Map<string, Program>()
  for (const p of programs) {
    groups.set(`${p.typegenAddress}|${p.contractAddress}|${p.range.from}|${p.range.to ?? ''}`, p)
  }

  const groupList = [...groups.values()]
  if (groupList.length === 1) {
    return [makeGroup('custom', groupList[0]!)]
  }

  const usedIds = new Set<string>()
  return groupList.map((deployment) => {
    const name = deployment.contractName
    const suffix = name.charAt(0).toUpperCase() + name.slice(1)
    let decoderId = `custom${suffix}`
    for (let n = 2; usedIds.has(decoderId); n++) {
      decoderId = `custom${suffix}${n}`
    }
    usedIds.add(decoderId)

    return makeGroup(decoderId, deployment)
  })
}

function makeGroup(decoderId: string, deployment: Program): DecoderGroup {
  const alias = deployment.contractName

  // Always the configured address: typegen takes `programId` from the address embedded in the IDL
  // when it has one, and that can differ from the address the IDL was fetched from.
  const program = `{ ...${alias}Program, programId: "${deployment.contractAddress}" }`

  return {
    decoderId,
    rangeFrom: deployment.range.from,
    rangeTo: deployment.range.to,
    programAlias: alias,
    program,
    programs: [deployment],
    instructions: deployment.contractEvents.map((e) => ({ contractName: alias, name: e.name, uniqueKey: e.name })),
  }
}

export function renderTransformer(params: CustomTemplateParams): string {
  const decoderGroups = buildDecoderGroups(params)

  // One typegen import per program, deduped across groups and deployments.
  const imports = new Map<string, string>()
  for (const group of decoderGroups) {
    for (const program of group.programs) {
      imports.set(program.contractName, program.typegenAddress)
    }
  }

  return Mustache.render(customContractTemplate, {
    decoderGroups,
    imports: [...imports].map(([alias, address]) => ({ alias, address })),
  })
}

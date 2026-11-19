import path from 'node:path'

import ts from 'typescript'
import { describe, expect, it } from 'vitest'

import type { NetworkType, Target } from '~/types/init.js'

import { tsconfigConfigTemplate } from '../static/tsconfig.js'
import { renderUtilsTemplate } from './utils.js'

// The file must sit inside this package, so that `@subsquid/pipes` resolves from its node_modules.
const projectRoot = path.resolve(__dirname, '__generated__')
const generatedFile = path.join(projectRoot, 'src/utils/index.ts')

function typeErrors(source: string): string[] {
  const generatedTsconfig = JSON.parse(tsconfigConfigTemplate)
  const { options, errors: tsconfigErrors } = ts.convertCompilerOptionsFromJson(
    generatedTsconfig.compilerOptions,
    projectRoot,
  )
  options.noEmit = true

  const host = ts.createCompilerHost(options)
  const readSourceFile = host.getSourceFile
  const fileExists = host.fileExists

  host.fileExists = (fileName) => fileName === generatedFile || fileExists(fileName)
  host.getSourceFile = (fileName, languageVersion, ...rest) => {
    if (fileName === generatedFile) {
      return ts.createSourceFile(fileName, source, languageVersion, true)
    }

    return readSourceFile(fileName, languageVersion, ...rest)
  }

  const program = ts.createProgram([generatedFile], options, host)
  const programErrors = ts.getPreEmitDiagnostics(program, program.getSourceFile(generatedFile))
  const diagnostics = [...tsconfigErrors, ...programErrors]

  return diagnostics.map((d) => ts.flattenDiagnosticMessageText(d.messageText, '\n'))
}

const cases: { networkType: NetworkType; target: Target }[] = [
  { networkType: 'evm', target: 'clickhouse' },
  { networkType: 'evm', target: 'postgresql' },
  { networkType: 'svm', target: 'clickhouse' },
  { networkType: 'svm', target: 'postgresql' },
]

describe('renderUtilsTemplate', () => {
  it.each(cases)('type-checks against @subsquid/pipes for $networkType + $target', ({ networkType, target }) => {
    const source = renderUtilsTemplate({ networkType, target })

    expect(typeErrors(source)).toEqual([])
  })
})

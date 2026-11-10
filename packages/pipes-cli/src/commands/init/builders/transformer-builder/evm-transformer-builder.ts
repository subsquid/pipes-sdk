import { BaseTransformerBuilder } from './base-transformer-builder.js'

export const template = `{{#deduplicatedImports}}
{{{.}}}
{{/deduplicatedImports}}

{{{envTemplate}}}

{{#transformerTemplates}}
{{{code}}}

{{/transformerTemplates}}
export async function main() {
{{^rpcFallback}}
  await evmPortalStream({
    id: '{{pipeId}}',
    portal: 'https://portal.sqd.dev/datasets/{{network}}',
{{/rpcFallback}}
{{#rpcFallback}}
  await evmStream({
    id: '{{pipeId}}',
    source: [
      'https://portal.sqd.dev/datasets/{{network}}',
      { type: 'rpc', url: env.RPC_URL, name: 'rpc-fallback' },
    ],
{{/rpcFallback}}
    outputs: {
{{#transformerTemplates}}
{{#templateId}}
      {{{templateId}}},
{{/templateId}}
{{#templateIds}}
      {{{.}}},
{{/templateIds}}
{{/transformerTemplates}}
    },
  })
  .pipeTo({{{targetTemplate}}})
}

void main()
`

export class EvmTransformerBuilder extends BaseTransformerBuilder<'evm'> {
  getTemplate(): string {
    return template
  }

  getNetworkImports() {
    const entry = this.config.rpcFallback === true ? 'evmStream' : 'evmPortalStream'

    return [`import { ${entry} } from "@subsquid/pipes/evm"`]
  }
}

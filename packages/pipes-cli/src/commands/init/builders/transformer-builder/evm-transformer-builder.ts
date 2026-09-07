import { BaseTransformerBuilder } from './base-transformer-builder.js'

export const template = `{{#deduplicatedImports}}
{{{.}}}
{{/deduplicatedImports}}

{{{envTemplate}}}

{{#transformerTemplates}}
{{{code}}}

{{/transformerTemplates}}
export async function main() {
  await evmStream({
    id: '{{pipeId}}',
{{^rpcFallback}}
    source: 'https://portal.sqd.dev/datasets/{{network}}',
{{/rpcFallback}}
{{#rpcFallback}}
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
    return ['import { evmStream } from "@subsquid/pipes/evm"']
  }
}

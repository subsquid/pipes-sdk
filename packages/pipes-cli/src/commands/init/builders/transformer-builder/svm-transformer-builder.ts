import { BaseTransformerBuilder } from './base-transformer-builder.js'

export const template = `{{#deduplicatedImports}}
{{{.}}}
{{/deduplicatedImports}}

{{{envTemplate}}}

{{#transformerTemplates}}
{{{code}}}

{{/transformerTemplates}}
export async function main() {
  await solanaPortalStream({
    id: '{{pipeId}}',
    source: 'https://portal.sqd.dev/datasets/{{network}}',
    outputs: {
{{#transformerTemplates}}
      {{{templateId}}},
{{/transformerTemplates}}
    },
  })
  .pipeTo({{{targetTemplate}}})
}

void main()
`

export class SvmTransformerBuilder extends BaseTransformerBuilder<'svm'> {
  getTemplate(): string {
    return template
  }

  getNetworkImports(): string[] {
    return ['import { solanaPortalStream } from "@subsquid/pipes/solana"']
  }
}

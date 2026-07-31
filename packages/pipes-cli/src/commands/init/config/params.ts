import z from 'zod'

import { NetworkType, packageManagerTypes, targetTypes } from '~/types/init.js'

import { TemplateNotFoundError } from '../init.errors.js'
import { getTemplate, getTemplates } from '../templates/registry.js'
import { getPortalNetworkSlugs } from './networks.js'

/** Canonical hosted location of the `--config` JSON schema (also its `$id`). */
export const CONFIG_SCHEMA_URL = 'https://cdn.subsquid.io/schemas/pipes_cli_config.json'

function getTemplateSchemas<N extends NetworkType>(networkType: N) {
  const networkTemplates = getTemplates(networkType)
  const options = networkTemplates.map((template) =>
    z
      .object({
        templateId: z.literal(template.id),
        ...(template.paramsSchema ? { params: template.paramsSchema } : {}),
      })
      .strict(),
  )

  if (options.length === 0) {
    throw new Error(`Expected at least one template for network ${networkType}, got none`)
  }

  // discriminatedUnion needs two options; a single-template network degrades to its one schema.
  if (options.length === 1) {
    return z.array(options[0]!)
  }

  const [first, second, ...rest] = options
  return z.array(z.discriminatedUnion('templateId', [first!, second!, ...rest]))
}

const baseSchemaRaw = z.object({
  projectFolder: z.string().min(1),
  packageManager: z.enum(packageManagerTypes.map((p) => p.value)),
  target: z.enum(targetTypes.map((t) => t.value)).describe('Storage target for the pipeline data.'),
  pipeId: z
    .string()
    .min(1)
    .max(64)
    // Interpolated into generated TS as `id: '{{pipeId}}'`. Mustache HTML-escapes
    // that tag, so a value containing & < > " ' would silently render as a
    // *different* id (`a&b` -> `a&amp;b`) and orphan the cursor — the exact
    // failure this option exists to prevent. Restrict to characters that survive
    // escaping unchanged, while still allowing readable ids like 'usdc-transfers'.
    .regex(/^[A-Za-z0-9._-]+$/, 'pipeId may only contain letters, digits, dots, underscores and hyphens')
    .optional()
    .describe(
      'Stream id of the generated pipe, and the key its target cursor is stored under. Letters, digits, dots, underscores and hyphens; up to 64 characters. Generated when omitted; keep the value written to pipes.config.json so regenerating reuses the same cursor.',
    ),
})

const evmConfig = baseSchemaRaw
  .extend({
    networkType: z.literal('evm'),
    defaultNetwork: z
      .enum(getPortalNetworkSlugs('evm'))
      .describe('Network every template indexes; per-deployment networks may override it in the future.'),
    templates: getTemplateSchemas('evm'),
  })
  .strict()

const svmConfig = baseSchemaRaw
  .extend({
    networkType: z.literal('svm'),
    defaultNetwork: z
      .enum(getPortalNetworkSlugs('svm'))
      .describe('Network every template indexes; per-deployment networks may override it in the future.'),
    templates: getTemplateSchemas('svm'),
  })
  .strict()

export const configJsonSchemaRaw = z.discriminatedUnion('networkType', [evmConfig, svmConfig])

export const configJsonSchema = configJsonSchemaRaw.transform((data) => {
  const networkType = data.networkType
  return {
    ...data,
    networkType,
    packageManager: data.packageManager,
    templates: data.templates.map((t) => {
      const template = getTemplate(networkType, t.templateId)
      if (!template) throw new TemplateNotFoundError(t.templateId, networkType)
      return { template, params: t.params }
    }),
  }
})

import { confirm, input, select } from '@inquirer/prompts'
import chalk from 'chalk'

import type { Config, PackageManager } from '~/types/init.js'
import { type NetworkType, networkTypes, packageManagerTypes } from '~/types/init.js'

import { networks } from './config/networks.js'
import { prepareConfig } from './config/prepare-config.js'
import { targets } from './config/targets.js'
import { templatePromptLoop } from './config/template-prompt-loop.js'
import { validateProjectFolder } from './config/validate-project-folder.js'
import { validateRpcUrl } from './config/validate-rpc-url.js'
import { InitHandler } from './init.handler.js'

export class InitPrompt {
  async run() {
    try {
      const { config, rpcUrl } = await this.promptConfig()
      await prepareConfig(config)
      const handler = new InitHandler(config, { rpcUrl })
      await handler.handle()
    } catch (error) {
      if (error instanceof Error) {
        throw error
      }
      throw new Error(`Failed to initialize project: ${String(error)}`)
    }
  }

  async promptConfig(): Promise<{ config: Config<NetworkType>; rpcUrl?: string }> {
    const projectFolder = await input({
      message: `Where should we create your new project? ${chalk.dim('Enter a folder name or path:')}`,
      validate: validateProjectFolder,
    })

    const packageManager = await select<PackageManager>({
      message: 'Which package manager would you like to use?',
      choices: packageManagerTypes,
    })

    const networkType = await select<NetworkType>({
      message: "Now, let's choose the type of blockchain you'd like to use:",
      choices: networkTypes,
    })

    const networksChoices = networks[networkType].map((n) => ({
      name: n.name,
      value: n.slug,
      priority: (n as { priority?: number }).priority,
    }))
    networksChoices.sort((a, b) => {
      if (a.priority && b.priority) return b.priority - a.priority
      if (a.priority) return -1
      if (b.priority) return 1
      return a.name.localeCompare(b.name)
    })

    const network = await select({
      message: `Now, let's select the network you'd like to use. ${chalk.dim('(Tip: you can type to search)')}`,
      choices: networksChoices,
      pageSize: 15,
    })

    // EVM only: the Solana SDK has no fallback support. The URL is returned
    // out-of-band of the config — it may embed an API key and must never reach
    // the committed pipes.config.json; it only lands in the generated .env.
    let rpcFallback = false
    let rpcUrl: string | undefined
    if (networkType === 'evm') {
      rpcFallback = await confirm({
        message: `Add an RPC fallback source? ${chalk.dim('The pipe keeps running from your RPC endpoint while the Portal is unavailable')}`,
        default: false,
      })
      if (rpcFallback) {
        const answer = await input({
          message: `RPC endpoint URL ${chalk.dim('(press Enter to skip — set RPC_URL in .env later)')}`,
          validate: validateRpcUrl,
        })
        rpcUrl = answer.trim() || undefined
      }
    }

    const selectedTemplates = await templatePromptLoop(networkType, network)

    const target = await select({
      message: 'Where would you like to store your data?',
      choices: targets.map((t) => ({ name: t.name, value: t.id })),
    })

    return {
      config: {
        projectFolder,
        networkType,
        defaultNetwork: network,
        templates: selectedTemplates,
        target,
        packageManager,
        ...(rpcFallback ? { rpcFallback } : {}),
      },
      rpcUrl,
    }
  }
}

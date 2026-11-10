import type { Config, NetworkType } from '~/types/init.js'

import { renderDependencies } from './dynamic/dependencies.js'
import { renderDockerCompose } from './dynamic/docker-compose.js'
import { renderDockerfile } from './dynamic/docker-file.js'
import { renderPackageJson } from './dynamic/package-json.js'
import { renderReadme } from './dynamic/readme.js'
import { renderUtilsTemplate } from './dynamic/utils.js'
import { agentsTemplate } from './static/agents.js'
import { biomeConfigTemplate } from './static/biome.js'
import { gitignoreTemplate } from './static/gitignore.js'
import { pnpmWorkspaceTemplate } from './static/pnpm-workspace.js'
import { tsconfigConfigTemplate } from './static/tsconfig.js'

export type ConfigFileSpec = {
  path: string
  contents: string
}

export function planConfigFiles(config: Config<NetworkType>, projectName: string): ConfigFileSpec[] {
  const isPostgres = config.target === 'postgresql'
  const hasRpcFallback = config.rpcFallback === true
  const { dependencies, devDependencies } = renderDependencies(config.target, config.networkType, {
    rpcFallback: hasRpcFallback,
  })

  const specs: ConfigFileSpec[] = [
    { path: 'biome.json', contents: biomeConfigTemplate },
    { path: 'tsconfig.json', contents: tsconfigConfigTemplate },
    { path: '.gitignore', contents: gitignoreTemplate },
    { path: 'AGENTS.md', contents: agentsTemplate },
  ]

  if (config.packageManager === 'pnpm') {
    specs.push({ path: 'pnpm-workspace.yaml', contents: pnpmWorkspaceTemplate })
  }

  specs.push(
    {
      path: 'package.json',
      contents: renderPackageJson({
        projectName,
        dependencies,
        devDependencies,
        hasPostgresScripts: isPostgres,
        packageManager: config.packageManager,
      }),
    },
    {
      path: 'Dockerfile',
      contents: renderDockerfile({
        isPostgres,
        packageManager: config.packageManager,
      }),
    },
    {
      path: 'docker-compose.yml',
      contents: renderDockerCompose({
        projectName,
        target: config.target,
        packageManager: config.packageManager,
        hasRpcFallback,
      }),
    },
    {
      path: 'README.md',
      contents: renderReadme({
        packageManager: config.packageManager,
        projectName,
        hasPostgresScripts: isPostgres,
        hasRpcFallback,
      }),
    },
    { path: 'src/utils/index.ts', contents: renderUtilsTemplate(config) },
  )

  return specs
}

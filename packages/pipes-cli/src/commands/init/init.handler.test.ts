import { mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { Config } from '~/types/init.js'

import { InitHandler } from './init.handler.js'
import { InitPipelineError } from './pipeline/index.js'
import { getTemplate } from './templates/registry.js'

const runIntegration = process.env['RUN_INTEGRATION'] === '1'
const describeIntegration = runIntegration ? describe : describe.skip

async function exists(p: string) {
  try {
    await stat(p)
    return true
  } catch {
    return false
  }
}

async function isDir(p: string) {
  const st = await stat(p)
  return st.isDirectory()
}

async function isDirEmpty(p: string) {
  const files = await readdir(p)
  return files.length === 0
}

async function isFile(p: string) {
  const st = await stat(p)
  return st.isFile()
}

function fileContent(p: string) {
  return readFile(p, 'utf8')
}

function configuredErc20(range: { from: string; to?: string } = { from: '12,369,621' }) {
  const template = getTemplate('evm', 'erc20Transfers')!
  return {
    template,
    params: {
      deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range }],
    },
  }
}

function configuredUniswap() {
  const template = getTemplate('evm', 'uniswapV3Swaps')!
  return {
    template,
    params: {
      factoryAddress: '0x1f98431c8ad98523631ae4a59f267346ea31f984',
      range: { from: '12,369,621' },
    },
  }
}

function configuredCustomWeth() {
  const template = getTemplate('evm', 'custom')!
  return {
    template,
    params: {
      contracts: [
        {
          contractName: 'WETH9',
          contractEvents: [
            {
              name: 'Approval',
              type: 'event',
              inputs: [
                { name: 'src', type: 'address' },
                { name: 'guy', type: 'address' },
                { name: 'wad', type: 'uint256' },
              ],
            },
            {
              name: 'Transfer',
              type: 'event',
              inputs: [
                { name: 'src', type: 'address' },
                { name: 'dst', type: 'address' },
                { name: 'wad', type: 'uint256' },
              ],
            },
          ],
          deployments: [{ address: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', range: { from: 'latest' } }],
        },
      ],
    },
  }
}

describeIntegration('InitHandler', () => {
  const PROJECT_NAME = 'my-project'
  let tmpRoot: string
  let projectDir: string

  beforeEach(async () => {
    tmpRoot = await mkdtemp(path.join(tmpdir(), 'my-cli-'))
    projectDir = path.join(tmpRoot, PROJECT_NAME)
  })

  afterEach(async () => {
    await rm(tmpRoot, { recursive: true, force: true })
  })

  it('creates expected base folder structure', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    await expect(exists(projectDir)).resolves.toBe(true)
    await expect(isDir(projectDir)).resolves.toBe(true)
    await expect(isDir(path.join(projectDir, 'src'))).resolves.toBe(true)
    await expect(isDir(path.join(projectDir, 'node_modules'))).resolves.toBe(true)
  })

  it('creates project base files', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    const filesInRoot = await readdir(projectDir)

    expect(filesInRoot).toEqual(
      expect.arrayContaining([
        'package.json',
        'biome.json',
        'tsconfig.json',
        '.env',
        '.gitignore',
        'pnpm-lock.yaml',
        'Dockerfile',
        'docker-compose.yml',
      ]),
    )
  })

  it('creates project specific folders and files for pre-built template target is clickhouse ', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    const packageJsonContent = await fileContent(path.join(projectDir, 'package.json'))
    expect(packageJsonContent).to.not.include('"db:generate": "drizzle-kit generate"')
    expect(packageJsonContent).to.not.include('"db:migrate": "drizzle-kit migrate"')
    expect(packageJsonContent).to.not.include('"db:push": "drizzle-kit push"')

    await expect(isDir(path.join(projectDir, 'migrations'))).resolves.toBe(true)
    await expect(isDirEmpty(path.join(projectDir, 'migrations'))).resolves.toBe(false)

    const dockerComposePath = path.join(projectDir, 'docker-compose.yml')
    await expect(isFile(dockerComposePath)).resolves.toBe(true)
    await expect(fileContent(dockerComposePath)).resolves.toMatchInlineSnapshot(`
      "services:
        my-project:
          build:
            context: .
            dockerfile: Dockerfile
          environment:
            CLICKHOUSE_URL: http://clickhouse:8123
            CLICKHOUSE_DATABASE: pipes
            CLICKHOUSE_USER: default
            CLICKHOUSE_PASSWORD: password
          command: ["sh", "-lc", "node dist/index.js"]
          depends_on:
            clickhouse:
              condition: service_healthy
          restart: unless-stopped
          profiles: ["with-pipeline"]

        clickhouse:
          image: clickhouse/clickhouse-server:latest
          ports:
            - "8123:8123"
          environment:
            CLICKHOUSE_DB: pipes
            CLICKHOUSE_USER: default
            CLICKHOUSE_PASSWORD: password
          healthcheck:
            test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
            interval: 3s
            timeout: 5s
            retries: 5

      "
    `)
  })

  it('creates migration folder and file for custom template when target is clickhouse', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredCustomWeth()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    await expect(isDir(path.join(projectDir, 'migrations'))).resolves.toBeTruthy()
    await expect(isDirEmpty(path.join(projectDir, 'migrations'))).resolves.toBeFalsy()
  })

  it('creates project specific folders and files for postgresql target', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'postgresql',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    const packageJsonContent = await fileContent(path.join(projectDir, 'package.json'))
    expect(packageJsonContent).to.include('"db:generate": "drizzle-kit generate"')
    expect(packageJsonContent).to.include('"db:migrate": "drizzle-kit migrate"')
    expect(packageJsonContent).to.include('"db:push": "drizzle-kit push"')

    await expect(isDir(path.join(projectDir, 'migrations'))).resolves.toBe(true)
    await expect(isFile(path.join(projectDir, 'src/schemas.ts'))).resolves.toBe(true)

    const dockerComposePath = path.join(projectDir, 'docker-compose.yml')
    await expect(isFile(dockerComposePath)).resolves.toBe(true)
    await expect(fileContent(dockerComposePath)).resolves.toMatchInlineSnapshot(`
      "services:
        my-project:
          build:
            context: .
            dockerfile: Dockerfile
          environment:
            DB_CONNECTION_STR: postgresql://postgres:password@postgres:5432/pipes
          command: ["sh", "-lc", "pnpm run db:generate && pnpm run db:migrate && node dist/index.js"]
          depends_on:
            postgres:
              condition: service_healthy
          restart: unless-stopped
          profiles: ["with-pipeline"]

        postgres:
          image: postgres:latest
          environment:
            POSTGRES_USER: postgres
            POSTGRES_PASSWORD: password
            POSTGRES_DB: pipes
          ports:
            - "5432:5432"
          healthcheck:
            test: ["CMD-SHELL", "pg_isready -U postgres"]
            interval: 3s
            timeout: 5s
            retries: 5

      "
    `)
  })

  it('generates additional folders from pipe templates', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredUniswap()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    const contractsDir = path.join(projectDir, 'src/contracts')
    await expect(isDir(contractsDir)).resolves.toBe(true)

    const filesInContractsDir = await readdir(contractsDir)
    expect(filesInContractsDir).toEqual(expect.arrayContaining(['factory.ts', 'pool.ts']))
  })

  it('install dependencies using pnpm as package manager', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'pnpm',
    }

    await new InitHandler(config).handle()

    await expect(isFile(path.join(projectDir, 'pnpm-lock.yaml'))).resolves.toBe(true)
  })

  it('install dependencies using yarn as package manager', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'yarn',
    }

    await new InitHandler(config).handle()

    await expect(isFile(path.join(projectDir, 'yarn.lock'))).resolves.toBe(true)
  })

  it.skip('install dependencies using bun as package manager', async () => {
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'clickhouse',
      packageManager: 'bun',
    }

    await new InitHandler(config).handle()

    await expect(isFile(path.join(projectDir, 'bun.lock'))).resolves.toBe(true)
  })

  it('cleans up the project directory and surfaces the failing stage id when a stage fails', async () => {
    // An invalid target (cast past the type system, as a hand-written --config could produce
    // before validation) makes buildTarget throw inside the `write-index-ts` stage; this is
    // a real stage failure occurring after the safe-to-clean check-project-path stage.
    const config: Config<'evm'> = {
      projectFolder: projectDir,
      networkType: 'evm',
      defaultNetwork: 'ethereum-mainnet',
      templates: [configuredErc20()],
      target: 'bogus' as unknown as Config<'evm'>['target'],
      packageManager: 'pnpm',
    }

    const thrown = await new InitHandler(config).handle().catch((e) => e)

    expect(thrown).toBeInstanceOf(InitPipelineError)
    expect((thrown as InitPipelineError).stageId).toBe('write-index-ts')
    await expect(exists(projectDir)).resolves.toBe(false)
  })
})

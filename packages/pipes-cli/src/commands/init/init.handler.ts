import { existsSync, readFileSync } from 'node:fs'
import path from 'node:path'

import chalk from 'chalk'
import { z } from 'zod'

import { type Config, type NetworkType, targetTypes } from '~/types/init.js'
import { deriveProjectName } from '~/utils/project-name.js'
import { ProjectWriter } from '~/utils/project-writer.js'
import { createSpinner } from '~/utils/spinner.js'

import { CONFIG_SCHEMA_URL, configJsonSchema, configJsonSchemaRaw } from './config/params.js'
import { prepareConfig } from './config/prepare-config.js'
import { PIPE_CONFIG_FILENAME } from './config/serialize-config.js'
import { type InitContext, type StageFailure, initStages, runStages } from './pipeline/index.js'

export class InitHandler {
  private readonly projectName: string
  private readonly projectWriter: ProjectWriter

  constructor(
    private config: Config<NetworkType>,
    private readonly options: { rpcUrl?: string } = {},
  ) {
    this.projectName = deriveProjectName(config.projectFolder)
    this.projectWriter = new ProjectWriter(this.config.projectFolder)
  }

  async handle(): Promise<void> {
    const spinner = createSpinner('Setting up new Pipes SDK project...')
    spinner.start()

    const projectPath = this.projectWriter.getAbsolutePath()
    const ctx: InitContext = {
      config: this.config,
      projectName: this.projectName,
      projectPath,
      projectWriter: this.projectWriter,
      // A re-run against an existing pipes project (identified by its saved
      // config) regenerates in place rather than erroring on the existing folder.
      regenerate: existsSync(path.join(projectPath, PIPE_CONFIG_FILENAME)),
      rpcUrl: this.options.rpcUrl,
    }

    let failures: StageFailure[]
    try {
      failures = await runStages(initStages, ctx, { spinner })
    } catch (error) {
      spinner.fail('Failed to initialize project')
      throw error
    }

    if (failures.length > 0) {
      spinner.warn(`${this.config.projectFolder} created — some steps need your attention`)
      this.reportFailures(failures)
    } else {
      spinner.succeed(`${this.config.projectFolder} project initialized successfully`)
    }

    this.nextSteps(failures)
  }

  private reportFailures(failures: StageFailure[]): void {
    for (const { label, error } of failures) {
      const detail = (error.message || String(error))
        .trimEnd()
        .split('\n')
        .map((line) => `    ${line}`)
        .join('\n')

      console.log('')
      console.log(`  ${chalk.yellow('⚠')} ${chalk.bold(label)} didn't finish:`)
      console.log(detail)
    }
  }

  private nextSteps(failures: StageFailure[] = []): void {
    const sep = `${chalk.green('─'.repeat(64))}`
    const installFailed = failures.some((failure) => failure.stageId === 'install-dependencies')
    const configPath = path.join(this.config.projectFolder, PIPE_CONFIG_FILENAME)

    const installFirst = installFailed
      ? `  ${chalk.bold.yellow('⚠ FIRST — INSTALL DEPENDENCIES')}
  They didn't install automatically (see the error above). Once you've resolved it:
    ${chalk.gray.italic(`cd ${this.config.projectFolder} && ${this.config.packageManager} install`)}

`
      : ''

    const configNote = `  ${chalk.gray('Config saved to')} ${chalk.cyan(configPath)}${chalk.gray('.')}
  ${chalk.gray('To change the generated code, edit this config and re-run')} ${chalk.gray.italic(`pipes init --config ${configPath}`)}
`

    const pgMessage = `3) Apply migrations
     ${chalk.gray.italic(`${this.config.packageManager} run db:migrate`)}

  4) Start the pipeline
     ${chalk.gray.italic(`${this.config.packageManager} run dev`)}`

    const clickhouseMessage = `3) Start the pipeline
     ${chalk.gray.italic(`${this.config.packageManager} run dev`)}`

    const message = `
  ${sep}

            ${chalk.bold.green('🦑 YOUR PIPES SDK PROJECT IS READY TO GO 🦑')}

  ${sep}

  ${chalk.gray.bold("What's next?")}


${installFirst}  ${chalk.bold.yellow('⚡ QUICKSTART')}

  1) Enter the project folder
    ${chalk.gray.italic(`cd ${this.config.projectFolder}`)}

  2) Start collecting data
    ${chalk.gray.italic('docker compose --profile with-pipeline up')}


  ${chalk.bold.blue('💻 DEVELOPMENT')}

  1) Enter the project folder
     ${chalk.gray.italic(`cd ${this.config.projectFolder}`)}

  2) Start your ${targetTypes.find((t) => t.value === this.config.target)?.name} database
     ${chalk.gray.italic('docker compose up -d')}

  ${this.config.target === 'postgresql' ? pgMessage : clickhouseMessage}

${configNote}
  ${chalk.gray('Need help? Check our documentation at')} ${chalk.bold.gray.underline('https://docs.sqd.dev/en/sdk/pipes-sdk')}`

    console.log(message)
  }

  static async fromFile(filePath: string): Promise<InitHandler> {
    const config = InitHandler.parseConfig(readFileSync(filePath, 'utf8'))
    await prepareConfig(config)
    return new InitHandler(config)
  }

  static async fromJson(jsonString: string): Promise<InitHandler> {
    const config = InitHandler.parseConfig(jsonString)
    await prepareConfig(config)
    return new InitHandler(config)
  }

  static parseConfig(jsonString: string): Config<NetworkType> {
    return configJsonSchema.parse(JSON.parse(jsonString))
  }

  static jsonSchema() {
    const { $schema, ...rest } = z.toJSONSchema(configJsonSchemaRaw) as Record<string, unknown>
    const schema = { $schema, $id: CONFIG_SCHEMA_URL, title: 'Subsquid Pipes CLI configuration', ...rest }
    console.log(JSON.stringify(schema, null, 2))
  }
}

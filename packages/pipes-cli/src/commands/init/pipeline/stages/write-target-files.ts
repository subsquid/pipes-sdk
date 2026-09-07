import { buildTarget } from '../../builders/target-builder/index.js'
import type { InitStage } from '../types.js'

export const writeTargetFilesStage: InitStage = {
  id: 'write-target-files',
  label: 'Creating target files',
  run: async (ctx) => {
    // File generation only; the target's post-steps (e.g. `db:generate`) run in
    // the optional target-post-steps stage so their shell-out can fail without
    // discarding the project.
    const artifacts = buildTarget(ctx.config, { rpcUrl: ctx.rpcUrl })
    for (const file of artifacts.files) {
      if (file.preserveExisting) {
        ctx.projectWriter.createFileIfAbsent(file.path, file.content)
      } else {
        ctx.projectWriter.createFile(file.path, file.content)
      }
    }
  },
}

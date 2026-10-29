import fs from 'node:fs/promises'
import path from 'node:path'

/** A statement of a SQL file, with the file it came from. */
export type SqlStatement = { file: string; query: string }

export async function loadSqlFiles(directoryOrFile: string): Promise<SqlStatement[]> {
  let sqlFiles: string[] = []

  if (directoryOrFile.endsWith('.sql')) {
    sqlFiles = [directoryOrFile]
  } else {
    const files = await fs.readdir(directoryOrFile)
    sqlFiles = files.filter((file) => path.extname(file) === '.sql').map((file) => path.join(directoryOrFile, file))
  }

  const contents = await Promise.all(sqlFiles.map((file) => fs.readFile(file, 'utf-8')))

  return contents.flatMap((content, index) =>
    content
      .split(';')
      .filter((query) => query.trim().length > 0)
      .map((query) => ({ file: sqlFiles[index], query })),
  )
}

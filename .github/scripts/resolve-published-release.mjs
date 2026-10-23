import { appendFileSync, readFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'

export const publicationWorkflows = ['release.yml']
const versionTag = /^pipes-v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/

export async function resolvePublishedRelease({ eventName, event, repository, tag }, api) {
  if (eventName === 'workflow_run') {
    const run = event.workflow_run
    if (
      run?.event !== 'push' ||
      run.conclusion !== 'success' ||
      run.head_repository?.full_name !== repository ||
      !versionTag.test(run.head_branch ?? '')
    ) {
      return { ready: false }
    }
    tag = run.head_branch
  } else if (eventName !== 'workflow_dispatch') {
    throw new Error('Unsupported release reporting event')
  }
  if (!versionTag.test(tag ?? '')) throw new Error('An existing version tag is required')

  const prefix = `repos/${repository}`
  const commit = await api(`${prefix}/commits/${encodeURIComponent(`refs/tags/${tag}`)}`)
  if (!/^[0-9a-f]{40}$/.test(commit.sha ?? '')) throw new Error('Could not resolve the release commit')
  if (eventName === 'workflow_run' && commit.sha !== event.workflow_run.head_sha) {
    throw new Error('Release tag no longer matches the publication commit')
  }

  let publicationRun
  for (const workflow of publicationWorkflows) {
    const runs = await api(
      `${prefix}/actions/workflows/${workflow}/runs?event=push&head_sha=${commit.sha}&per_page=100`,
    )
    const latest = runs.workflow_runs
      .filter(
        (run) =>
          run.event === 'push' &&
          run.head_branch === tag &&
          run.head_sha === commit.sha &&
          run.head_repository?.full_name === repository,
      )
      .sort((a, b) => b.id - a.id)[0]
    if (latest?.status !== 'completed' || latest.conclusion !== 'success') {
      if (eventName === 'workflow_dispatch') throw new Error(`Publication is not complete: ${workflow}`)
      return { ready: false }
    }
    publicationRun = latest.id
  }

  const release = await api(`${prefix}/releases/tags/${encodeURIComponent(tag)}`)
  if (release.draft || !release.published_at || release.tag_name !== tag) {
    throw new Error('A published GitHub release is required')
  }
  const file = await api(`${prefix}/contents/packages/pipes/package.json?ref=${commit.sha}`)
  const manifest = JSON.parse(Buffer.from(file.content, 'base64').toString('utf8'))
  if (manifest.name !== '@subsquid/pipes' || manifest.version !== tag.slice('pipes-v'.length)) {
    throw new Error('The published source manifest does not match the version tag')
  }
  return { ready: true, tag, sha: commit.sha, runId: publicationRun }
}

async function main() {
  const repository = process.env.GITHUB_REPOSITORY
  if (!/^[\w.-]+\/[\w.-]+$/.test(repository ?? '')) throw new Error('Invalid repository')
  const event = JSON.parse(readFileSync(process.env.GITHUB_EVENT_PATH, 'utf8'))
  const result = await resolvePublishedRelease(
    { eventName: process.env.GITHUB_EVENT_NAME, event, repository, tag: process.env.RELEASE_TAG },
    async (path) => {
      const response = await fetch(`https://api.github.com/${path}`, {
        headers: {
          Authorization: `Bearer ${process.env.GH_TOKEN}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2022-11-28',
        },
        signal: AbortSignal.timeout(30_000),
      })
      if (!response.ok) throw new Error(`GitHub publication check failed: HTTP ${response.status}`)
      return response.json()
    },
  )
  appendFileSync(
    process.env.GITHUB_OUTPUT,
    `${Object.entries(result)
      .map(([key, value]) => `${key}=${value}`)
      .join('\n')}\n`,
  )
  console.log(result.ready ? 'Versioned publication verified.' : 'No fully published version to report yet.')
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error.message)
    process.exitCode = 1
  })
}

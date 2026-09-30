import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { test } from 'node:test'

const workflow = readFileSync(new URL('../workflows/release.yml', import.meta.url), 'utf8')
const section = workflow.split('      - name: Verify it landed\n')[1].split('\n  #')[0]
const script = section.split('        run: |\n')[1].split('\n').map(line => line.replace(/^          /, '')).join('\n')

function checkRegistry(mode) {
  const dir = mkdtempSync(join(tmpdir(), 'package-visibility-'))
  try {
    writeFileSync(join(dir, 'curl'), `#!/bin/sh
count=0
[ ! -f "$VISIBILITY_DIR/count" ] || count=$(cat "$VISIBILITY_DIR/count")
count=$((count + 1))
echo "$count" > "$VISIBILITY_DIR/count"
case "$VISIBILITY_MODE" in
  delayed) [ "$count" -gt 2 ] || exit 22 ;;
  unavailable) exit 22 ;;
  mismatch) echo '{"name":"@example/other","version":"1.2.3"}'; exit 0 ;;
esac
printf '{"name":"@example/package","version":"1.2.3"}'
`, { mode: 0o700 })
    writeFileSync(join(dir, 'sleep'), '#!/bin/sh\nexit 0\n', { mode: 0o700 })
    const result = spawnSync('bash', ['-eo', 'pipefail', '-c', script], {
      encoding: 'utf8', timeout: 10000,
      env: { ...process.env, PATH: `${dir}:${process.env.PATH}`, VISIBILITY_DIR: dir, VISIBILITY_MODE: mode,
        NPM_NAME: '@example/package', VERSION: '1.2.3' },
    })
    return { status: result.status, attempts: Number(readFileSync(join(dir, 'count'), 'utf8')) }
  } finally { rmSync(dir, { recursive: true, force: true }) }
}

test('waits through registry propagation before accepting the exact package version', () => {
  assert.deepEqual(checkRegistry('delayed'), { status: 0, attempts: 3 })
})

test('fails after bounded retries for an absent or mismatched package version', () => {
  for (const mode of ['unavailable', 'mismatch']) {
    assert.deepEqual(checkRegistry(mode), { status: 1, attempts: 40 })
  }
})

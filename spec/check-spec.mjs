#!/usr/bin/env node
/**
 * Spec ID and link cross-reference checker.
 *
 * Validates the banded ID system of spec/: every individually referenced ID must be
 * defined somewhere; range references (WP-10…WP-16) may span banded gaps by design,
 * but their band prefix must exist. Also validates every relative markdown link: the
 * target file must exist, and an anchor must match a heading there. Finally, checks that
 * docs/errors.md stays in sync with the codes the SDK actually throws — every thrown
 * E-code has a documented section, and every documented code is still thrown. Exit 1 on
 * any dangling reference, broken link, or errors.md drift.
 *
 * Run: node spec/check-spec.mjs
 */
import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const SPEC = dirname(fileURLToPath(import.meta.url))

const files = [
  ...readdirSync(SPEC)
    .filter((f) => f.endsWith('.md'))
    .map((f) => join(SPEC, f)),
  ...readdirSync(join(SPEC, 'decisions'))
    .filter((f) => f.endsWith('.md'))
    .map((f) => join(SPEC, 'decisions', f)),
]

const text = new Map(files.map((f) => [f, readFileSync(f, 'utf8')]))
const base = (f) => basename(f)

// The README explains the P-NAME convention with a placeholder token.
const WHITELIST = new Set(['P-NAME'])

const defined = new Set()
const definedByKind = new Map()
const defSite = new Map()

function addDef(id, file) {
  defined.add(id)
  const kind = id.match(/^(NG|[A-Z]+)/)[1] === 'NG' ? 'NG' : id.match(/^[A-Z]+/)[0]
  if (!definedByKind.has(kind)) definedByKind.set(kind, new Set())
  definedByKind.get(kind).add(id)
  if (!defSite.has(id)) defSite.set(id, base(file))
}

const BOLD_DEF = /\*\*((?:REQ|DEF|WP|RP|CN|INV|LIV|FM|RS|PF|SLI|HZ|OB|IB|CT|GAP)-\d+)\s+—/g

for (const [f, t] of text) {
  const b = base(f)
  for (const m of t.matchAll(BOLD_DEF)) addDef(m[1], f)
  // FM rows live in 09; module 16 owns the fallback band (FM-60…69) in the same table form.
  if (b === '09-failure-model.md' || b === '16-fallback.md')
    for (const m of t.matchAll(/^\|\s*(FM-\d+)\s*\|/gm)) addDef(m[1], f)
  if (b === '13-conformance-tdd.md') {
    for (const m of t.matchAll(/^\|\s*(CT-\d+)\s*\|/gm)) addDef(m[1], f)
    for (const m of t.matchAll(/^\|\s*(GAP-\d+)\s*\|/gm)) addDef(m[1], f)
  }
  if (b === '02-requirements.md') for (const m of t.matchAll(/^\|\s*(OQ-\d+)\s*\|/gm)) addDef(m[1], f)
  if (b === '11-performance.md') {
    for (const m of t.matchAll(/^\|\s*(W-[A-Z][A-Z-]*[A-Z])\s*\|/gm)) addDef(m[1], f)
    for (const m of t.matchAll(/\*\*(S[1-6])\*\*/g)) addDef(m[1], f)
  }
  if (b === '15-parameters.md') for (const m of t.matchAll(/^\|\s*(P-[A-Z][A-Z0-9-]*[A-Z0-9])\s*\|/gm)) addDef(m[1], f)
  if (b === '01-overview.md') for (const m of t.matchAll(/\*\*(NG\d|G\d)\s+—/g)) addDef(m[1], f)
  if (b === '03-data-model.md') for (const m of t.matchAll(/^\|\s*(T-[A-Z]+)\s*\|/gm)) addDef(m[1], f)
  if (b === '14-interface-binding.md') {
    for (const m of t.matchAll(/\bE\d{4}\b/g)) addDef(m[0], f)
    // E-code ranges in the registry define their members.
    for (const m of t.matchAll(/\bE(\d{4})\s*[–…-]\s*E(\d{4})\b/g)) {
      const [a, z] = [Number(m[1]), Number(m[2])]
      if (z - a < 60) for (let n = a; n <= z; n++) addDef(`E${String(n).padStart(4, '0')}`, f)
    }
  }
  if (b.startsWith('ADR-')) {
    const m = b.match(/^ADR-(\d+)-/)
    if (m) addDef(`ADR-${m[1]}`, f)
  }
}

const REF =
  /\b((?:REQ|DEF|WP|RP|CN|INV|LIV|FM|RS|PF|SLI|HZ|OB|IB|CT|GAP|ADR|OQ)-\d+|T-(?:INIT|BATCH|RELEASE|CHECKPOINT|FORK|STOP)|P-[A-Z][A-Z0-9-]*[A-Z0-9]|W-[A-Z][A-Z-]*[A-Z]|NG\d|G\d(?![\w-])|S[1-6](?![\w-])|E\d{4})\b/g
const RANGE =
  /((?:REQ|DEF|WP|RP|CN|INV|LIV|FM|RS|PF|SLI|HZ|OB|IB|CT|GAP)-)(\d+)\s*[…–]\s*(?:(?:REQ|DEF|WP|RP|CN|INV|LIV|FM|RS|PF|SLI|HZ|OB|IB|CT|GAP)-)?(\d+)/g

const individual = new Map() // id -> files (direct, non-range references)
const rangeRefs = []

for (const [f, t] of text) {
  const b = base(f)
  const rangeSpans = []
  for (const m of t.matchAll(RANGE)) {
    rangeSpans.push([m.index, m.index + m[0].length])
    rangeRefs.push({ prefix: m[1], from: Number(m[2]), to: Number(m[3]), file: b })
  }
  for (const m of t.matchAll(REF)) {
    const inRange = rangeSpans.some(([s, e]) => m.index >= s && m.index < e)
    if (inRange) continue
    if (!individual.has(m[1])) individual.set(m[1], new Set())
    individual.get(m[1]).add(b)
  }
}

const errors = []

for (const [id, sites] of [...individual].sort()) {
  if (WHITELIST.has(id)) continue
  if (!defined.has(id)) errors.push(`dangling reference: ${id} (in ${[...sites].join(', ')})`)
}

for (const r of rangeRefs) {
  const kind = r.prefix.replace(/-$/, '')
  if (!definedByKind.has(kind)) {
    errors.push(`range over unknown band: ${r.prefix}${r.from}…${r.to} (in ${r.file})`)
    continue
  }
  const members = definedByKind.get(kind)
  let any = false
  for (let n = r.from; n <= r.to; n++) if (members.has(`${kind}-${n}`)) any = true
  if (!any) errors.push(`range matches no defined ID: ${r.prefix}${r.from}…${r.to} (in ${r.file})`)
}

// GitHub heading-anchor slug: lowercase, punctuation dropped, spaces to hyphens.
const slug = (s) =>
  s
    .toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-')

const headingCache = new Map()
function headingSlugs(file) {
  if (!headingCache.has(file)) {
    let src = text.get(file)
    if (src === undefined) {
      try {
        src = readFileSync(file, 'utf8')
      } catch {
        src = ''
      }
    }
    headingCache.set(file, new Set([...src.matchAll(/^#{1,6}\s+(.+)$/gm)].map((m) => slug(m[1]))))
  }

  return headingCache.get(file)
}

let linkCount = 0
for (const [f, t] of text) {
  for (const m of t.matchAll(/\[[^\]]*\]\(([^)\s]+)\)/g)) {
    const url = m[1]
    if (/^(https?|mailto):/.test(url)) continue

    const [rel, anchor] = url.split('#')
    const target = rel ? resolve(dirname(f), rel) : f
    linkCount++

    if (!existsSync(target)) {
      errors.push(`broken link: ${url} (in ${base(f)})`)
      continue
    }
    if (anchor && !headingSlugs(target).has(anchor)) {
      errors.push(`broken anchor: ${url} (in ${base(f)})`)
    }
  }
}

// ─── docs/errors.md ↔ thrown-code sync ───────────────────────────────────────
// docs/errors.md is the user-facing registry every SDK error message links to. Authority
// is the code: the same error-class sources docs/errors.md names in its header comment.
// Every code thrown there must have a section, and every documented code must still be
// thrown — otherwise the reference drifts from reality (as E0003 once did).
const REPO = resolve(SPEC, '..')

// Discovered, not listed: a hardcoded roster fails open — a new target's errors.ts is
// simply not scanned, and the check passes while the reference drifts.
function errorSources(dir) {
  const found = []
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    if (e.isDirectory()) {
      if (e.name === 'node_modules' || e.name === 'dist') continue
      found.push(...errorSources(join(dir, e.name)))
    } else if (e.name === 'errors.ts') {
      found.push(join(dir, e.name))
    }
  }

  return found
}

const sources = errorSources(join(REPO, 'packages'))
if (sources.length === 0) {
  errors.push('errors.md sync: no errors.ts sources found under packages/')
}

const thrownCodes = new Set()
for (const p of sources) {
  for (const m of readFileSync(p, 'utf8').matchAll(/'(E\d{4})'/g)) thrownCodes.add(m[1])
}

const errorsDoc = join(REPO, 'docs/errors.md')
const documentedCodes = new Set()
if (!existsSync(errorsDoc)) {
  errors.push('errors.md sync: docs/errors.md not found')
} else {
  for (const m of readFileSync(errorsDoc, 'utf8').matchAll(/^#{2,4}\s+(E\d{4})\b/gm)) documentedCodes.add(m[1])
}

for (const c of [...thrownCodes].sort()) {
  if (!documentedCodes.has(c)) {
    errors.push(`errors.md missing ${c}: thrown in code but has no section in docs/errors.md`)
  }
}
for (const c of [...documentedCodes].sort()) {
  if (!thrownCodes.has(c)) {
    errors.push(`errors.md stale ${c}: documented but no source throws it`)
  }
}

let total = 0
const parts = []
for (const kind of [...definedByKind.keys()].sort()) {
  const n = definedByKind.get(kind).size
  total += n
  parts.push(`${kind}:${n}`)
}
console.log(`spec IDs defined: ${total} (${parts.join(' ')})`)
console.log(`individual references checked: ${individual.size}; range references: ${rangeRefs.length}`)
console.log(`relative links checked: ${linkCount}`)
console.log(`error codes: ${thrownCodes.size} thrown, ${documentedCodes.size} documented`)

if (errors.length) {
  console.error(`\n${errors.length} error(s):`)
  for (const e of errors) console.error(`  - ${e}`)
  process.exit(1)
}
console.log('0 dangling references, 0 broken links')

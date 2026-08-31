# Release notes template

For any package released from this repo — stable and prereleases alike
(prereleases can be leaner; see the "Prereleases" section in [SKILL.md](SKILL.md)).

**Scope notes to the package being released.** The three packages ship
separately, on separate tags. Never mix them: a CLI feature does not belong in
`@subsquid/pipes` notes, and a core streaming fix does not belong in
`@subsquid/pipes-ui` notes.

Two shapes — pick by version delta.

## Patch release (X.Y.Z → X.Y.Z+1)

Flat bullet list, no area sections.

```markdown
## <Headline>

<1-2 sentence lead — what the user notices.>

- Bullet 1
- Bullet 2
- Bullet 3

**Full Changelog**: https://github.com/subsquid-labs/pipes-sdk/compare/<PREV_TAG>...<TAG>
```

## Minor / major release (X.Y.Z → X.Y+1.0 or X+1.0.0)

Sectioned. Use only the sections that apply, and pick the section set that fits
the package — the groupings below are suggestions, not a required skeleton.

### `@subsquid/pipes`

```markdown
## <Headline>

<1-3 sentence lead — what changed for the user, not the implementation.>

### Highlights
- **Bold lede** — short explanation.

### Core
- Stream/pipeline behavior, forks, watermarks, cursor handling.

### Targets
- ClickHouse / Postgres (Drizzle) / Parquet / BigQuery / PubSub changes. Name the target.

### Portal client
- Query, caching, or portal-client API changes.

### Chains
- EVM / Solana / Hyperliquid / Bitcoin decoder or helper changes.

### API
- `<new export or option>` — short description.
- Breaking: `<what changed>` — migration note.

**Full Changelog**: https://github.com/subsquid-labs/pipes-sdk/compare/<PREV_TAG>...<TAG>
```

### `@subsquid/pipes-cli`

Group by what the user runs: `### Commands`, `### Templates / scaffolding`,
`### Config`, `### Breaking`.

Call out anything that changes generated project output — users regenerate and
diff against their working tree.

### `@subsquid/pipes-ui`

Group by surface: `### Dashboard`, `### Charts`, `### Performance`, `### Breaking`.

Screenshots are welcome here and nowhere else.

## Style rules

- **No emoji** — not in the title, headers, or bullets.
- **Never hard-wrap prose.** GitHub renders release bodies with GFM hard line
  breaks, so every newline inside a paragraph becomes a visible `<br>` — an
  80-column source wrap comes out as ragged breaks mid-sentence. One paragraph is
  one line, however long. (This does not apply to `.md` files in the repo, where
  single newlines collapse; it is specific to release/issue/PR bodies.)
- **No version in the title** — GitHub renders the tag separately.
- **Lead with user-visible impact**, not internal mechanism.
- **Bold the lede of each highlight bullet** so the page scans in 5 seconds.
- **Don't restate the commit message verbatim.** The commit body is the engineer's
  view; release notes are the user's.
- **Call out breaking changes explicitly** with a migration note — these are
  `1.0.0`-track packages and consumers pin on them.
- **Skip "Tests" / internal-only churn** unless the headline is *about* it.
- **End with the compare link.** Resolve `<PREV_TAG>` as the previous tag *for the
  same package* (`git tag --list '<prefix>-v*' --sort=-v:refname | head -2 | tail -1`),
  otherwise the parent commit SHA: `git rev-parse <TAG>^ | cut -c1-7`. Don't blindly
  write a tag that may not exist — early releases predate the tag scheme and the
  link would 404.
- **No install block.** Install instructions live in the README. Release notes are
  for *what changed*.

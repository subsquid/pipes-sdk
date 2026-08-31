---
name: release
description: Cut a release of any published package in this repo — @subsquid/pipes, @subsquid/pipes-cli, or @subsquid/pipes-ui. Bumps the version, tags, pushes, watches the Release workflow, and publishes curated GitHub release notes. Handles stable releases and alpha/beta/rc prereleases (betas ship on the npm `latest` tag until 1.0.0; alpha and rc get their own, all via Trusted Publishing). Use when the user asks to "release", "publish", "cut a version", "ship", "deploy", or "cut an alpha/beta" for any of these packages.
---

# Release

One procedure for all three published packages. Publishing is automated by
[release.yml](../../../.github/workflows/release.yml): you bump, tag and push;
CI builds, gates, publishes to npm and opens the GitHub release. Then you replace
the auto-generated notes with curated ones.

| package | tag prefix | directory | what actually ships |
|---|---|---|---|
| `@subsquid/pipes` | `pipes-v` | `packages/pipes` | the package root |
| `@subsquid/pipes-cli` | `pipes-cli-v` | `packages/pipes-cli` | the package root (`prepack` rewrites `workspace:`) |
| `@subsquid/pipes-ui` | `pipes-ui-v` | `packages/pipes-ui` | **`dist/`** — a generated, pruned manifest |

Publishing runs on **npm Trusted Publishing** (OIDC + provenance, no `NPM_TOKEN`).
The tagged commit is the source of truth: CI refuses to publish unless
`package.json` already carries the version in the tag.

## Preconditions

- `git status` clean on `main`, up to date with `origin/main`. The workflow builds
  and publishes the tagged commit, so `main` must already contain what you intend
  to ship.
- The user named a package and a target version. If either is missing, ask — do
  not guess. See [Choosing the next version](#choosing-the-next-version).
- A **trusted publisher** exists on npmjs.com for that package pointing at
  `subsquid-labs/pipes-sdk` → the workflow that will publish it. Without it the
  `publish` job 404s. See [First-time setup](#first-time-setup-trusted-publishing).
- **`@subsquid/pipes-cli` only:** the core version its `package.json` pins must
  already be on npm. `prepack` rewrites `workspace:*` to whatever
  `packages/pipes/package.json` says **locally**, so releasing the CLI against an
  unpublished core ships a dependency nobody can install. CI blocks this, but
  check first rather than burn a tag — **release core before the CLI.**

## Steps

### 1. Bump the version

Set the target version in `packages/<dir>/package.json` (see the table above).

CI compares the exact string — prerelease suffix included — against the tag and
refuses to publish on a mismatch.

### 2. Commit and tag

```sh
git add packages/<dir>/package.json
git commit -m "chore(release): <npm-name> <version>"
git tag <prefix>-v<version>
git push origin main
git push origin <prefix>-v<version>
```

Both pushes are required — the workflow triggers on the tag.

### 3. Watch the workflow

```sh
gh run watch --repo subsquid-labs/pipes-sdk --exit-status
```

Or tail the latest run for a specific workflow:

```sh
RUN_ID=$(gh run list --repo subsquid-labs/pipes-sdk --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$RUN_ID" --repo subsquid-labs/pipes-sdk --exit-status
```

Jobs: `resolve` → `check` → `publish` → `github-release`. If `publish` fails on
Trusted Publishing, the npm-side config is probably missing or points elsewhere —
surface the error, don't retry blindly.

### 4. Rewrite the release notes

Applies to **every** release, prereleases included. `github-release` creates the
release with `generate_release_notes: true`, but those auto-notes are unreliable:
they diff against the previous tag in the repo, which now interleaves three
prefixes, so they mix packages. With no previous tag at all, GitHub dumps the
entire history.

Replace them with curated highlights from [release-template.md](release-template.md).

**Scope notes to the package being released.** A `pipes-ui` redesign does not
belong in `@subsquid/pipes` notes, and vice versa. Source highlights from the
commits since that package's previous tag; pick user-visible changes rather than
restating every commit.

```sh
gh release edit <prefix>-v<version> --repo subsquid-labs/pipes-sdk --notes-file notes.md
```

Prefer `--notes-file` over a heredoc — it avoids escaping problems around code
fences.

### 5. Confirm

```sh
echo "https://github.com/subsquid-labs/pipes-sdk/releases/tag/<prefix>-v<version>"
npm view <npm-name> dist-tags
```

## Prereleases (alpha/beta/rc)

Cut one exactly like a stable release — just name a prerelease version
(`1.0.0-alpha.17`, `1.0.0-beta.5`, `1.0.0-rc.1`). No extra inputs; `resolve`
derives the channel from the identifier:

| version | npm dist-tag | GitHub |
|---|---|---|
| `X.Y.Z` | `@latest` | normal release |
| `…-beta.N` | `@latest` | normal release |
| `…-alpha.N` | `@alpha` | prerelease |
| `…-rc.N` | `@rc` | prerelease |
| anything else | `@next` | prerelease |

**beta is the shipping channel until 1.0.0 lands.** A beta is what a plain
`npm i <pkg>` serves, and its GitHub release is not flagged a preview — so cut one
only when it is fit to be the default. alpha and rc stay off `latest`; testers opt
into those with `npm i <pkg>@alpha`.

This is a temporary policy, and the rule keys off the identifier rather than the
version. **When a stable ships, revert the `beta` arm in `release.yml` to
`DIST_TAG=beta` / `PRERELEASE=true`** — otherwise a later `2.0.0-beta.1` displaces
stable `1.x` for everyone who just types `npm i`.

The "Latest release" slot on the repo page is separate, and still reserved for
`@subsquid/pipes` — a `pipes-ui` or `pipes-cli` release never claims it.

Alpha and rc notes get the same package-scoped curation as a stable cut, but can
be leaner (drop the lead paragraph, group only what changed, optionally add a
one-line "what to test"). Keep the install line minimal — a bare
`npm i <pkg>@alpha`. **Skip** prose about "published to the `@alpha` dist-tag /
`@latest` is unaffected": it's obvious and just noise. A beta ships as the default
version, so give it stable-grade notes — including breaking changes.

## Dry runs

`release.yml` takes a `dry_run` input. It runs `resolve`, the build, and every
gate, then skips the publish and the GitHub release:

```sh
gh workflow run release.yml --repo subsquid-labs/pipes-sdk \
  -f package=pipes-ui -f version=1.0.0-alpha.8 -f dry_run=true
```

Use it when changing the workflow, or before the first release of a package.
**It does not exercise the OIDC exchange** — it proves the build and gates, not
that npm will accept the token. The first real publish is the test of that.

## Choosing the next version

Numbering is manual — the user names it. Check what's already published first:

```sh
npm view <npm-name> dist-tags
npm view <npm-name> versions --json | tail -20
```

Increment within a channel (`1.0.0-alpha.16` → `1.0.0-alpha.17`), or move channels
when stabilizing: `…-alpha.N` → `…-beta.1` → `…-rc.1` → `1.0.0`.

The three packages version **independently**. Do not sync their numbers just
because they happen to be close.

## First-time setup (Trusted Publishing)

There is no `NPM_TOKEN`. Once per package, a maintainer with publish rights
configures a trusted publisher on npmjs.com:

- Package → **Settings** → **Trusted Publishing** → add a GitHub Actions publisher
- Repository: **`subsquid/pipes-sdk`** — the canonical name. The org was renamed;
  `subsquid-labs/pipes-sdk` still works as a redirect (and is what every
  `repository` field and `gh --repo` invocation in this repo still says), but
  OIDC claims carry the canonical name, so trusted publishing must match it.
- Workflow filename: `release.yml` — for all three packages

The workflow requests `id-token: write` and publishes with `--provenance`, so
releases carry a verifiable provenance attestation. If the config is missing, the
publish step fails with a 404/401 — that's the signal, not a transient error.

**This binds to the exact filename.** Renaming or moving `release.yml` breaks
publishing for every package until the npm config is updated to match.

A mismatch between the OIDC claim and the trusted publisher record surfaces as a
`404 Not Found - PUT` on an otherwise green run: the tarball builds, provenance
signs, and only the final PUT is rejected. Read it as "no trusted publisher
matches this token", and check all three fields — org, repo, workflow filename —
before touching anything else.

## Failure modes

- **Tag exists** — `git tag` fails. The release was already started, or a previous
  attempt half-finished. Check `gh release view <tag>` and `gh run list` before
  doing anything. Don't force-delete tags without confirming.
- **`Verify source manifest` fails** — `package.json` doesn't match the tag, or the
  directory in the workflow's package table doesn't exist (a rename landed without
  updating the table). Fix, commit, retag.
- **`Verify publish manifest` fails on `repository.url`** — `--provenance` refuses
  to publish without a `repository` field resolving to `subsquid-labs/pipes-sdk`.
  For `pipes-ui` this comes from the root manifest via `scripts/package.js`, which
  copies it into the generated `dist/package.json`.
- **`Verify packed tarball` finds `workspace:`** — `prepack` didn't rewrite the
  protocol. **Do not fall back to `npm publish`**: it doesn't rewrite `workspace:`
  at all, which is exactly how `pipes-cli@1.0.0-alpha.1` shipped broken and gave
  consumers `EUNSUPPORTEDPROTOCOL`. Fix `scripts/rewrite-workspace-deps.cjs`.
- **`@subsquid/pipes@<version> is not on npm`** — the CLI pins a core version that
  was never released. Release core first.
- **Publish fails on Trusted Publishing** — config missing or pointing at the wrong
  repo/workflow. Don't fall back to a manual `npm publish` with a token unless the
  user explicitly asks: that bypasses provenance.
- **`Multiple versions of pnpm specified`** — `pnpm/action-setup` must **not** pass
  a `version:`; the root `package.json` pins pnpm via `packageManager`. If a
  workflow drifted and re-added it, remove it.
- **An alpha or rc served to a plain `npm i`** — shouldn't happen; the dist-tag
  logic routes those off `latest`. (A beta on `latest` is the intended policy, not
  this bug.) If it did, inspect `npm view <pkg> dist-tags` and repoint with
  `npm dist-tag add <pkg>@<version> latest` (needs npm auth — manual, outside the
  OIDC workflow).

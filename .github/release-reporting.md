# Release reporting

`linear-release.yml` reports a completed versioned publication to Linear using
`LINEAR_ACCESS_KEY`. It runs after the publication workflow succeeds for a version
tag pushed to this repository. It checks the tag's commit and the latest matching
publication run before reporting. The version omits the tag prefix; the release
name preserves it. Retries target the same version.

The manual workflow accepts an existing published tag and defaults to a read-only
preview. It does not build, publish, or deploy. An optional `base_ref` bounds the
preview's commit scan. Arbitrary-ref builds and manual publication runs are not
used as automatic publication evidence; the reporter requires successful tag-push
publication evidence for the same tag and commit.

The reporter checks out full history at the verified commit. Only its final step
receives the pipeline key. The CLI is pinned and checksum-verified, and its output
is captured so private tracker content does not enter workflow logs or artifacts.

Run the offline regression tests with:

```sh
node --test .github/scripts/test-release-reporting.mjs
```

Only `pipes-v*` releases of `@subsquid/pipes` are included. Issue attribution is
filtered to `packages/pipes/**`; CLI and UI package releases are excluded.

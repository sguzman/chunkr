# Git-Cliff Guidance

This file documents the shared workflow for editing `cliff.toml` in this
template so the changelog hooks work reliably without guesswork.

## Purpose

- `cliff.toml` drives `git cliff --config cliff.toml --output CHANGELOG.md`,
  which is executed from the `cargo release` pre-release hook (`release.toml`).
- The config is already wired for Conventional Commits and the template we ship.
  The goal of this doc is to codify how to customize owner/repo metadata without
  rewriting the hook.

## Key Sections to Edit

1. **`[remote.github]`**
  - Populate `owner` and `repo` with your GitHub organization/user and
    repository name before running `git cliff`.
  - The template intentionally keeps these values blank and throws (
    `{{ throw(...) }}`) if they remain empty, so the command fails fast instead
    of generating links to a placeholder repo.

2. **`body` template**
  - `cliff.toml` already builds `repo_url` from `remote.github.owner`/ `repo`
    and uses it for commit links.
  - We rely on the documented Tera context (`remote.github`) instead of
    inventing `metadata()` helpers so the template remains compatible with the
    released `git-cliff` binary.

3. **`commit_preprocessors` link**
  - The regex in `cliff.toml` rewrites `(#123)` references to
    `https://github.com/OWNER/REPO/issues/123`. Update `OWNER/REPO` to match the
    same values you set under `[remote.github]`.

## Running Git-Cliff

```bash
git cliff --config cliff.toml --output CHANGELOG.md
```

If you edit the template, run the command manually to verify links render
correctly before using `cargo release`.

## Release Workflow Notes

- `cargo release patch --workspace --execute --no-publish --no-push` runs the
  pre-release hook that generates `CHANGELOG.md`.
- Since the hook reads `[remote.github]`, this document and the template must
  stay synchronized: keeping the placeholders or visiting the wrong sections is
  what caused the earlier cycle of failing releases.

## Troubleshooting

- If the hook dies with `Variable 'owner' not found in context`, it means the
  body template was using macros or variables that do not exist in the shipped
  version of `git-cliff`. Revert to the current template or follow the
  `remote.github` pattern in this doc.
- Persistent failures contacting `https://api.github.com` during `git cliff`
  usually indicate the release hook is running inside an environment without
  outbound DNS access. In that case, prefilling `[remote.github]` is harmless
  but `git cliff` cannot fetch metadata until the network works—or until you
  stub `remote.github.owner`/ `repo` and set `direct_remote = false` (not needed
  for the template).

## Keep This Doc Updated

If future changes aim to keep metadata external (e.g., environment variables,
workspace metadata, or cargo-release flags), document those alternatives here so
we never land back in the guessing cycle.

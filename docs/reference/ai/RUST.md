# AI Rules for This Rust Repo

These rules apply to any AI agent making changes in this repository.

## Required Workflow

### 1. Build Must Succeed (required)

After making changes, ensure the repo builds:

- Run: `cargo build`

If the build fails, fix the issues until it compiles.

### 2. SemVer Discipline (required)

Changes must respect the release policy in `docs/RELEASE.md`.

- Use Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `perf:`,
  `test:`, `chore:`, `build:`, `ci:`).
- Mark breaking changes with `!` (e.g., `feat!: ...`) and describe the break in
  the body.
- If behavior, config schemas, or API contracts change, call it out explicitly
  in the final response.
- Recommend a SemVer bump (major/minor/patch) in the final response; the user
  decides whether/when to apply it.

### 3. Provide a Commit Message Only (required)

After completing a coherent, working change:

- Generate an appropriate commit message (Conventional Commit format).
- Do not run git commands (add/commit/push); the user will do that manually.

### 4. Post-Change Checks Are Manual (required)

See `docs/ai/POST-CHANGES.md` for the manual checklist and script.

### 5. Refactor Oversized Rust Files into Modules (required)

If any Rust source file grows beyond 600 lines, refactor it into modules.

- Convert the file into a directory module:
  - Example: `handler.rs` becomes `handler/`
- Split the code into multiple files inside the new directory, organized by
  clear functional/domain boundaries.
  - Prefer cohesive modules with single responsibilities (e.g., parsing,
    validation, IO, DB, HTTP, types, errors, helpers).
- Add `handler/mod.rs` that re-exports and wires the modules so other code can
  keep importing through the parent module.
  - Keep public surface area intentional: export only what needs to be used
    externally.
- Preserve behavior and public API where possible:
  - Avoid churn in call sites unless there is a strong reason.
  - If imports/paths must change, update them consistently across the repo.
- After refactoring, re-run step 1 (build) to ensure everything still compiles.

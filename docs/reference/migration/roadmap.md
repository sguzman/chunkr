# Primary Migration Roadmap (target `v1.0.0`)

This roadmap outlines how to re-implement the referenced project in Rust while
preserving feature parity and honoring the original tests. The end goal is a
1-to-1 reproduction of every capability documented in
`docs/migration/structure.md`, guarded by the same (or improved) validation, and
released as `v1.0.0`.

## SemVer Target

- **Planned release**: `v1.0.0` once feature parity is established and tests
  pass.
- **Cross-cutting concerns** are prioritized in early phases so downstream code
  can depend on consistent foundations.
- **Tests and validation** are ported alongside each feature rather than tacked
  on at the end; consider every test mentioned in the reference as mandatory
  verification for the Rust equivalents.

## Phase 0 – Shared Foundations (cross-Cutting Base)

1. **Workspace and toolchain**
  - Initialize the Rust workspace/crate structure.
  - Wire up shared tooling (`tokio`, `tracing`, `serde`, `thiserror`, etc.) to
    match the preferred library guidance.
  - Configure formatting, linting, and CI scaffolding.

2. **Configuration & secrets**
  - Port the reference config (TOML/JSON) into `config`/ `serde` structs with
    `toml` parsing.
  - Implement a centralized config loader with layered overrides, matching env
    vars or files the original project consumed.

3. **Logging and observability**
  - Establish structured logging via `tracing`, aligning spans with the existing
    runtime flow.
  - Optionally mirror the reference’s metrics/tracing points for parity.

4. **Error handling & validation**
  - Introduce error enums (`thiserror`) covering shared failure cases.
  - Wire `jsonschema` or other validation logic if the source project relied on
    schema validation for payloads.

5. **Runtime orchestration**
  - Embed the async runtime (`tokio`) strategy used by the reference (e.g.,
    single-threaded vs multi-threaded).
  - Build base utilities for starting the app, managing graceful shutdown, and
    injecting services.

## Phase 1 – Core Feature Translation (1-to-1 Behavior)

1. **Feature modules**
  - For each feature in the reference project, create a corresponding Rust
    module or crate. Prioritize modules that serve as dependencies for others
    (e.g., data ingestion, HTTP handling, database access).
  - Document the goal, inputs, and outputs for each module based on the
    reference structure.

2. **Dependency matching**
  - Replace the reference libraries with the preferred Rust counterparts (e.g.,
    `reqwest` for HTTP calls, `sqlx` for database access).
  - Ensure any custom logic (batching, caching, scheduling) is mirrored in the
    Rust implementation before moving to the next module.

3. **Testing parity**
  - For every feature, port the corresponding tests (unit/integration) from the
    reference or rewrite them in Rust with the same assertions.
  - If the reference lacks tests for a capability, write new ones guided by the
    documented behavior to ensure ongoing reliability.

4. **Validation gates**
  - After each feature translation, run the accepted test suite plus any schema
    checks (`jsonschema`) to confirm feature behavior.
  - Keep a running changelog of tests added or adapted as part of this migration
    for future traceability.

## Phase 2 – Integration and Polish

1. **End-to-end scenarios**
  - Build integration tests that exercise the combined behavior of the major
    features, mirroring the reference’s workflows (e.g., request → process →
    store → respond).
  - Use fixtures or sample data captured from the reference project in `tmp/` to
    assert equivalence.

2. **Runtime readiness**
  - Confirm CLI/daemon entry points behave like the original (same flags,
    outputs).
  - Validate config overrides, environment handling, and logging alignments
    before tagging `v1.0.0`.

3. **Documentation & release prep**
  - Update README and docs to signal the migration is complete, referencing the
    original project and highlighting feature parity.
  - Re-run `cargo build`/ `cargo test`/ `cliff` in the release workflow to prove
    readiness.

Once Phase 2 checks succeed, tag and publish `v1.0.0` (per `release.toml`) with
all the migrated functionality documented and verified.

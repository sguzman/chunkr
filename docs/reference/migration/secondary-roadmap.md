# Secondary/optional Roadmap

After releasing `v1.0.0`, use this space to capture aspirational enhancements
that go beyond the original project’s scope. These should not block the primary
port but provide a structured queue for future iterations.

## Potential Additions

1. **Advanced tooling**
  - Build CLI helpers (e.g., `projectname doctor`, `projectname format`) that
    streamline developer workflows.
  - Consider adding a `just` task or script that mirrors existing shell helpers
    the reference project may have used.

2. **Observability enhancements**
  - Introduce richer `tracing` spans and metrics dashboards (Prometheus
    exporters, structured log exporters) if the original app only had simple
    logging.
  - Add configuration toggles for telemetry sampling or sink selection.

3. **Platform integrations**
  - If the reference app interacts with external services (e.g., S3, message
    brokers), think about building adapters or plugins so the Rust port can swap
    providers more easily.

4. **Documentation & guides**
  - Expand on the README/brand docs by adding architecture diagrams, onboarding
    checklists, or sample data for the Rust port.
  - Capture any previously undocumented behaviors or heuristics uncovered while
    translating.

5. **Performance/assertion tests**
  - Write benchmarks or smoke tests that prove the Rust version meets or exceeds
    the original performance profile.
  - Bolster the test suite with regression cases derived from real-world usage.

## Process Note

Treat these ideas as a backlog you can reorder once `v1.0.0` is complete.
Document the required dependencies, efforts, and acceptance criteria for each
item so future work remains predictable and tied to this template’s tooling.
Keep this file updated as you uncover new opportunities during or after the
migration.

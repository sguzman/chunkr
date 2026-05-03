# Chunkr

Chunkr is a Rust CLI for extracting text and metadata from Calibre libraries, cleaning and chunking that text, and inserting the results into search/vector stores.

## Intent

Centralize the text-ingestion workflow for personal or research-scale ebook collections so extraction, cleanup, chunking, deduplication, and indexing can all be driven from one config.

## Ambition

The command surface and integration points show a clear ambition to be an end-to-end document-ingestion pipeline for retrieval systems rather than just a chunking helper.

## Current Status

The project already has multiple subcommands, config files, tests, examples, and documentation for the main pipeline stages. It looks actively used for real ingestion workflows.

## Core Capabilities Or Focus Areas

- Extract text and metadata from Calibre libraries.
- Chunk and clean extracted text.
- Insert prepared chunks into Qdrant and Quickwit workflows.
- Inspect and manage duplicates.
- Drive the whole process from TOML configuration.

## Project Layout

- `docs/`: project documentation, reference material, and roadmap notes.
- `examples/`: sample inputs, example configs, or demonstration workflows.
- `new-project/`: scaffold or template material for starting new chunkr-based projects.
- `src/`: Rust source for the main crate or application entrypoint.
- `tests/`: automated tests, fixtures, or parity scenarios.
- `Cargo.toml`: crate or workspace manifest and the first place to check for package structure.

## Setup And Requirements

- Rust toolchain.
- A Calibre library or compatible input corpus.
- Configured downstream stores if using insert/index stages.

## Build / Run / Test Commands

```bash
cargo build
cargo test
cargo run -- --help
```

## Notes, Limitations, Or Known Gaps

- Different subcommands have different external requirements, especially for insertion/indexing targets.
- This project sits in the middle of a larger ingestion pipeline, so config discipline matters.

## Next Steps Or Roadmap Hints

- Keep extraction, chunking, and insertion contracts explicit as the pipeline grows.
- Add more end-to-end fixtures around deduplication and store insertion if this becomes a shared dependency.

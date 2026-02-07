---
title: main.rs Summary
type: note
permalink: main.rs-summary
---

**Purpose**: CLI entry point for Chunkr's text processing pipeline
**Key functionality**:

- Parses command-line arguments using Clap
- Routes execution to subcommands (extract, chunk, insert, dups)
- Loads configuration and initializes logging before executing commands
  **Critical components**:
- `Cli`: Command structure with config path and subcommand options
- `Commands` enum: Dispatches to specific workflow modules
- Main async function: Coordinates command execution flow

---
title: config.rs Summary
type: note
permalink: config.rs-summary
---

**Purpose**: Central configuration for the Chunkr tool **Key functionality**:

- Defines TOML configuration structures (logging, paths, extraction, chunking,
  insertion)
- Provides default values for all configuration parameters
- Loads config files via `load()` function **Critical components**:
- `Config`: Main struct aggregating all settings
- `PathsConfig`: File system path configurations
- `ChunkConfig`: Text chunking parameters (size, overlap)
- `InsertConfig`: Database connection details (Qdrant/Quickwit/embeddings)

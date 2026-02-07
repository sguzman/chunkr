---
title: lib.rs Summary
type: note
permalink: lib.rs-summary
---

**Purpose**: Main crate entry point that exports all submodules for Chunkr functionality
**Key functionality**:
- Provides module-level organization for the project's components
- Acts as a namespace for all core modules (chunk processing, config, duplicates detection)
**Critical components**:
- `chunk`: Text chunking processor
- `config`: Project configuration structures
- `dups`: Duplicate file detection
- `extract`: Document extraction from e-books
- `insert`: Vector database integration
---
title: logging.rs Summary
type: note
permalink: logging.rs-summary
---

**Purpose**: Manages structured logging with color-coded output based on
operation types and file chunks. **Key functionality**:

- Initializes tracing subscriber with custom ANSI color prefixes
- Generates colored log entries for different operations (Ollama, Qdrant,
  Quickwit)
- Provides dynamic color coding using hash-based palette **Critical components**:
- `ColorPrefixFormat`: Custom event formatter for colored logs
- `LogOp`: Enum defining operation types (Ollama/Qdrant/Quickwit)
- `color_prefix()`: Generates ANSI color-coded prefixes for log entries

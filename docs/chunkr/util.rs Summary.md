---
title: util.rs Summary
type: note
permalink: util.rs-summary
---

**Purpose**: Provides utility functions for text processing and path
manipulation within the Chunkr pipeline **Key functionality**:

- Converts strings to lowercase alphanumeric identifiers with underscores (
  `slugify`)
- Dynamically generates file paths using template layout strings (
  `apply_layout`)
- Safely replaces file extensions without modifying directory structure (
  `replace_extension`) **Critical components**:
- `slugify()`: Generates clean, URL-safe titles for output files
- `apply_layout()`: Builds output paths from templates (e.g., `{format}` →
  "epub")
- `replace_extension()`: Standardizes file extensions for consistent processing

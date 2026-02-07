---
title: dups.rs Summary
type: note
permalink: dups.rs-summary
---

**Purpose**: Identifies duplicate files in a library (e.g., Calibre e-books) using content-based hashing
**Key functionality**:
- Parallel file scanning with BLAKE3 hashing (chunked reading)
- Candidate filtering by extension, size, and sidecar files
- Grouping files by hash + byte count to find duplicates
- Outputs human-readable text or JSON reports
**Critical flow**: Scan directory → Filter candidates → Hash files → Group duplicates → Format output
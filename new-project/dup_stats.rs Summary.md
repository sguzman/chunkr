---
title: dup_stats.rs Summary
type: note
permalink: dup-stats.rs-summary
---

**Purpose**: Analyzes duplication statistics from duplicate reports to quantify redundant data
**Key functionality**:
- Parses JSON-formatted duplicate reports into structured groups
- Calculates total duplicated bytes, file counts, and top duplicate buckets
- Outputs human-readable summaries (top 5 duplicates) or machine-readable JSON
**Critical flow**: Read report → Parse groups → Calculate stats → Format output based on mode
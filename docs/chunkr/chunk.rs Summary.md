---
title: chunk.rs Summary
type: note
permalink: chunk.rs-summary
---

**Purpose**: Processes raw CFR .txt files into structured JSONL chunks with
metadata **Key functionality**:

- Normalizes text (Unicode, whitespace)
- Splits text into paragraphs
- Builds configurable-size chunks
- Writes output to JSONL files
- Handles metadata from source documents **Critical flow**: Reads → Normalizes →
  Paragraphs → Chunks → JSONL output

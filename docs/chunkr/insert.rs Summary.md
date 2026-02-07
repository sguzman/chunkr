---
title: insert.rs Summary
type: note
permalink: insert.rs-summary
---

**Purpose**: Integrates processed text chunks into vector databases (Qdrant &
Quickwit) with embedded semantic search **Key functionality**:

- Processes JSONL chunk files in parallel using semaphores
- Generates embeddings via Ollama API with caching to avoid redundant requests
- Upserts data into Qdrant collections with vector distances
- Ingests documents into Quickwit index with batched processing
  **Critical flow**: Read chunk files → Process batches → Generate embeddings
  (with cache) → Upsert to Qdrant → Ingest into Quickwit

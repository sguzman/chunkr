# Chunkr

Chunkr is a CLI for extracting text + metadata from Calibre libraries, cleaning and chunking that text, and inserting the resulting chunks into Qdrant and Quickwit. Configuration is centralized in a single TOML file so that all properties, policies, and paths are controlled in one place.

This README formalizes the intended interface and configuration for the project.

## Goals

- Deterministic, idempotent extraction from Calibre (skip already-processed files).
- Robust handling for large EPUB/PDF files (chunk during extraction to avoid memory spikes).
- Clean, normalized text and metadata-enriched chunks for downstream search and embeddings.
- Straightforward insertion into Qdrant + Quickwit with sensible defaults.
- Extensive logging for long-running pipelines.

## Commands

### `extract`

Extracts plaintext and metadata from a Calibre library into a target folder.

Key behaviors:
- Points at a Calibre library root (e.g. `/drive/calibre/en_nonfiction`).
- Supports EPUB and PDF (for now).
- Idempotent: skips items already extracted unless configured otherwise.
- EPUB extraction should follow the approach in `tmp/epub.fish`.
- PDF extraction should attempt text-first, and fall back to OCR via Docling (see `tmp/pdf.fish`).
- Large files are segmented during extraction using chapter boundaries when available.
- All extraction and segmentation policy is configured in TOML.

### `chunk`

Cleans, normalizes, and chunks a large corpus of text files into chunked JSONL (or similar) with metadata.

Key behaviors:
- Uses the chunking strategies and policies defined in config.
- Mirrors the “oxbed” ingestion/chunking approach (see `tmp/oxbed`).
- Paragraph-aware segmentation: pack small paragraphs together, split oversized paragraphs.
- Emits normalized text + metadata for downstream insertion.

### `insert`

Inserts chunked text into Qdrant and Quickwit.

Key behaviors:
- Qdrant: vector store for embeddings, uses Ollama for embeddings.
- Quickwit: text search index for fast keyword queries.
- Connection details and collection/index policies are configured in TOML.
- Defaults are aligned with `tmp/docker-compose-quickwit.yaml` and `tmp/docker-compose-ollama.yaml`.

## Configuration

All properties, policies, and paths are set in a single TOML config file. Example:

```toml
[logging]
level = "info"

[paths]
calibre_root = "/drive/calibre/en_nonfiction"
extract_root = "/drive/books/plaintext/books"
chunk_root = "/drive/books/plaintext/chunked"
state_dir = "/drive/books/.chunkr-state"

[extract]
extensions = ["epub", "pdf"]
skip_existing = true
write_metadata = true
output_layout = "{format}/{title_slug}.txt"
metadata_layout = "{format}/{title_slug}.json"

[extract.epub]
backend = "pandoc"
toc_depth = 3
chapter_split = true
max_chapter_bytes = 2_000_000
max_file_bytes = 20_000_000

[extract.pdf]
backend = "docling"
text_first = true
text_min_chars = 40
text_sample_pages = 3
ocr_fallback = true
ocr_lang = "eng"
docling_device = "cuda"
docling_pipeline = "standard"
docling_pdf_backend = "dlparse_v4"
docling_threads = 16
docling_tables = true
docling_table_mode = "accurate"

[chunk]
normalize_unicode = true
collapse_whitespace = true
strip_headers = true
min_paragraph_chars = 120
max_paragraph_chars = 2_400
target_chunk_chars = 1_800
max_chunk_chars = 2_600
chunk_overlap_chars = 200
emit_jsonl = true

[chunk.metadata]
include_source_path = true
include_calibre_id = true
include_title = true
include_authors = true
include_published = true
include_language = true

[insert]
batch_size = 128
retry_max = 5
retry_backoff_ms = 500

[insert.qdrant]
url = "http://127.0.0.1:6333"
collection = "books"
distance = "Cosine"
vector_size = 384

[insert.quickwit]
url = "http://127.0.0.1:7280"
index_id = "books"
commit_timeout_seconds = 30

[insert.embeddings]
provider = "ollama"
base_url = "http://127.0.0.1:11434"
model = "qllama/bge-small-en-v1.5:latest"
request_timeout_seconds = 120
max_concurrency = 4
```

Notes:
- The example values align with `tmp/docker-compose-quickwit.yaml` and `tmp/docker-compose-ollama.yaml`.
- All extraction and chunking policy must be driven from this file (no hard-coded defaults).
- Use the config file to set max sizes/limits to prevent large EPUB/PDF files from exhausting memory or GPU.

## Example usage

```bash
# Extract from Calibre into /drive/books/plaintext/books
chunkr extract --config /path/to/config.toml

# Chunk all extracted files into chunked JSONL
chunkr chunk --config /path/to/config.toml

# Insert into Qdrant + Quickwit
chunkr insert --config /path/to/config.toml
```

## Dependencies and external tools

- EPUB extraction uses Pandoc.
- PDF extraction uses Docling; OCR falls back to Tesseract via Docling.
- Qdrant and Quickwit are expected to be running (Docker Compose configs in `tmp/`).
- Ollama serves embeddings at the configured host/port.

## Logging

All commands should emit extensive structured logs (start/end, counts, skips, timing, errors). Configure log level via `[logging]`.


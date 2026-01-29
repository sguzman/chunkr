use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub logging: LoggingConfig,
    pub paths: PathsConfig,
    pub extract: ExtractConfig,
    pub chunk: ChunkConfig,
    pub insert: InsertConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub calibre_root: PathBuf,
    pub extract_root: PathBuf,
    pub chunk_root: PathBuf,
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractConfig {
    pub extensions: Vec<String>,
    pub skip_existing: bool,
    pub write_metadata: bool,
    pub output_layout: String,
    pub metadata_layout: String,
    pub epub: ExtractEpubConfig,
    pub pdf: ExtractPdfConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractEpubConfig {
    pub backend: String,
    pub pandoc_bin: String,
    pub toc_depth: u8,
    pub chapter_split: bool,
    pub max_chapter_bytes: u64,
    pub max_file_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractPdfConfig {
    pub backend: String,
    pub pdffonts_bin: String,
    pub pdftotext_bin: String,
    pub docling_bin: String,
    pub docling_script: String,
    pub text_first: bool,
    pub text_min_chars: usize,
    pub text_sample_pages: usize,
    pub ocr_fallback: bool,
    pub ocr_lang: String,
    pub ocr_engine: String,
    pub docling_device: String,
    pub docling_pipeline: String,
    pub docling_pdf_backend: String,
    pub docling_threads: usize,
    pub docling_tables: bool,
    pub docling_table_mode: String,
    pub max_file_bytes: u64,
    pub skip_oversize: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkConfig {
    pub normalize_unicode: bool,
    pub collapse_whitespace: bool,
    pub strip_headers: bool,
    pub min_paragraph_chars: usize,
    pub max_paragraph_chars: usize,
    pub target_chunk_chars: usize,
    pub max_chunk_chars: usize,
    pub chunk_overlap_chars: usize,
    pub emit_jsonl: bool,
    pub metadata: ChunkMetadataConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkMetadataConfig {
    pub include_source_path: bool,
    pub include_calibre_id: bool,
    pub include_title: bool,
    pub include_authors: bool,
    pub include_published: bool,
    pub include_language: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertConfig {
    pub batch_size: usize,
    pub retry_max: usize,
    pub retry_backoff_ms: u64,
    pub qdrant: InsertQdrantConfig,
    pub quickwit: InsertQuickwitConfig,
    pub embeddings: InsertEmbeddingsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertQdrantConfig {
    pub url: String,
    pub collection: String,
    pub distance: String,
    pub vector_size: usize,
    pub create_collection: bool,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertQuickwitConfig {
    pub url: String,
    pub index_id: String,
    pub commit_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertEmbeddingsConfig {
    pub provider: String,
    pub base_url: String,
    pub model: String,
    pub request_timeout_seconds: u64,
    pub max_concurrency: usize,
    pub max_input_chars: usize,
}

pub fn load(path: &PathBuf) -> anyhow::Result<Config> {
    let raw = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&raw)?;
    Ok(config)
}

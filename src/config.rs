use std::path::PathBuf;

use clap::ValueEnum;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
  pub logging: LoggingConfig,
  pub paths:   PathsConfig,
  pub extract: ExtractConfig,
  pub chunk:   ChunkConfig,
  pub insert:  InsertConfig,
  #[serde(default)]
  pub dups:    DupsConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
  pub level: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
  pub calibre_root:     PathBuf,
  pub extract_root:     PathBuf,
  pub chunk_root:       PathBuf,
  pub state_dir:        PathBuf,
  pub examples_cfr_dir: Option<PathBuf>
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractConfig {
  pub extensions:      Vec<String>,
  pub skip_existing:   bool,
  pub write_metadata:  bool,
  pub output_layout:   String,
  pub metadata_layout: String,
  pub epub: ExtractEpubConfig,
  pub pdf:             ExtractPdfConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractEpubConfig {
  pub backend:           String,
  pub pandoc_bin:        String,
  pub toc_depth:         u8,
  pub chapter_split:     bool,
  pub max_chapter_bytes: u64,
  pub max_file_bytes:    u64,
  pub join_parts:        bool,
  pub keep_parts:        bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractPdfConfig {
  pub backend:                  String,
  pub pdffonts_bin:             String,
  pub pdftotext_bin:            String,
  pub pdfinfo_bin:              String,
  pub docling_bin:              String,
  pub docling_script:           String,
  pub text_first:               bool,
  pub text_good_min_chars:      usize,
  pub text_low_min_chars:       usize,
  pub text_alpha_ratio_min:     f32,
  pub text_sample_pages:        usize,
  pub ocr_fallback:             bool,
  pub ocr_lang:                 String,
  pub ocr_engine:               String,
  pub docling_device:           String,
  pub docling_pipeline:         String,
  pub docling_pdf_backend:      String,
  pub docling_threads:          usize,
  pub docling_tables:           bool,
  pub docling_table_mode:       String,
  pub low_quality_use_ocr:      bool,
  pub low_quality_force_ocr:    bool,
  pub low_quality_tables:       bool,
  pub low_quality_table_mode:   String,
  pub scan_force_ocr:           bool,
  pub scan_tables:              bool,
  pub scan_table_mode:          String,
  pub page_batch_size:          usize,
  pub document_timeout_seconds: u64,
  pub max_pages_per_pass:       usize,
  pub split_text_extraction:    bool,
  pub max_file_bytes:           u64,
  pub skip_oversize:            bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkConfig {
  pub normalize_unicode:   bool,
  pub collapse_whitespace: bool,
  pub strip_headers:       bool,
  pub min_paragraph_chars: usize,
  pub max_paragraph_chars: usize,
  pub target_chunk_chars:  usize,
  pub max_chunk_chars:     usize,
  pub chunk_overlap_chars: usize,
  pub emit_jsonl:          bool,
  pub metadata: ChunkMetadataConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkMetadataConfig {
  pub include_source_path: bool,
  pub include_calibre_id:  bool,
  pub include_title:       bool,
  pub include_authors:     bool,
  pub include_published:   bool,
  pub include_language:    bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertConfig {
  pub batch_size:         usize,
  pub retry_max:          usize,
  pub retry_backoff_ms:   u64,
  pub max_parallel_files: usize,
  pub qdrant: InsertQdrantConfig,
  pub quickwit: InsertQuickwitConfig,
  pub embeddings:
    InsertEmbeddingsConfig
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertQdrantConfig {
  pub url:               String,
  pub collection:        String,
  pub distance:          String,
  pub vector_size:       usize,
  pub create_collection: bool,
  pub api_key:           Option<String>,
  pub wait:              bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertQuickwitConfig {
  pub url:                    String,
  pub index_id:               String,
  pub commit_timeout_seconds: u64,
  pub commit_mode:            String,
  pub commit_at_end:          bool
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertEmbeddingsConfig {
  pub provider:                String,
  pub base_url:                String,
  pub model:                   String,
  pub request_timeout_seconds: u64,
  pub max_concurrency:         usize,
  pub max_input_chars:         usize,
  pub global_max_concurrency:  usize,
  pub request_batch_size:      usize,
  pub cache_max_entries:       usize
}

#[derive(Debug, Clone, Deserialize)]
pub struct DupsConfig {
  pub output: DupsOutputFormat,
  pub out:              Option<PathBuf>,
  pub ext:              Vec<String>,
  pub follow_symlinks:  bool,
  pub threads:          usize,
  pub min_size:         u64,
  pub include_sidecars: bool
}

impl Default for DupsConfig {
  fn default() -> Self {
    Self {
      output:
        DupsOutputFormat::Json,
      out:              None,
      ext:              vec![
        "epub", "mobi", "azw3", "pdf",
        "djvu",
      ]
      .into_iter()
      .map(String::from)
      .collect(),
      follow_symlinks:  false,
      threads:          8,
      min_size:         1024,
      include_sidecars: false
    }
  }
}

#[derive(
  Copy,
  Clone,
  Debug,
  Deserialize,
  PartialEq,
  Eq,
  ValueEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum DupsOutputFormat {
  Text,
  Json
}

pub fn load(
  path: &PathBuf
) -> anyhow::Result<Config> {
  let raw =
    std::fs::read_to_string(path)?;
  let config: Config =
    toml::from_str(&raw)?;
  Ok(config)
}

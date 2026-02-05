use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::{
  Path,
  PathBuf
};
use std::process::Command;

use anyhow::{
  Context,
  anyhow
};
use chrono::Utc;
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::Serialize;
use tracing::{
  debug,
  info,
  warn
};
use walkdir::WalkDir;

use crate::config::{
  Config,
  ExtractEpubConfig,
  ExtractPdfConfig
};
use crate::util::{
  apply_layout,
  replace_extension,
  slugify
};

#[derive(
  Debug, Default, Serialize, Clone,
)]
struct ExtractedMetadata {
  source_path:  String,
  format:       String,
  title:        Option<String>,
  authors:      Vec<String>,
  language:     Option<String>,
  published:    Option<String>,
  identifiers:  Vec<String>,
  calibre_id:   Option<String>,
  extracted_at: String
}

pub fn run(
  config: &Config
) -> anyhow::Result<()> {
  let exts = config
    .extract
    .extensions
    .iter()
    .map(|s| s.to_ascii_lowercase())
    .collect::<Vec<_>>();
  let mut total = 0usize;
  let mut skipped = 0usize;
  for entry in WalkDir::new(
    &config.paths.calibre_root
  )
  .into_iter()
  .filter_map(|e| e.ok())
  .filter(|e| e.file_type().is_file())
  {
    let path = entry.path();
    let ext = path
      .extension()
      .and_then(OsStr::to_str)
      .unwrap_or("")
      .to_ascii_lowercase();
    if !exts.contains(&ext) {
      continue;
    }
    total += 1;
    if process_one(path, &ext, config)
      .unwrap_or(false)
    {
      skipped += 1;
    }
  }
  info!(
    total,
    skipped, "extract complete"
  );
  Ok(())
}

fn process_one(
  path: &Path,
  format: &str,
  config: &Config
) -> anyhow::Result<bool> {
  let metadata =
    read_metadata(path, format);
  let title_seed = metadata
    .title
    .clone()
    .unwrap_or_else(|| {
      path
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("book")
        .to_string()
    });
  let title_slug = slugify(&title_seed);
  let rel_output = apply_layout(
    &config.extract.output_layout,
    format,
    &title_slug
  );
  let rel_meta = apply_layout(
    &config.extract.metadata_layout,
    format,
    &title_slug
  );
  let output_path = config
    .paths
    .extract_root
    .join(rel_output);
  let metadata_path = config
    .paths
    .extract_root
    .join(rel_meta);

  if config.extract.skip_existing
    && output_path.exists()
  {
    debug!(path = %path.display(), "skip existing");
    return Ok(true);
  }

  if let Some(parent) =
    output_path.parent()
  {
    fs::create_dir_all(parent)?;
  }
  if let Some(parent) =
    metadata_path.parent()
  {
    fs::create_dir_all(parent)?;
  }

  let outputs = match format {
    | "epub" => {
      extract_epub(
        path,
        &output_path,
        &config.extract.epub
      )?
    }
    | "pdf" => {
      extract_pdf(
        path,
        &output_path,
        &config.extract.pdf
      )?
    }
    | _ => {
      return Err(anyhow!(
        "unsupported format: {}",
        format
      ))
    }
  };

  if config.extract.write_metadata {
    for out_path in outputs {
      let mut meta = metadata.clone();
      meta.source_path =
        path.display().to_string();
      meta.format = format.to_string();
      meta.extracted_at =
        Utc::now().to_rfc3339();
      let meta_path = replace_extension(
        &out_path, "json"
      );
      write_metadata(
        &meta_path, &meta
      )?;
    }
  }

  Ok(false)
}

fn extract_epub(
  input: &Path,
  output: &Path,
  cfg: &ExtractEpubConfig
) -> anyhow::Result<Vec<PathBuf>> {
  if cfg.backend != "pandoc" {
    return Err(anyhow!(
      "unsupported epub backend: {}",
      cfg.backend
    ));
  }
  info!(path = %input.display(), "extract epub");
  let status =
    Command::new(&cfg.pandoc_bin)
      .arg("--from")
      .arg("epub")
      .arg("--to")
      .arg("markdown")
      .arg("--verbose")
      .arg("--toc")
      .arg("--toc-depth")
      .arg(cfg.toc_depth.to_string())
      .arg("--output")
      .arg(output)
      .arg("--")
      .arg(input)
      .status()
      .with_context(|| {
        format!(
          "pandoc failed for {}",
          input.display()
        )
      })?;
  if !status.success() {
    return Err(anyhow!(
      "pandoc exit status: {}",
      status
    ));
  }

  let meta = fs::metadata(output)?;
  if cfg.chapter_split
    && meta.len() > cfg.max_file_bytes
  {
    info!(
      bytes = meta.len(),
      "split epub output"
    );
    let parts = split_markdown_file(
      output,
      cfg.max_chapter_bytes
    )?;
    if cfg.join_parts {
      join_parts_into(output, &parts)?;
      if !cfg.keep_parts {
        for part in &parts {
          let _ = fs::remove_file(part);
        }
      }
      return Ok(vec![
        output.to_path_buf(),
      ]);
    }
    fs::remove_file(output).ok();
    return Ok(parts);
  }

  Ok(vec![output.to_path_buf()])
}

fn extract_pdf(
  input: &Path,
  output: &Path,
  cfg: &ExtractPdfConfig
) -> anyhow::Result<Vec<PathBuf>> {
  if cfg.backend != "docling" {
    return Err(anyhow!(
      "unsupported pdf backend: {}",
      cfg.backend
    ));
  }
  let meta = fs::metadata(input)?;
  if cfg.skip_oversize
    && meta.len() > cfg.max_file_bytes
  {
    warn!(bytes = meta.len(), path = %input.display(), "skip oversized pdf");
    return Ok(Vec::new());
  }

  let output_dir = output
    .parent()
    .map(Path::to_path_buf)
    .ok_or_else(|| {
      anyhow!("missing output parent")
    })?;
  fs::create_dir_all(&output_dir)?;

  let quality = if cfg.text_first {
    classify_pdf_quality(input, cfg)?
  } else {
    PdfQuality::Scan
  };

  match quality {
    | PdfQuality::Text => {
      if cfg.split_text_extraction {
        info!(path = %input.display(), "extract pdf (paged text)");
        extract_pdf_text_paged(
          input, output, cfg
        )?;
      } else {
        info!(path = %input.display(), "extract pdf (text)");
        extract_pdf_text_single(
          input, output, cfg
        )?;
      }
      return Ok(vec![
        output.to_path_buf(),
      ]);
    }
    | PdfQuality::LowQuality => {
      info!(path = %input.display(), "extract pdf (low quality)");
      run_docling(
        input,
        &output_dir,
        cfg,
        DoclingMode::LowQuality
      )?;
    }
    | PdfQuality::Scan => {
      info!(path = %input.display(), "extract pdf (scan)");
      run_docling(
        input,
        &output_dir,
        cfg,
        DoclingMode::Scan
      )?;
    }
  }

  let default_out = output_dir.join(
    input
      .file_stem()
      .and_then(OsStr::to_str)
      .unwrap_or("document")
      .to_string()
      + ".txt"
  );
  if default_out.exists()
    && default_out != output
  {
    fs::rename(&default_out, output)
      .with_context(|| {
        format!(
          "rename {} -> {}",
          default_out.display(),
          output.display()
        )
      })?;
  }

  if !output.exists() {
    return Err(anyhow!(
      "docling output missing: {}",
      output.display()
    ));
  }
  Ok(vec![output.to_path_buf()])
}

fn join_parts_into(
  output: &Path,
  parts: &[PathBuf]
) -> anyhow::Result<()> {
  let mut out =
    fs::File::create(output)?;
  for part in parts {
    let bytes = fs::read(part)?;
    out.write_all(&bytes)?;
    out.write_all(b"\n")?;
  }
  Ok(())
}

fn extract_pdf_text_paged(
  input: &Path,
  output: &Path,
  cfg: &ExtractPdfConfig
) -> anyhow::Result<()> {
  let total_pages =
    pdf_page_count(input, cfg)?;
  if total_pages == 0 {
    return Err(anyhow!(
      "pdf page count is zero: {}",
      input.display()
    ));
  }
  let pages_per_pass =
    cfg.max_pages_per_pass.max(1);
  let mut out =
    fs::File::create(output)?;
  let mut page = 1usize;
  while page <= total_pages {
    let end = (page + pages_per_pass
      - 1)
      .min(total_pages);
    let output =
      Command::new(&cfg.pdftotext_bin)
        .arg("-f")
        .arg(page.to_string())
        .arg("-l")
        .arg(end.to_string())
        .arg(input)
        .arg("-")
        .output()
        .with_context(|| {
          format!(
            "pdftotext failed for {}",
            input.display()
          )
        })?;
    out.write_all(&output.stdout)?;
    out.write_all(b"\n")?;
    page = end + 1;
  }
  Ok(())
}

fn extract_pdf_text_single(
  input: &Path,
  output: &Path,
  cfg: &ExtractPdfConfig
) -> anyhow::Result<()> {
  let output_text =
    Command::new(&cfg.pdftotext_bin)
      .arg(input)
      .arg("-")
      .output()
      .with_context(|| {
        format!(
          "pdftotext failed for {}",
          input.display()
        )
      })?;
  fs::write(
    output,
    output_text.stdout
  )?;
  Ok(())
}

fn pdf_page_count(
  input: &Path,
  cfg: &ExtractPdfConfig
) -> anyhow::Result<usize> {
  let output =
    Command::new(&cfg.pdfinfo_bin)
      .arg(input)
      .output()
      .with_context(|| {
        format!(
          "pdfinfo failed for {}",
          input.display()
        )
      })?;
  let stdout = String::from_utf8_lossy(
    &output.stdout
  );
  for line in stdout.lines() {
    if let Some(rest) =
      line.strip_prefix("Pages:")
    {
      let pages = rest
        .trim()
        .parse::<usize>()
        .unwrap_or(0);
      return Ok(pages);
    }
  }
  Ok(0)
}

#[derive(Debug, Clone, Copy)]
enum PdfQuality {
  Text,
  LowQuality,
  Scan
}

#[derive(Debug, Clone, Copy)]
enum DoclingMode {
  LowQuality,
  Scan
}

fn classify_pdf_quality(
  input: &Path,
  cfg: &ExtractPdfConfig
) -> anyhow::Result<PdfQuality> {
  let output =
    Command::new(&cfg.pdffonts_bin)
      .arg(input)
      .output()
      .with_context(|| {
        format!(
          "pdffonts failed for {}",
          input.display()
        )
      })?;
  let stdout = String::from_utf8_lossy(
    &output.stdout
  );
  let has_fonts =
    stdout.lines().any(|line| {
      line.contains("TrueType")
        || line.contains("Type")
        || line.contains("CID")
    });
  if !has_fonts {
    return Ok(PdfQuality::Scan);
  }

  let mut total_chars = 0usize;
  let mut alpha_chars = 0usize;
  let mut pages_sampled = 0usize;
  for page in 1..=cfg.text_sample_pages
  {
    let output =
      Command::new(&cfg.pdftotext_bin)
        .arg("-f")
        .arg(page.to_string())
        .arg("-l")
        .arg(page.to_string())
        .arg(input)
        .arg("-")
        .output()
        .with_context(|| {
          format!(
            "pdftotext failed for {}",
            input.display()
          )
        })?;
    let text = String::from_utf8_lossy(
      &output.stdout
    );
    let trimmed = text.trim();
    if trimmed.is_empty() {
      continue;
    }
    pages_sampled += 1;
    for ch in trimmed.chars() {
      if ch.is_ascii_alphabetic() {
        alpha_chars += 1;
      }
      if !ch.is_control() {
        total_chars += 1;
      }
    }
  }
  if pages_sampled == 0 {
    return Ok(PdfQuality::Scan);
  }
  let avg_chars =
    total_chars / pages_sampled;
  let alpha_ratio = if total_chars == 0
  {
    0.0
  } else {
    alpha_chars as f32
      / total_chars as f32
  };
  if avg_chars
    >= cfg.text_good_min_chars
    && alpha_ratio
      >= cfg.text_alpha_ratio_min
  {
    return Ok(PdfQuality::Text);
  }
  if avg_chars >= cfg.text_low_min_chars
  {
    return Ok(PdfQuality::LowQuality);
  }
  Ok(PdfQuality::Scan)
}

fn run_docling(
  input: &Path,
  output_dir: &Path,
  cfg: &ExtractPdfConfig,
  mode: DoclingMode
) -> anyhow::Result<()> {
  let mut cmd =
    Command::new(&cfg.docling_bin);
  cmd
    .arg(&cfg.docling_script)
    .arg("--from")
    .arg("pdf")
    .arg("--to")
    .arg("text")
    .arg("--device")
    .arg(&cfg.docling_device)
    .arg("--pipeline")
    .arg(&cfg.docling_pipeline)
    .arg("--pdf-backend")
    .arg(&cfg.docling_pdf_backend)
    .arg("--num-threads")
    .arg(
      cfg.docling_threads.to_string()
    );
  if cfg.page_batch_size > 0 {
    cmd.arg("--page-batch-size").arg(
      cfg.page_batch_size.to_string()
    );
  }
  if cfg.document_timeout_seconds > 0 {
    cmd.arg("--document-timeout").arg(
      cfg
        .document_timeout_seconds
        .to_string()
    );
  }
  match mode {
    | DoclingMode::LowQuality => {
      if cfg.low_quality_use_ocr {
        cmd.arg("--ocr");
        if cfg.low_quality_force_ocr {
          cmd.arg("--force-ocr");
        }
      } else {
        cmd.arg("--no-ocr");
      }
      if cfg.low_quality_tables {
        cmd
          .arg("--tables")
          .arg("--table-mode")
          .arg(
            &cfg.low_quality_table_mode
          );
      } else {
        cmd.arg("--no-tables");
      }
    }
    | DoclingMode::Scan => {
      cmd.arg("--ocr");
      if cfg.scan_force_ocr {
        cmd.arg("--force-ocr");
      }
      if cfg.scan_tables {
        cmd
          .arg("--tables")
          .arg("--table-mode")
          .arg(&cfg.scan_table_mode);
      } else {
        cmd.arg("--no-tables");
      }
    }
  }
  if cfg.ocr_fallback {
    cmd
      .arg("--ocr-lang")
      .arg(&cfg.ocr_lang)
      .arg("--ocr-engine")
      .arg(&cfg.ocr_engine);
  }
  cmd
    .arg("--output")
    .arg(output_dir)
    .arg("--")
    .arg(input);

  let status = cmd
    .status()
    .with_context(|| {
      format!(
        "docling failed for {}",
        input.display()
      )
    })?;
  if !status.success() {
    return Err(anyhow!(
      "docling exit status: {}",
      status
    ));
  }
  Ok(())
}

fn split_markdown_file(
  path: &Path,
  max_chapter_bytes: u64
) -> anyhow::Result<Vec<PathBuf>> {
  let raw = fs::read_to_string(path)?;
  let mut parts = Vec::new();
  let mut current = String::new();
  let mut part_index = 0usize;

  for line in raw.lines() {
    let is_heading =
      line.starts_with('#');
    if is_heading
      && !current.is_empty()
      && current.len() as u64
        > max_chapter_bytes
    {
      part_index += 1;
      parts.push(write_part(
        path, part_index, &current
      )?);
      current.clear();
    }
    current.push_str(line);
    current.push('\n');
  }

  if !current.is_empty() {
    part_index += 1;
    parts.push(write_part(
      path, part_index, &current
    )?);
  }

  Ok(parts)
}

fn write_part(
  base: &Path,
  index: usize,
  contents: &str
) -> anyhow::Result<PathBuf> {
  let part_path =
    base.with_file_name(format!(
      "{}-part{:04}.txt",
      base
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("book"),
      index
    ));
  let mut file =
    fs::File::create(&part_path)?;
  file
    .write_all(contents.as_bytes())?;
  Ok(part_path)
}

fn read_metadata(
  path: &Path,
  format: &str
) -> ExtractedMetadata {
  let mut metadata =
    ExtractedMetadata {
      format: format.to_string(),
      ..Default::default()
    };
  if let Some(opf_path) = find_opf(path)
    && let Ok(opf) =
      fs::read_to_string(&opf_path)
  {
    parse_opf(&opf, &mut metadata);
  }
  metadata
}

fn find_opf(
  path: &Path
) -> Option<PathBuf> {
  let parent = path.parent()?;
  let opf = parent.join("metadata.opf");
  if opf.exists() {
    return Some(opf);
  }
  None
}

fn parse_opf(
  xml: &str,
  metadata: &mut ExtractedMetadata
) {
  let mut reader =
    Reader::from_str(xml);
  reader.config_mut().trim_text(true);
  let mut buf = Vec::new();
  let mut current = String::new();
  let mut tag = String::new();

  loop {
    match reader
      .read_event_into(&mut buf)
    {
      | Ok(Event::Start(e)) => {
        tag = String::from_utf8_lossy(
          e.name().as_ref()
        )
        .to_string();
        current.clear();
      }
      | Ok(Event::Text(e)) => {
        if let Ok(text) = e.decode() {
          current.push_str(&text);
        }
      }
      | Ok(Event::End(_)) => {
        let text = current.trim();
        if !text.is_empty() {
          match tag.as_str() {
            | "dc:title" | "title" => {
              metadata.title =
                Some(text.to_string())
            }
            | "dc:creator"
            | "creator" => {
              metadata
                .authors
                .push(text.to_string())
            }
            | "dc:language"
            | "language" => {
              metadata.language =
                Some(text.to_string())
            }
            | "dc:date" | "date" => {
              metadata.published =
                Some(text.to_string())
            }
            | "dc:identifier"
            | "identifier" => {
              metadata
                .identifiers
                .push(text.to_string())
            }
            | _ => {}
          }
        }
        tag.clear();
        current.clear();
      }
      | Ok(Event::Eof) => break,
      | Err(_) => break,
      | _ => {}
    }
    buf.clear();
  }

  if metadata.calibre_id.is_none() {
    for ident in &metadata.identifiers {
      if ident
        .to_ascii_lowercase()
        .contains("calibre")
      {
        metadata.calibre_id =
          Some(ident.clone());
        break;
      }
    }
  }
}

fn write_metadata(
  path: &Path,
  metadata: &ExtractedMetadata
) -> anyhow::Result<()> {
  if let Some(parent) = path.parent() {
    fs::create_dir_all(parent)?;
  }
  let raw = serde_json::to_vec_pretty(
    metadata
  )?;
  fs::write(path, raw)?;
  Ok(())
}

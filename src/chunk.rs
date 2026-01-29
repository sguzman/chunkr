use crate::config::{ChunkConfig, Config};
use crate::util::replace_extension;
use anyhow::Context;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::fs;
use std::io::Write;
use std::path::Path;
use tracing::{debug, info, warn};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Serialize)]
struct ChunkRecord {
    id: String,
    text: String,
    metadata: Value,
}

pub fn run(config: &Config) -> anyhow::Result<()> {
    let mut total_files = 0usize;
    let mut total_chunks = 0usize;
    for entry in WalkDir::new(&config.paths.extract_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("txt") {
            continue;
        }
        total_files += 1;
        let chunks = chunk_file(path, config)?;
        total_chunks += chunks;
    }
    info!(total_files, total_chunks, "chunk complete");
    Ok(())
}

fn chunk_file(path: &Path, config: &Config) -> anyhow::Result<usize> {
    let rel = path
        .strip_prefix(&config.paths.extract_root)
        .unwrap_or(path);
    let out_path = replace_extension(&config.paths.chunk_root.join(rel), "jsonl");
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let cleaned = normalize_text(&raw, &config.chunk);
    if cleaned.trim().is_empty() {
        warn!(path = %path.display(), "empty text after normalization");
        return Ok(0);
    }

    let paragraphs = split_paragraphs(&cleaned, &config.chunk);
    let chunks = build_chunks(&paragraphs, &config.chunk);
    if chunks.is_empty() {
        warn!(path = %path.display(), "no chunks emitted");
        return Ok(0);
    }

    let mut file_meta = load_metadata(path)?;
    let mut writer = fs::File::create(&out_path)?;

    let mut total = 0usize;
    let mut cursor = 0usize;
    for (idx, chunk_text) in chunks.into_iter().enumerate() {
        let mut meta = Map::new();
        if config.chunk.metadata.include_source_path {
            meta.insert(
                "source_path".to_string(),
                Value::String(path.display().to_string()),
            );
            meta.insert(
                "source_rel".to_string(),
                Value::String(rel.display().to_string()),
            );
        }
        meta.insert("chunk_index".to_string(), Value::Number((idx as u64).into()));
        meta.insert("char_start".to_string(), Value::Number((cursor as u64).into()));
        meta.insert(
            "char_end".to_string(),
            Value::Number(((cursor + chunk_text.len()) as u64).into()),
        );
        cursor += chunk_text.len();

        if let Some(obj) = file_meta.as_object_mut() {
            for (k, v) in obj.iter() {
                if should_include_metadata(k, &config.chunk) {
                    meta.insert(k.clone(), v.clone());
                }
            }
        }

        let record = ChunkRecord {
            id: Uuid::new_v4().to_string(),
            text: chunk_text,
            metadata: Value::Object(meta),
        };
        let line = serde_json::to_string(&record)?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        total += 1;
    }

    debug!(path = %path.display(), chunks = total, "chunked file");
    Ok(total)
}

fn normalize_text(input: &str, cfg: &ChunkConfig) -> String {
    let mut out = input.to_string();
    if cfg.normalize_unicode {
        out = out.nfkc().collect::<String>();
    }
    if cfg.collapse_whitespace {
        let mut collapsed = String::with_capacity(out.len());
        let mut last_space = false;
        for ch in out.chars() {
            if ch.is_whitespace() {
                if !last_space {
                    collapsed.push(' ');
                    last_space = true;
                }
            } else {
                collapsed.push(ch);
                last_space = false;
            }
        }
        out = collapsed;
    }
    out
}

fn split_paragraphs(text: &str, cfg: &ChunkConfig) -> Vec<String> {
    let mut paragraphs = Vec::new();
    let mut current = String::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            push_paragraph(&mut paragraphs, &mut current, cfg);
            continue;
        }
        current.push_str(line.trim());
        current.push('\n');
    }
    push_paragraph(&mut paragraphs, &mut current, cfg);
    paragraphs
}

fn push_paragraph(out: &mut Vec<String>, current: &mut String, cfg: &ChunkConfig) {
    let trimmed = current.trim();
    if trimmed.is_empty() {
        current.clear();
        return;
    }
    if cfg.strip_headers {
        let lower = trimmed.to_ascii_lowercase();
        if trimmed.starts_with('#')
            || lower == "table of contents"
            || lower == "contents"
        {
            current.clear();
            return;
        }
    }
    let cleaned = trimmed.replace('\n', " ");
    if cleaned.len() < cfg.min_paragraph_chars {
        if let Some(last) = out.last_mut() {
            last.push(' ');
            last.push_str(&cleaned);
        } else {
            out.push(cleaned);
        }
    } else {
        out.push(cleaned);
    }
    current.clear();
}

fn build_chunks(paragraphs: &[String], cfg: &ChunkConfig) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut last_overlap = String::new();

    for para in paragraphs {
        let mut parts = Vec::new();
        if para.len() > cfg.max_paragraph_chars {
            parts.extend(split_large_paragraph(para, cfg.max_paragraph_chars));
        } else {
            parts.push(para.clone());
        }

        for part in parts {
            let bounded_parts = if part.len() > cfg.max_chunk_chars {
                split_by_max_bytes(&part, cfg.max_chunk_chars)
            } else {
                vec![part]
            };

            for part in bounded_parts {
                if current.len() + part.len() + 1 > cfg.max_chunk_chars && !current.is_empty() {
                if !last_overlap.is_empty() {
                    let mut overlap_chunk = last_overlap.clone();
                    overlap_chunk.push(' ');
                    overlap_chunk.push_str(&part);
                    if overlap_chunk.len() > cfg.max_chunk_chars {
                        current.clear();
                        current.push_str(&part);
                    } else {
                        current = overlap_chunk;
                    }
                } else {
                    current.clear();
                    current.push_str(&part);
                }
                } else {
                    if !current.is_empty() {
                        current.push(' ');
                    }
                    current.push_str(&part);
                }

                if current.len() >= cfg.target_chunk_chars {
                    let finalized = current.clone();
                    last_overlap = overlap_tail(&finalized, cfg.chunk_overlap_chars);
                    chunks.push(finalized);
                    current.clear();
                }
            }
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

fn split_large_paragraph(paragraph: &str, max_len: usize) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut buf = String::new();
    let mut chars = paragraph.chars().peekable();
    while let Some(ch) = chars.next() {
        buf.push(ch);
        if matches!(ch, '.' | '!' | '?') {
            if let Some(next) = chars.peek() {
                if next.is_whitespace() {
                    sentences.push(buf.trim().to_string());
                    buf.clear();
                }
            }
        }
    }
    if !buf.trim().is_empty() {
        sentences.push(buf.trim().to_string());
    }

    let mut parts = Vec::new();
    let mut current = String::new();
    for sentence in sentences {
        let sentence_parts = if sentence.len() > max_len {
            split_by_max_bytes(&sentence, max_len)
        } else {
            vec![sentence]
        };
        for sub in sentence_parts {
            if current.len() + sub.len() + 1 > max_len && !current.is_empty() {
                parts.push(current.trim().to_string());
                current.clear();
            }
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(&sub);
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    if parts.is_empty() {
        parts.push(paragraph.to_string());
    }
    parts
}

fn split_by_max_bytes(text: &str, max_len: usize) -> Vec<String> {
    if max_len == 0 {
        return Vec::new();
    }
    if text.len() <= max_len {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if word.len() > max_len {
            if !current.is_empty() {
                out.push(current.trim().to_string());
                current.clear();
            }
            let mut start = 0usize;
            for (idx, _) in word.char_indices() {
                if idx - start >= max_len {
                    out.push(word[start..idx].to_string());
                    start = idx;
                }
            }
            if start < word.len() {
                out.push(word[start..].to_string());
            }
            continue;
        }
        if current.is_empty() {
            current.push_str(word);
        } else if current.len() + 1 + word.len() <= max_len {
            current.push(' ');
            current.push_str(word);
        } else {
            out.push(current);
            current = word.to_string();
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    if out.is_empty() {
        out.push(text.to_string());
    }
    out
}

fn overlap_tail(text: &str, overlap: usize) -> String {
    if overlap == 0 {
        return String::new();
    }
    let total_chars = text.chars().count();
    if total_chars <= overlap {
        return text.to_string();
    }
    let start = total_chars.saturating_sub(overlap);
    text.chars().skip(start).collect()
}

fn load_metadata(path: &Path) -> anyhow::Result<Value> {
    let meta_path = replace_extension(path, "json");
    if !meta_path.exists() {
        return Ok(json!({}));
    }
    let raw = fs::read_to_string(&meta_path)?;
    let value: Value = serde_json::from_str(&raw)?;
    Ok(value)
}

fn should_include_metadata(key: &str, cfg: &ChunkConfig) -> bool {
    match key {
        "calibre_id" => cfg.metadata.include_calibre_id,
        "title" => cfg.metadata.include_title,
        "authors" => cfg.metadata.include_authors,
        "published" => cfg.metadata.include_published,
        "language" => cfg.metadata.include_language,
        _ => true,
    }
}

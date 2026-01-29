use crate::config::{Config, InsertEmbeddingsConfig, InsertQdrantConfig, InsertQuickwitConfig};
use crate::logging::{color_prefix, LogOp};
use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
struct ChunkRecord {
    id: String,
    text: String,
    metadata: Value,
}

pub async fn run(config: &Config) -> anyhow::Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(
            config.insert.embeddings.request_timeout_seconds,
        ))
        .build()?;

    if config.insert.qdrant.create_collection {
        ensure_qdrant_collection(&client, &config.insert.qdrant).await?;
    }

    let mut files = Vec::new();
    for entry in WalkDir::new(&config.paths.chunk_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        files.push(path.to_path_buf());
    }

    let total_files = files.len();
    if total_files == 0 {
        warn!("no chunk files found for insert");
        return Ok(());
    }
    info!(
        total_files,
        max_parallel_files = config.insert.max_parallel_files,
        "insert starting"
    );

    let file_semaphore = Arc::new(Semaphore::new(
        config.insert.max_parallel_files.max(1),
    ));
    let mut tasks = Vec::new();
    for path in files {
        let permit = file_semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let embeddings = config.insert.embeddings.clone();
        let qdrant = config.insert.qdrant.clone();
        let quickwit = config.insert.quickwit.clone();
        let batch_size = config.insert.batch_size;
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let prefix = color_prefix(&path.display().to_string(), None, None);
            info!(path = %path.display(), "{}insert file start", prefix);
            let count = ingest_file(
                &path,
                &client,
                &embeddings,
                &qdrant,
                &quickwit,
                batch_size,
            )
            .await?;
            Ok::<(usize, String), anyhow::Error>((count, path.display().to_string()))
        }));
    }

    let mut total_chunks = 0usize;
    for task in tasks {
        let (count, path) = task.await??;
        let prefix = color_prefix(&path, None, None);
        info!(path, count, "{}insert file complete", prefix);
        total_chunks += count;
    }

    info!(total_files, total_chunks, "insert complete");
    Ok(())
}

async fn ingest_file(
    path: &Path,
    client: &Client,
    embed_cfg: &InsertEmbeddingsConfig,
    qdrant_cfg: &InsertQdrantConfig,
    quickwit_cfg: &InsertQuickwitConfig,
    batch_size: usize,
) -> anyhow::Result<usize> {
    let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut total = 0usize;
    let mut buffer = Vec::new();
    let mut lines_seen = 0usize;
    let mut batch_idx = 0usize;

    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        lines_seen += 1;
        let record: ChunkRecord = serde_json::from_str(line)?;
        buffer.push(record);
        if buffer.len() >= batch_size {
            batch_idx += 1;
            debug!(
                path = %path.display(),
                batch_size = buffer.len(),
                lines_seen,
                "insert batch start"
            );
            total += process_batch(
                &buffer,
                client,
                embed_cfg,
                qdrant_cfg,
                quickwit_cfg,
                &BatchContext::new(path, batch_idx, lines_seen, &buffer),
            )
            .await?;
            debug!(
                path = %path.display(),
                total,
                lines_seen,
                "insert batch complete"
            );
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        batch_idx += 1;
        debug!(
            path = %path.display(),
            batch_size = buffer.len(),
            lines_seen,
            "insert final batch start"
        );
        total += process_batch(
            &buffer,
            client,
            embed_cfg,
            qdrant_cfg,
            quickwit_cfg,
            &BatchContext::new(path, batch_idx, lines_seen, &buffer),
        )
        .await?;
        debug!(
            path = %path.display(),
            total,
            lines_seen,
            "insert final batch complete"
        );
    }

    Ok(total)
}

#[derive(Clone)]
struct BatchContext {
    path: String,
    batch_idx: usize,
    lines_seen: usize,
    first_id: String,
    last_id: String,
}

impl BatchContext {
    fn new(path: &Path, batch_idx: usize, lines_seen: usize, batch: &[ChunkRecord]) -> Self {
        let first_id = batch
            .first()
            .map(|r| r.id.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let last_id = batch
            .last()
            .map(|r| r.id.clone())
            .unwrap_or_else(|| "unknown".to_string());
        Self {
            path: path.display().to_string(),
            batch_idx,
            lines_seen,
            first_id,
            last_id,
        }
    }
}

async fn process_batch(
    batch: &[ChunkRecord],
    client: &Client,
    embed_cfg: &InsertEmbeddingsConfig,
    qdrant_cfg: &InsertQdrantConfig,
    quickwit_cfg: &InsertQuickwitConfig,
    ctx: &BatchContext,
) -> anyhow::Result<usize> {
    let semaphore = Arc::new(Semaphore::new(embed_cfg.max_concurrency));
    let mut tasks = Vec::new();

    let batch_len = batch.len();
    let batch_start = std::time::Instant::now();
    let (mut min_len, mut max_len, mut sum_len) = (usize::MAX, 0usize, 0usize);
    for record in batch {
        let len = record.text.len();
        min_len = min_len.min(len);
        max_len = max_len.max(len);
        sum_len += len;
    }
    let avg_len = if batch_len == 0 { 0 } else { sum_len / batch_len };
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        lines_seen = ctx.lines_seen,
        first_id = %ctx.first_id,
        last_id = %ctx.last_id,
        min_len,
        max_len,
        avg_len,
        "{}embedding batch start",
        color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Ollama))
    );
    for record in batch {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let model = embed_cfg.model.clone();
        let base_url = embed_cfg.base_url.clone();
        let mut text = record.text.clone();
        if embed_cfg.max_input_chars > 0 && text.len() > embed_cfg.max_input_chars {
            text = text.chars().take(embed_cfg.max_input_chars).collect();
        }
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            embed_text(&client, &base_url, &model, &text).await
        }));
    }

    let mut vectors = Vec::new();
    for task in tasks {
        let vec = task.await??;
        vectors.push(vec);
    }

    let vector_dim = vectors.first().map(|v| v.len()).unwrap_or(0);
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        vector_dim,
        elapsed = ?batch_start.elapsed(),
        "{}embedding batch complete",
        color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Ollama))
    );
    upsert_qdrant(client, qdrant_cfg, batch, &vectors).await?;
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        "{}qdrant upsert complete",
        color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Qdrant))
    );
    ingest_quickwit(client, quickwit_cfg, batch).await?;
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        "{}quickwit ingest complete",
        color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Quickwit))
    );
    Ok(batch.len())
}

async fn embed_text(
    client: &Client,
    base_url: &str,
    model: &str,
    text: &str,
) -> anyhow::Result<Vec<f32>> {
    let url = format!("{}/api/embeddings", base_url.trim_end_matches('/'));
    let resp = client
        .post(url)
        .json(&json!({ "model": model, "prompt": text }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let snippet: String = text.chars().take(120).collect();
        return Err(anyhow!(
            "ollama embedding failed: {} {} (text_len={} snippet={:?})",
            status,
            body,
            text.len(),
            snippet
        ));
    }
    let value: Value = resp.json().await?;
    let embedding = value
        .get("embedding")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing embedding in response"))?
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0) as f32)
        .collect::<Vec<_>>();
    Ok(embedding)
}

async fn ensure_qdrant_collection(
    client: &Client,
    cfg: &InsertQdrantConfig,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/collections/{}",
        cfg.url.trim_end_matches('/'),
        cfg.collection
    );
    let body = json!({
        "vectors": {
            "size": cfg.vector_size,
            "distance": cfg.distance,
        }
    });
    let mut req = client.put(url).json(&body);
    if let Some(key) = cfg.api_key.as_ref().filter(|k| !k.is_empty()) {
        req = req.header("api-key", key);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        warn!(status = %resp.status(), "qdrant collection create failed");
    }
    Ok(())
}

async fn upsert_qdrant(
    client: &Client,
    cfg: &InsertQdrantConfig,
    batch: &[ChunkRecord],
    vectors: &[Vec<f32>],
) -> anyhow::Result<()> {
    if batch.len() != vectors.len() {
        return Err(anyhow!("embedding batch mismatch"));
    }
    let points = batch
        .iter()
        .zip(vectors.iter())
        .map(|(record, vector)| {
            json!({
                "id": record.id,
                "vector": vector,
                "payload": record.metadata,
            })
        })
        .collect::<Vec<_>>();
    let url = format!(
        "{}/collections/{}/points?wait=true",
        cfg.url.trim_end_matches('/'),
        cfg.collection
    );
    let mut req = client.put(url).json(&json!({ "points": points }));
    if let Some(key) = cfg.api_key.as_ref().filter(|k| !k.is_empty()) {
        req = req.header("api-key", key);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("qdrant upsert failed: {} {}", status, text));
    }
    Ok(())
}

async fn ingest_quickwit(
    client: &Client,
    cfg: &InsertQuickwitConfig,
    batch: &[ChunkRecord],
) -> anyhow::Result<()> {
    let url = format!(
        "{}/api/v1/{}/ingest?commit=force&commit_timeout_seconds={}",
        cfg.url.trim_end_matches('/'),
        cfg.index_id,
        cfg.commit_timeout_seconds
    );
    let mut body = String::new();
    for record in batch {
        let doc = json!({
            "id": record.id,
            "text": record.text,
            "metadata": record.metadata,
        });
        body.push_str(&serde_json::to_string(&doc)?);
        body.push('\n');
    }
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("quickwit ingest failed: {} {}", status, text));
    }
    Ok(())
}

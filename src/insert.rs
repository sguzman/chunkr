use crate::config::{Config, InsertEmbeddingsConfig, InsertQdrantConfig, InsertQuickwitConfig};
use crate::logging::{color_prefix, LogOp};
use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
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
    let global_embed_limit = if config.insert.embeddings.global_max_concurrency > 0 {
        config.insert.embeddings.global_max_concurrency
    } else {
        config.insert.embeddings.max_concurrency
    };
    let embed_semaphore = Arc::new(Semaphore::new(global_embed_limit.max(1)));
    let cache = if config.insert.embeddings.cache_max_entries > 0 {
        Some(Arc::new(Mutex::new(EmbeddingCache::new(
            config.insert.embeddings.cache_max_entries,
        ))))
    } else {
        None
    };
    let mut tasks = Vec::new();
    for path in files {
        let permit = file_semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let embeddings = config.insert.embeddings.clone();
        let qdrant = config.insert.qdrant.clone();
        let quickwit = config.insert.quickwit.clone();
        let batch_size = config.insert.batch_size;
        let embed_semaphore = embed_semaphore.clone();
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let prefix = color_prefix(&path.display().to_string(), None, None);
            info!(color_prefix = %prefix, path = %path.display(), "insert file start");
            let count = ingest_file(
                &path,
                &client,
                &embeddings,
                &qdrant,
                &quickwit,
                batch_size,
                &embed_semaphore,
                cache.as_ref(),
            )
            .await?;
            Ok::<(usize, String), anyhow::Error>((count, path.display().to_string()))
        }));
    }

    let mut total_chunks = 0usize;
    for task in tasks {
        let (count, path) = task.await??;
        let prefix = color_prefix(&path, None, None);
        info!(color_prefix = %prefix, path, count, "insert file complete");
        total_chunks += count;
    }

    if config.insert.quickwit.commit_at_end {
        quickwit_commit(&client, &config.insert.quickwit).await?;
    }
    info!(
        total_files,
        total_chunks,
        global_embed_limit,
        "insert complete"
    );
    Ok(())
}

async fn ingest_file(
    path: &Path,
    client: &Client,
    embed_cfg: &InsertEmbeddingsConfig,
    qdrant_cfg: &InsertQdrantConfig,
    quickwit_cfg: &InsertQuickwitConfig,
    batch_size: usize,
    embed_semaphore: &Arc<Semaphore>,
    cache: Option<&Arc<Mutex<EmbeddingCache>>>,
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
                embed_semaphore,
                cache,
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
            embed_semaphore,
            cache,
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
    embed_semaphore: &Arc<Semaphore>,
    cache: Option<&Arc<Mutex<EmbeddingCache>>>,
) -> anyhow::Result<usize> {
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
        color_prefix = %color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Ollama)),
        "embedding batch start"
    );
    let mut vectors: Vec<Option<Vec<f32>>> = vec![None; batch_len];
    let mut misses = Vec::new();
    let cache = cache.cloned();
    for (idx, record) in batch.iter().enumerate() {
        if let Some(cache) = cache.as_ref() {
            if let Some(vec) = cache.lock().unwrap().get(&record.text) {
                vectors[idx] = Some(vec);
                continue;
            }
        }
        misses.push((idx, record.text.clone()));
    }

    let request_batch_size = embed_cfg.request_batch_size.max(1);
    let mut tasks = Vec::new();
    for chunk in misses.chunks(request_batch_size) {
        let client = client.clone();
        let model = embed_cfg.model.clone();
        let base_url = embed_cfg.base_url.clone();
        let embed_semaphore = embed_semaphore.clone();
        let cache = cache.clone();
        let chunk = chunk.to_vec();
        let max_input_chars = embed_cfg.max_input_chars;
        tasks.push(tokio::spawn(async move {
            let mut results = Vec::new();
            for (idx, mut text) in chunk {
                if max_input_chars > 0 && text.len() > max_input_chars {
                    text = text.chars().take(max_input_chars).collect();
                }
                let permit = embed_semaphore.clone().acquire_owned().await?;
                let vec = embed_text(&client, &base_url, &model, &text).await?;
                drop(permit);
                if let Some(cache) = cache.as_ref() {
                    cache.lock().unwrap().insert(text.clone(), vec.clone());
                }
                results.push((idx, vec));
            }
            Ok::<Vec<(usize, Vec<f32>)>, anyhow::Error>(results)
        }));
    }

    for task in tasks {
        for (idx, vec) in task.await?? {
            vectors[idx] = Some(vec);
        }
    }

    let vectors = vectors
        .into_iter()
        .map(|v| v.ok_or_else(|| anyhow!("missing embedding result")))
        .collect::<Result<Vec<_>, _>>()?;

    let vector_dim = vectors.first().map(|v| v.len()).unwrap_or(0);
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        vector_dim,
        elapsed = ?batch_start.elapsed(),
        color_prefix = %color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Ollama)),
        "embedding batch complete"
    );
    let qdrant = upsert_qdrant(client, qdrant_cfg, batch, &vectors);
    let quickwit = ingest_quickwit(client, quickwit_cfg, batch);
    let (qdrant_res, quickwit_res) = tokio::join!(qdrant, quickwit);
    qdrant_res?;
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        color_prefix = %color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Qdrant)),
        "qdrant upsert complete"
    );
    quickwit_res?;
    info!(
        path = %ctx.path,
        batch_idx = ctx.batch_idx,
        batch_len,
        color_prefix = %color_prefix(&ctx.path, Some(&ctx.first_id), Some(LogOp::Quickwit)),
        "quickwit ingest complete"
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
    let wait = if cfg.wait { "true" } else { "false" };
    let url = format!(
        "{}/collections/{}/points?wait={}",
        cfg.url.trim_end_matches('/'),
        cfg.collection,
        wait
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
    let commit_mode = if cfg.commit_mode.is_empty() {
        "auto"
    } else {
        cfg.commit_mode.as_str()
    };
    let url = format!(
        "{}/api/v1/{}/ingest?commit={}&commit_timeout_seconds={}",
        cfg.url.trim_end_matches('/'),
        cfg.index_id,
        commit_mode,
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

async fn quickwit_commit(client: &Client, cfg: &InsertQuickwitConfig) -> anyhow::Result<()> {
    let url = format!(
        "{}/api/v1/{}/commit",
        cfg.url.trim_end_matches('/'),
        cfg.index_id
    );
    let resp = client.post(url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("quickwit commit failed: {} {}", status, text));
    }
    Ok(())
}

struct EmbeddingCache {
    max_entries: usize,
    order: VecDeque<u64>,
    values: HashMap<u64, Vec<f32>>,
}

impl EmbeddingCache {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            order: VecDeque::new(),
            values: HashMap::new(),
        }
    }

    fn get(&self, text: &str) -> Option<Vec<f32>> {
        let key = hash_text(text);
        self.values.get(&key).cloned()
    }

    fn insert(&mut self, text: String, vec: Vec<f32>) {
        let key = hash_text(&text);
        if !self.values.contains_key(&key) {
            self.order.push_back(key);
        }
        self.values.insert(key, vec);
        while self.values.len() > self.max_entries {
            if let Some(old) = self.order.pop_front() {
                self.values.remove(&old);
            } else {
                break;
            }
        }
    }
}

fn hash_text(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

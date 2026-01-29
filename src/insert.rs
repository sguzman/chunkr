use crate::config::{Config, InsertEmbeddingsConfig, InsertQdrantConfig, InsertQuickwitConfig};
use anyhow::{anyhow, Context};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{info, warn};
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

    let mut total_chunks = 0usize;
    for entry in WalkDir::new(&config.paths.chunk_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let count = ingest_file(
            path,
            &client,
            &config.insert.embeddings,
            &config.insert.qdrant,
            &config.insert.quickwit,
            config.insert.batch_size,
        )
        .await?;
        total_chunks += count;
    }
    info!(total_chunks, "insert complete");
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

    for line in raw.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let record: ChunkRecord = serde_json::from_str(line)?;
        buffer.push(record);
        if buffer.len() >= batch_size {
            total += process_batch(
                &buffer,
                client,
                embed_cfg,
                qdrant_cfg,
                quickwit_cfg,
            )
            .await?;
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        total += process_batch(&buffer, client, embed_cfg, qdrant_cfg, quickwit_cfg).await?;
    }

    Ok(total)
}

async fn process_batch(
    batch: &[ChunkRecord],
    client: &Client,
    embed_cfg: &InsertEmbeddingsConfig,
    qdrant_cfg: &InsertQdrantConfig,
    quickwit_cfg: &InsertQuickwitConfig,
) -> anyhow::Result<usize> {
    let semaphore = Arc::new(Semaphore::new(embed_cfg.max_concurrency));
    let mut tasks = Vec::new();

    for record in batch {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let model = embed_cfg.model.clone();
        let base_url = embed_cfg.base_url.clone();
        let text = record.text.clone();
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

    upsert_qdrant(client, qdrant_cfg, batch, &vectors).await?;
    ingest_quickwit(client, quickwit_cfg, batch).await?;
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
        return Err(anyhow!("ollama embedding failed: {}", resp.status()));
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
    let mut req = client.post(url).json(&json!({ "points": points }));
    if let Some(key) = cfg.api_key.as_ref().filter(|k| !k.is_empty()) {
        req = req.header("api-key", key);
    }
    let resp = req.send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("qdrant upsert failed: {}", resp.status()));
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
        return Err(anyhow!("quickwit ingest failed: {}", resp.status()));
    }
    Ok(())
}

use anyhow::{anyhow, Context, Result};
use chunkr::{chunk, config, insert, logging};
use reqwest::Client;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::{Duration, Instant};
use tracing::info;
use uuid::Uuid;

const SAMPLE_BYTES: usize = usize::MAX;

#[tokio::test]
#[ignore]
async fn chunk_and_insert_pipeline() -> Result<()> {
    let started = Instant::now();
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let (config_path, mut config) = load_test_config()?;
    let temp_root = std::env::temp_dir().join(format!("chunkr-test-{}", Uuid::new_v4()));
    let extract_root = temp_root.join("extract");
    let chunk_root = temp_root.join("chunked");
    let state_dir = temp_root.join("state");
    fs::create_dir_all(&extract_root)?;
    fs::create_dir_all(&chunk_root)?;
    fs::create_dir_all(&state_dir)?;

    let sample_files = list_example_files("examples")?;
    if sample_files.is_empty() {
        return Err(anyhow!("no example .txt files found"));
    }
    info!(
        "[test] using {} example files -> {}",
        sample_files.len(),
        extract_root.display()
    );
    for src in sample_files {
        let dst = extract_root.join(
            src.file_name()
                .ok_or_else(|| anyhow!("missing filename"))?,
        );
        copy_truncated(&src, &dst, SAMPLE_BYTES)?;
    }

    config.paths.extract_root = extract_root.clone();
    config.paths.chunk_root = chunk_root.clone();
    config.paths.state_dir = state_dir.clone();

    let embed_dim = detect_embedding_dim(&client, &config.insert.embeddings).await?;
    config.insert.qdrant.vector_size = embed_dim;

    init_logging_once(&config)?;
    info!("[test] using config {}", config_path.display());
    let test_collection = config.insert.qdrant.collection.clone();
    let test_index = config.insert.quickwit.index_id.clone();

    info!("[test] resetting qdrant collection {}", test_collection);
    reset_qdrant(
        &client,
        &config.insert.qdrant.url,
        test_collection,
        embed_dim,
    )
    .await?;
    info!("[test] resetting quickwit index {}", test_index);
    reset_quickwit(&client, &config.insert.quickwit.url, test_index).await?;

    info!("[test] starting chunk");
    run_in_process(&config, CommandKind::Chunk).await?;
    info!(
        "[test] chunk finished in {:?}, building sample query",
        started.elapsed()
    );
    let sample_query = sample_query_from_chunks(&chunk_root)?;
    info!(
        "[test] sample query picked (len={}): {:?}",
        sample_query.term.len(),
        sample_query.term
    );
    info!("[test] starting insert");
    run_in_process(&config, CommandKind::Insert).await?;
    info!(
        "[test] insert finished in {:?}, verifying qdrant/quickwit",
        started.elapsed()
    );

    verify_qdrant(
        &client,
        &config.insert.qdrant.url,
        test_collection,
        &config.insert.embeddings,
        &sample_query.embed_text,
    )
    .await?;
    verify_quickwit(
        &client,
        &config.insert.quickwit.url,
        test_index,
        &sample_query.term,
    )
    .await?;

    info!("[test] done in {:?}", started.elapsed());
    Ok(())
}

enum CommandKind {
    Chunk,
    Insert,
}

async fn run_in_process(config: &config::Config, command: CommandKind) -> Result<()> {
    match command {
        CommandKind::Chunk => {
            chunk::run(&config)?;
        }
        CommandKind::Insert => {
            insert::run(&config).await?;
        }
    }
    Ok(())
}

fn init_logging_once(config: &config::Config) -> Result<()> {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        logging::init(&config.logging);
    });
    Ok(())
}

async fn reset_qdrant(
    client: &Client,
    url: &str,
    collection: &str,
    vector_size: usize,
) -> Result<()> {
    let _ = delete_qdrant(client, url, collection).await;
    let create_url = format!(
        "{}/collections/{}",
        url.trim_end_matches('/'),
        collection
    );
    let body = json!({
        "vectors": { "size": vector_size, "distance": "Cosine" }
    });
    let resp = client.put(create_url).json(&body).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("qdrant create failed: {} {}", status, text));
    }
    Ok(())
}

async fn delete_qdrant(client: &Client, url: &str, collection: &str) -> Result<()> {
    let delete_url = format!(
        "{}/collections/{}",
        url.trim_end_matches('/'),
        collection
    );
    let _ = client.delete(delete_url).send().await?;
    Ok(())
}

async fn reset_quickwit(client: &Client, url: &str, index_id: &str) -> Result<()> {
    let _ = delete_quickwit(client, url, index_id).await;
    let create_url = format!("{}/api/v1/indexes", url.trim_end_matches('/'));
    let body = json!({
        "version": "0.7",
        "index_id": index_id,
        "doc_mapping": {
            "field_mappings": [
                { "name": "id", "type": "text", "tokenizer": "raw", "stored": true },
                { "name": "text", "type": "text", "tokenizer": "default", "stored": true },
                { "name": "metadata", "type": "json", "stored": true }
            ],
            "timestamp_field": null
        },
        "search_settings": {
            "default_search_fields": ["text"]
        },
        "indexing_settings": {
            "commit_timeout_secs": 30
        }
    });
    let resp = client.post(create_url).json(&body).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("quickwit create failed: {}", resp.status()));
    }
    Ok(())
}

async fn delete_quickwit(client: &Client, url: &str, index_id: &str) -> Result<()> {
    let delete_url = format!(
        "{}/api/v1/indexes/{}",
        url.trim_end_matches('/'),
        index_id
    );
    let _ = client.delete(delete_url).send().await?;
    Ok(())
}

async fn verify_qdrant(
    client: &Client,
    url: &str,
    collection: &str,
    embeddings: &config::InsertEmbeddingsConfig,
    query: &str,
) -> Result<()> {
    let count_url = format!(
        "{}/collections/{}/points/count",
        url.trim_end_matches('/'),
        collection
    );
    let resp = retry_request(|| {
        client
            .post(&count_url)
            .json(&json!({ "exact": true }))
    })
    .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("qdrant count failed: {}", resp.status()));
    }
    let count_json: serde_json::Value = resp.json().await?;
    let count = count_json
        .get("result")
        .and_then(|r| r.get("count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0);
    if count == 0 {
        return Err(anyhow!("qdrant count is zero"));
    }

    let embed = ollama_embed(client, embeddings, query).await?;
    let search_url = format!(
        "{}/collections/{}/points/search",
        url.trim_end_matches('/'),
        collection
    );
    let resp = retry_request(|| {
        client
            .post(&search_url)
            .json(&json!({ "vector": embed, "limit": 3 }))
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("qdrant search failed: {} {}", status, text));
    }
    let search_json: serde_json::Value = resp.json().await?;
    let hits = search_json
        .get("result")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);
    if hits == 0 {
        return Err(anyhow!("qdrant search returned no hits"));
    }
    Ok(())
}

async fn verify_quickwit(client: &Client, url: &str, index_id: &str, query: &str) -> Result<()> {
    let search_url = format!(
        "{}/api/v1/{}/search",
        url.trim_end_matches('/'),
        index_id
    );
    let hits = quickwit_hits(client, &search_url, query).await?;
    if hits == 0 {
        let fallback = quickwit_hits(client, &search_url, "*").await?;
        if fallback == 0 {
            return Err(anyhow!("quickwit search returned no hits"));
        }
    }
    Ok(())
}

async fn quickwit_hits(client: &Client, url: &str, query: &str) -> Result<u64> {
    let resp = retry_request(|| {
        client
            .post(url)
            .json(&json!({ "query": query, "max_hits": 3 }))
    })
    .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("quickwit search failed: {}", resp.status()));
    }
    let search_json: serde_json::Value = resp.json().await?;
    let hits = search_json
        .get("num_hits")
        .and_then(|t| t.as_u64())
        .or_else(|| search_json.get("hits").and_then(|h| h.get("total")).and_then(|t| t.as_u64()))
        .or_else(|| search_json.get("hits").and_then(|h| h.as_array()).map(|a| a.len() as u64))
        .unwrap_or(0);
    Ok(hits)
}

async fn ollama_embed(
    client: &Client,
    embeddings: &config::InsertEmbeddingsConfig,
    text: &str,
) -> Result<Vec<f32>> {
    let url = format!(
        "{}/api/embeddings",
        embeddings.base_url.trim_end_matches('/')
    );
    let resp = client
        .post(url)
        .json(&json!({ "model": embeddings.model, "prompt": text }))
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("ollama embed failed: {} {}", status, body));
    }
    let value: serde_json::Value = resp.json().await?;
    let embedding = value
        .get("embedding")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("missing embedding in response"))?
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0) as f32)
        .collect::<Vec<_>>();
    Ok(embedding)
}

async fn detect_embedding_dim(
    client: &Client,
    embeddings: &config::InsertEmbeddingsConfig,
) -> Result<usize> {
    let embedding = ollama_embed(client, embeddings, "federal regulation").await?;
    if embedding.is_empty() {
        return Err(anyhow!("ollama returned empty embedding"));
    }
    Ok(embedding.len())
}

async fn retry_request<F>(mut f: F) -> Result<reqwest::Response>
where
    F: FnMut() -> reqwest::RequestBuilder,
{
    let mut last_err: Option<anyhow::Error> = None;
    for _ in 0..3 {
        match f().send().await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                last_err = Some(anyhow!(err));
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("request failed")))
}

fn copy_truncated(src: &Path, dst: &Path, max_bytes: usize) -> Result<()> {
    let raw = fs::read(src).with_context(|| format!("read {}", src.display()))?;
    let slice = if raw.len() > max_bytes {
        &raw[..max_bytes]
    } else {
        &raw[..]
    };
    fs::write(dst, slice).with_context(|| format!("write {}", dst.display()))?;
    Ok(())
}

fn list_example_files(root: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("txt") {
            files.push(path.to_path_buf());
        }
    }
    Ok(files)
}

struct SampleQuery {
    embed_text: String,
    term: String,
}

fn sample_query_from_chunks(chunk_root: &Path) -> Result<SampleQuery> {
    for entry in walkdir::WalkDir::new(chunk_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let raw = fs::read_to_string(path)?;
        if let Some(line) = raw.lines().find(|l| !l.trim().is_empty()) {
            let value: serde_json::Value = serde_json::from_str(line)?;
            if let Some(text) = value.get("text").and_then(|v| v.as_str()) {
                let snippet = text.trim();
                if !snippet.is_empty() {
                    let embed_text: String = snippet.chars().take(160).collect();
                    let term = pick_query_term(&embed_text)
                        .unwrap_or_else(|| embed_text.clone());
                    return Ok(SampleQuery { embed_text, term });
                }
            }
        }
    }
    Err(anyhow!("no chunk text available for query"))
}

fn pick_query_term(text: &str) -> Option<String> {
    for token in text.split(|c: char| !c.is_alphanumeric()) {
        if token.len() >= 4 {
            return Some(token.to_lowercase());
        }
    }
    None
}

fn load_test_config() -> Result<(PathBuf, config::Config)> {
    let test_path = PathBuf::from("test.toml");
    let path = if test_path.exists() {
        test_path
    } else {
        PathBuf::from("config.toml")
    };
    let config = config::load(&path.to_path_buf())?;
    Ok((path, config))
}

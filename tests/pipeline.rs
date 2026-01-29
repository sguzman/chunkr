use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

const SAMPLE_FILES: &[&str] = &[
    "examples/CFR-2025-title2-vol1.part001.txt",
    "examples/CFR-2025-title2-vol1.part002.txt",
];
const SAMPLE_BYTES: usize = 20_000;

#[tokio::test]
#[ignore]
async fn chunk_and_insert_pipeline() -> Result<()> {
    let base = load_base_config()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let temp_root = std::env::temp_dir().join(format!("chunkr-test-{}", Uuid::new_v4()));
    let extract_root = temp_root.join("extract");
    let chunk_root = temp_root.join("chunked");
    let state_dir = temp_root.join("state");
    fs::create_dir_all(&extract_root)?;
    fs::create_dir_all(&chunk_root)?;
    fs::create_dir_all(&state_dir)?;

    for file in SAMPLE_FILES {
        let src = Path::new(file);
        let dst = extract_root.join(
            src.file_name()
                .ok_or_else(|| anyhow!("missing filename"))?,
        );
        copy_truncated(src, &dst, SAMPLE_BYTES)?;
    }

    let test_collection = "chunkr_test";
    let test_index = "chunkr_test";
    let config_path = temp_root.join("config.toml");
    let embed_dim = detect_embedding_dim(&client, &base).await?;
    let config_contents = render_config(
        &base,
        &extract_root,
        &chunk_root,
        &state_dir,
        test_collection,
        test_index,
        embed_dim,
    );
    fs::write(&config_path, config_contents)?;

    reset_qdrant(&client, &base.qdrant_url, test_collection, embed_dim).await?;
    reset_quickwit(&client, &base.quickwit_url, test_index).await?;

    run_chunkr(&config_path, "chunk")?;
    let sample_query = sample_query_from_chunks(&chunk_root)?;
    run_chunkr(&config_path, "insert")?;

    verify_qdrant(
        &client,
        &base.qdrant_url,
        test_collection,
        &base,
        &sample_query.embed_text,
    )
    .await?;
    verify_quickwit(
        &client,
        &base.quickwit_url,
        test_index,
        &sample_query.term,
    )
    .await?;

    // Best-effort cleanup
    let _ = delete_qdrant(&client, &base.qdrant_url, test_collection).await;
    let _ = delete_quickwit(&client, &base.quickwit_url, test_index).await;

    Ok(())
}

fn run_chunkr(config: &Path, command: &str) -> Result<()> {
    let status = Command::new(env!("CARGO_BIN_EXE_chunkr"))
        .arg("--config")
        .arg(config)
        .arg(command)
        .status()
        .with_context(|| format!("run chunkr {}", command))?;
    if !status.success() {
        return Err(anyhow!("chunkr {} failed: {}", command, status));
    }
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
    base: &BaseConfig,
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

    let embed = ollama_embed(client, base, query).await?;
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

async fn ollama_embed(client: &Client, base: &BaseConfig, text: &str) -> Result<Vec<f32>> {
    let url = format!("{}/api/embeddings", base.ollama_url.trim_end_matches('/'));
    let resp = client
        .post(url)
        .json(&json!({ "model": base.ollama_model, "prompt": text }))
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

async fn detect_embedding_dim(client: &Client, base: &BaseConfig) -> Result<usize> {
    let embedding = ollama_embed(client, base, "federal regulation").await?;
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

struct BaseConfig {
    qdrant_url: String,
    quickwit_url: String,
    ollama_url: String,
    ollama_model: String,
}

fn load_base_config() -> Result<BaseConfig> {
    let raw = fs::read_to_string("config.toml")?;
    let value: toml::Value = toml::from_str(&raw)?;
    let qdrant_url = value
        .get("insert")
        .and_then(|v| v.get("qdrant"))
        .and_then(|v| v.get("url"))
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1:6333")
        .to_string();
    let quickwit_url = value
        .get("insert")
        .and_then(|v| v.get("quickwit"))
        .and_then(|v| v.get("url"))
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1:7280")
        .to_string();
    let ollama_url = value
        .get("insert")
        .and_then(|v| v.get("embeddings"))
        .and_then(|v| v.get("base_url"))
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1:11434")
        .to_string();
    let ollama_model = value
        .get("insert")
        .and_then(|v| v.get("embeddings"))
        .and_then(|v| v.get("model"))
        .and_then(|v| v.as_str())
        .unwrap_or("qllama/bge-small-en-v1.5:latest")
        .to_string();
    Ok(BaseConfig {
        qdrant_url,
        quickwit_url,
        ollama_url,
        ollama_model,
    })
}

fn render_config(
    base: &BaseConfig,
    extract_root: &Path,
    chunk_root: &Path,
    state_dir: &Path,
    collection: &str,
    index_id: &str,
    vector_size: usize,
) -> String {
    format!(
        r#"[logging]
level = "info"

[paths]
calibre_root = "/drive/calibre/en_nonfiction"
extract_root = "{extract_root}"
chunk_root = "{chunk_root}"
state_dir = "{state_dir}"

[extract]
extensions = ["epub", "pdf"]
skip_existing = true
write_metadata = false
output_layout = "{{format}}/{{title_slug}}.txt"
metadata_layout = "{{format}}/{{title_slug}}.json"

[extract.epub]
backend = "pandoc"
pandoc_bin = "pandoc"
toc_depth = 3
chapter_split = false
max_chapter_bytes = 2000000
max_file_bytes = 20000000

[extract.pdf]
backend = "docling"
pdffonts_bin = "pdffonts"
pdftotext_bin = "pdftotext"
docling_bin = "/home/admin/Code/AI/docling/.venv/bin/python"
docling_script = "/home/admin/Code/AI/docling/docling/cli/main.py"
text_first = true
text_min_chars = 40
text_sample_pages = 3
ocr_fallback = true
ocr_lang = "eng"
ocr_engine = "tesseract"
docling_device = "cuda"
docling_pipeline = "standard"
docling_pdf_backend = "dlparse_v4"
docling_threads = 16
docling_tables = true
docling_table_mode = "accurate"
max_file_bytes = 20000000
skip_oversize = false

[chunk]
normalize_unicode = true
collapse_whitespace = true
strip_headers = true
min_paragraph_chars = 80
max_paragraph_chars = 1200
target_chunk_chars = 800
max_chunk_chars = 900
chunk_overlap_chars = 100
emit_jsonl = true

[chunk.metadata]
include_source_path = true
include_calibre_id = true
include_title = true
include_authors = true
include_published = true
include_language = true

[insert]
batch_size = 16
retry_max = 3
retry_backoff_ms = 500

[insert.qdrant]
url = "{qdrant_url}"
collection = "{collection}"
distance = "Cosine"
vector_size = {vector_size}
create_collection = false
api_key = ""

[insert.quickwit]
url = "{quickwit_url}"
index_id = "{index_id}"
commit_timeout_seconds = 30

[insert.embeddings]
provider = "ollama"
base_url = "{ollama_url}"
model = "{ollama_model}"
request_timeout_seconds = 120
max_concurrency = 2
"#,
        extract_root = extract_root.display(),
        chunk_root = chunk_root.display(),
        state_dir = state_dir.display(),
        qdrant_url = base.qdrant_url,
        quickwit_url = base.quickwit_url,
        ollama_url = base.ollama_url,
        ollama_model = base.ollama_model,
        collection = collection,
        index_id = index_id,
        vector_size = vector_size
    )
}

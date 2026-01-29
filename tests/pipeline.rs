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
    let config_contents = render_config(
        &base,
        &extract_root,
        &chunk_root,
        &state_dir,
        test_collection,
        test_index,
    );
    fs::write(&config_path, config_contents)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;

    reset_qdrant(&client, &base.qdrant_url, test_collection).await?;
    reset_quickwit(&client, &base.quickwit_url, test_index).await?;

    run_chunkr(&config_path, "chunk")?;
    run_chunkr(&config_path, "insert")?;

    verify_qdrant(&client, &base.qdrant_url, test_collection, &base).await?;
    verify_quickwit(&client, &base.quickwit_url, test_index).await?;

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

async fn reset_qdrant(client: &Client, url: &str, collection: &str) -> Result<()> {
    let _ = delete_qdrant(client, url, collection).await;
    let create_url = format!(
        "{}/collections/{}",
        url.trim_end_matches('/'),
        collection
    );
    let body = json!({
        "vectors": { "size": 384, "distance": "Cosine" }
    });
    let resp = client.put(create_url).json(&body).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!("qdrant create failed: {}", resp.status()));
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

async fn verify_qdrant(client: &Client, url: &str, collection: &str, base: &BaseConfig) -> Result<()> {
    let count_url = format!(
        "{}/collections/{}/points/count",
        url.trim_end_matches('/'),
        collection
    );
    let resp = client
        .post(count_url)
        .json(&json!({ "exact": true }))
        .send()
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

    let embed = ollama_embed(client, base, "federal regulation").await?;
    let search_url = format!(
        "{}/collections/{}/points/search",
        url.trim_end_matches('/'),
        collection
    );
    let resp = client
        .post(search_url)
        .json(&json!({ "vector": embed, "limit": 3 }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("qdrant search failed: {}", resp.status()));
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

async fn verify_quickwit(client: &Client, url: &str, index_id: &str) -> Result<()> {
    let search_url = format!(
        "{}/api/v1/{}/search",
        url.trim_end_matches('/'),
        index_id
    );
    let resp = client
        .post(search_url)
        .json(&json!({ "query": "federal regulation", "max_hits": 3 }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("quickwit search failed: {}", resp.status()));
    }
    let search_json: serde_json::Value = resp.json().await?;
    let hits = search_json
        .get("hits")
        .and_then(|h| h.get("total"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0);
    if hits == 0 {
        return Err(anyhow!("quickwit search returned no hits"));
    }
    Ok(())
}

async fn ollama_embed(client: &Client, base: &BaseConfig, text: &str) -> Result<Vec<f32>> {
    let url = format!("{}/api/embeddings", base.ollama_url.trim_end_matches('/'));
    let resp = client
        .post(url)
        .json(&json!({ "model": base.ollama_model, "prompt": text }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!("ollama embed failed: {}", resp.status()));
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
min_paragraph_chars = 120
max_paragraph_chars = 2400
target_chunk_chars = 1800
max_chunk_chars = 2600
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
batch_size = 16
retry_max = 3
retry_backoff_ms = 500

[insert.qdrant]
url = "{qdrant_url}"
collection = "{collection}"
distance = "Cosine"
vector_size = 384
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
        index_id = index_id
    )
}

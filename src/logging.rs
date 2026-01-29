use crate::config::LoggingConfig;
use tracing_subscriber::EnvFilter;

pub fn init(config: &LoggingConfig) {
    let filter = EnvFilter::try_new(config.level.clone()).unwrap_or_else(|_| {
        EnvFilter::new("info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .init();
}

#[derive(Debug, Clone, Copy)]
pub enum LogOp {
    Ollama,
    Qdrant,
    Quickwit,
}

pub fn color_prefix(file_key: &str, chunk_key: Option<&str>, op: Option<LogOp>) -> String {
    let file_color = color_for_key(file_key);
    let chunk_color = chunk_key.map(color_for_key).unwrap_or(240);
    let op_color = op.map(op_color).unwrap_or(244);
    format!(
        "{} {} {} ",
        color_block(file_color),
        color_block(chunk_color),
        color_block(op_color)
    )
}

fn op_color(op: LogOp) -> u8 {
    match op {
        LogOp::Ollama => 39,
        LogOp::Qdrant => 82,
        LogOp::Quickwit => 220,
    }
}

fn color_for_key(key: &str) -> u8 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let palette = 216u64;
    let idx = (hash % palette) as u8;
    16 + idx
}

fn color_block(color: u8) -> String {
    format!("\x1b[38;5;{}m█\x1b[0m", color)
}

mod chunk;
mod config;
mod extract;
mod insert;
mod logging;
mod util;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "chunkr", version, about = "Text extraction, chunking, and indexing pipeline")]
struct Cli {
    #[arg(long, short, default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Extract,
    Chunk,
    Insert,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = config::load(&cli.config)?;
    logging::init(&config.logging);

    match cli.command {
        Commands::Extract => extract::run(&config)?,
        Commands::Chunk => chunk::run(&config)?,
        Commands::Insert => insert::run(&config).await?,
    }

    Ok(())
}

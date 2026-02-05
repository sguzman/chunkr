use std::path::PathBuf;

use chunkr::{
  chunk,
  config,
  dups,
  extract,
  insert,
  logging
};
use clap::{
  Parser,
  Subcommand
};

#[derive(Debug, Parser)]
#[command(
  name = "chunkr",
  version,
  about = "Text extraction, chunking, \
           and indexing pipeline"
)]
struct Cli {
  #[arg(
    long,
    short,
    default_value = "config.toml"
  )]
  config:  PathBuf,
  #[command(subcommand)]
  command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
  Extract,
  Chunk,
  Insert,
  Dups(dups::DupsArgs)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let cli = Cli::parse();
  let config =
    config::load(&cli.config)?;
  logging::init(&config.logging);

  match cli.command {
    | Commands::Extract => {
      extract::run(&config)?
    }
    | Commands::Chunk => {
      chunk::run(&config)?
    }
    | Commands::Insert => {
      insert::run(&config).await?
    }
    | Commands::Dups(args) => {
      dups::run(&config, &args)?
    }
  }

  Ok(())
}

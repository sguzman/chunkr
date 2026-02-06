use std::fs;
use std::path::PathBuf;

use anyhow::{
  Context,
  Result
};
use clap::Args;
use serde::Serialize;

use crate::config::{
  Config,
  DupsStatsMode
};
use crate::dups::DuplicateGroup;

#[derive(Debug, Args)]
pub struct DupStatsArgs {
  /// Path to the duplicated report
  /// produced by `chunkr dups`
  #[arg(long)]
  pub input: PathBuf,

  /// Output mode (human vs machine
  /// readable)
  #[arg(long, value_enum)]
  pub mode: Option<DupsStatsMode>
}

#[derive(Debug, Serialize)]
struct DupGroupSummary {
  bytes:       u64,
  files:       usize,
  extra_bytes: u64
}

#[derive(Debug, Serialize)]
struct DupStatsSummary {
  total_groups:      usize,
  total_files:       usize,
  total_extra_bytes: u64,
  groups: Vec<DupGroupSummary>
}

pub fn run(
  config: &Config,
  args: &DupStatsArgs
) -> Result<()> {
  let mode = args
    .mode
    .unwrap_or(config.dup_stats.mode);
  let data =
    fs::read_to_string(&args.input)
      .with_context(|| {
        format!(
          "read duplicate report {}",
          args.input.display()
        )
      })?;
  let groups: Vec<DuplicateGroup> =
    serde_json::from_str(&data)
      .with_context(|| {
        format!(
          "parse duplicate report {}",
          args.input.display()
        )
      })?;
  let summary = summarize(&groups);
  match mode {
    | DupsStatsMode::Human => {
      print_human(&summary)
    }
    | DupsStatsMode::Machine => {
      print_machine(&summary)
    }
  }
}

fn summarize(
  groups: &[DuplicateGroup]
) -> DupStatsSummary {
  let mut summary = DupStatsSummary {
    total_groups:      groups.len(),
    total_files:       0,
    total_extra_bytes: 0,
    groups:            Vec::new()
  };
  for group in groups {
    summary.total_files +=
      group.files.len();
    if group.files.len() >= 2 {
      let extra =
        (group.files.len() as u64 - 1)
          * group.bytes;
      if extra > 0 {
        summary.total_extra_bytes +=
          extra;
        summary.groups.push(
          DupGroupSummary {
            bytes:       group.bytes,
            files:       group
              .files
              .len(),
            extra_bytes: extra
          }
        );
      }
    }
  }
  summary.groups.sort_by(|a, b| {
    b.extra_bytes.cmp(&a.extra_bytes)
  });
  summary
}

fn print_machine(
  summary: &DupStatsSummary
) -> Result<()> {
  println!(
    "{}",
    serde_json::to_string_pretty(
      summary
    )?
  );
  Ok(())
}

fn print_human(
  summary: &DupStatsSummary
) -> Result<()> {
  println!(
    "Duplicate groups: {}\nFiles \
     involved: {}\nDuplicate bytes: {}",
    summary.total_groups,
    summary.total_files,
    format_bytes(
      summary.total_extra_bytes
    )
  );
  if !summary.groups.is_empty() {
    println!("Top duplicate buckets:");
    for group in
      summary.groups.iter().take(5)
    {
      println!(
        "- {} duplicates × {} bytes = \
         {} extra",
        group.files,
        format_bytes(group.bytes),
        format_bytes(group.extra_bytes)
      );
    }
    if summary.groups.len() > 5 {
      println!(
        "... and {} more groups",
        summary.groups.len() - 5
      );
    }
  } else {
    println!(
      "No duplicate bytes detected."
    );
  }
  Ok(())
}

fn format_bytes(bytes: u64) -> String {
  const UNITS: &[&str] =
    &["B", "KB", "MB", "GB", "TB"];
  let mut value = bytes as f64;
  let mut idx = 0usize;
  while value >= 1024.0
    && idx < UNITS.len() - 1
  {
    value /= 1024.0;
    idx += 1;
  }
  if idx == 0 {
    format!("{} {}", bytes, UNITS[idx])
  } else {
    format!(
      "{:.2} {}",
      value, UNITS[idx]
    )
  }
}

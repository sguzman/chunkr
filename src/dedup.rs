use std::collections::HashSet;
use std::fs;
use std::path::{
  Path,
  PathBuf
};
use std::process::Command;

use anyhow::{
  Context,
  Result,
  bail
};
use clap::Args;
use serde_json::Value;
use tracing::{
  info,
  warn
};

use crate::calibre_metadata::{
  metadata_snapshot,
  score_good_enough
};
use crate::config::{
  CalibreConfig,
  Config,
  PathsConfig
};
use crate::dups::DuplicateGroup;

#[derive(Debug, Args)]
pub struct DedupArgs {
  /// JSON file produced by `chunkr
  /// dups`
  #[arg(long)]
  pub input: PathBuf,

  /// Minimum duplicate size (bytes)
  /// override
  #[arg(long)]
  pub min_size: Option<u64>,

  /// Force removals even when config
  /// says dry run
  #[arg(long)]
  pub dry_run: bool
}

pub fn run(
  config: &Config,
  args: &DedupArgs
) -> Result<()> {
  let settings = DedupSettings {
    min_size: args
      .min_size
      .unwrap_or(config.dedup.min_size),
    dry_run:  args.dry_run
      || config.dedup.dry_run
  };

  let connection =
    CalibreConnection::new(
      &config.calibre,
      &config.paths
    )?;
  let raw =
    fs::read_to_string(&args.input)
      .with_context(|| {
        format!(
          "read {}",
          args.input.display()
        )
      })?;
  let groups: Vec<DuplicateGroup> =
    serde_json::from_str(&raw)
      .with_context(|| {
        format!(
          "parse {}",
          args.input.display()
        )
      })?;

  let mut removed_ids = HashSet::new();
  let mut total_removed = 0usize;
  let mut total_removed_bytes = 0u64;
  for group in groups {
    if group.bytes < settings.min_size {
      continue;
    }
    let mut candidates = Vec::new();
    for path in &group.files {
      if let Some(id) =
        extract_book_id(path.as_path())
      {
        if removed_ids.contains(&id) {
          continue;
        }
        if let Ok(metadata) =
          fetch_metadata(
            &connection,
            id
          )
        {
          let snapshot =
            metadata_snapshot(
              &metadata
            );
          let (score, _) =
            score_good_enough(
              &snapshot,
              &config.calibre.scoring
            );
          candidates.push(Candidate {
            id,
            path: path.clone(),
            score
          });
        } else {
          warn!(
            path = %path.display(),
            "skipping duplicate due to metadata fetch error"
          );
        }
      } else {
        warn!(path = %path.display(), "unable to parse calibre book id");
      }
    }
    if candidates.len() <= 1 {
      continue;
    }
    candidates.sort_by(|a, b| {
      b.score.cmp(&a.score).then_with(
        || a.path.cmp(&b.path)
      )
    });
    let keep = &candidates[0];
    info!(
      group_bytes = group.bytes,
      keep_id = keep.id,
      keep_path = %keep.path.display(),
      keep_score = keep.score,
      "keeping duplicate candidate"
    );
    for cand in
      candidates.iter().skip(1)
    {
      if removed_ids.contains(&cand.id)
      {
        continue;
      }
      if settings.dry_run {
        info!(
          path = %cand.path.display(),
          score = cand.score,
          "dry-run: would remove duplicate book"
        );
      } else {
        remove_book(
          &connection,
          cand.id
        )?;
        info!(
          path = %cand.path.display(),
          book_id = cand.id,
          bytes = group.bytes,
          "removed duplicate book"
        );
      }
      removed_ids.insert(cand.id);
      total_removed += 1;
      total_removed_bytes +=
        group.bytes;
    }
  }

  info!(
    removed_files = total_removed,
    removed_bytes = total_removed_bytes,
    mode = if settings.dry_run {
      "dry-run"
    } else {
      "live"
    },
    "dedup summary"
  );
  Ok(())
}

struct DedupSettings {
  min_size: u64,
  dry_run:  bool
}

struct Candidate {
  id:    u64,
  path:  PathBuf,
  score: i32
}

struct CalibreConnection {
  global_opts: Vec<String>
}

impl CalibreConnection {
  fn new(
    calibre: &CalibreConfig,
    paths: &PathsConfig
  ) -> Result<Self> {
    let library = calibre
      .library_url
      .as_ref()
      .cloned()
      .or_else(|| {
        calibre
          .library_path
          .as_ref()
          .map(|p| {
            p.to_string_lossy()
              .to_string()
          })
      })
      .or_else(|| {
        Some(
          paths
            .calibre_root
            .to_string_lossy()
            .to_string()
        )
      })
      .unwrap();
    let mut opts = vec![
      "--with-library".to_string(),
      library,
    ];
    if let Some(username) =
      &calibre.content_server.username
    {
      opts
        .push("--username".to_string());
      opts.push(username.clone());
      if let Some(password) =
        &calibre.content_server.password
      {
        opts.push(
          "--password".to_string()
        );
        opts.push(password.clone());
      }
    }
    Ok(Self {
      global_opts: opts
    })
  }
}

fn fetch_metadata(
  connection: &CalibreConnection,
  book_id: u64
) -> Result<Value> {
  let fields = [
    "title",
    "authors",
    "publisher",
    "pubdate",
    "languages",
    "isbn",
    "identifiers",
    "tags",
    "comments",
    "cover"
  ]
  .join(",");
  let mut args =
    connection.global_opts.clone();
  args
    .push("show_metadata".to_string());
  args
    .push("--for-machine".to_string());
  args.push("--fields".to_string());
  args.push(fields);
  args.push(book_id.to_string());
  let output = run_calibredb(&args)?;
  if output.status_code != 0 {
    bail!(
      "calibredb show_metadata \
       failed: {}",
      output.stderr
    );
  }
  let value: Value =
    serde_json::from_str(
      &output.stdout
    )?;
  Ok(value)
}

fn remove_book(
  connection: &CalibreConnection,
  book_id: u64
) -> Result<()> {
  let mut args =
    connection.global_opts.clone();
  args.push("remove".to_string());
  args.push("--yes".to_string());
  args.push(book_id.to_string());
  let output = run_calibredb(&args)?;
  if output.status_code != 0 {
    bail!(
      "calibredb remove failed: {}",
      output.stderr
    );
  }
  Ok(())
}

fn run_calibredb(
  cmd: &[String]
) -> Result<CalibredbResult> {
  let mut command =
    Command::new("calibredb");
  for arg in cmd {
    command.arg(arg);
  }
  let output = command
    .output()
    .with_context(|| {
      format!(
        "failed to run calibredb: {}",
        cmd.join(" ")
      )
    })?;
  Ok(CalibredbResult {
    status_code: output
      .status
      .code()
      .unwrap_or(1),
    stdout:
      String::from_utf8_lossy(
        &output.stdout
      )
      .to_string(),
    stderr:
      String::from_utf8_lossy(
        &output.stderr
      )
      .to_string()
  })
}

struct CalibredbResult {
  status_code: i32,
  stdout:      String,
  stderr:      String
}

fn extract_book_id(
  path: &Path
) -> Option<u64> {
  for ancestor in path.ancestors() {
    if let Some(name) = ancestor
      .file_name()
      .and_then(|s| s.to_str())
      && let Some(start) =
        name.rfind('(')
      && name.ends_with(')')
    {
      let digits = &name
        [start + 1..name.len() - 1];
      if let Ok(id) =
        digits.parse::<u64>()
      {
        return Some(id);
      }
    }
  }
  None
}

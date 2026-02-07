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

  /// Dry-run mode (never delete)
  #[arg(long)]
  pub dry_run: bool
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

struct CalibreTarget {
  label:       String,
  global_opts: Vec<String>
}

struct CalibreConnection {
  targets: Vec<CalibreTarget>
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
      let Some(id) =
        extract_book_id(path.as_path())
      else {
        warn!(path = %path.display(), "unable to parse calibre book id");
        continue;
      };

      if removed_ids.contains(&id) {
        continue;
      }

      match fetch_metadata(
        &connection,
        id
      ) {
        | Ok(metadata) => {
          let snapshot =
            metadata_snapshot(
              &metadata
            );
          let (score, reasons) =
            score_good_enough(
              &snapshot,
              &config.calibre.scoring
            );
          info!(
            book_id = id,
            path = %path.display(),
            score,
            missing = reasons.join(", "),
            "dedup candidate scored"
          );
          candidates.push(Candidate {
            id,
            path: path.clone(),
            score
          });
        }
        | Err(err) => {
          warn!(
            book_id = id,
            path = %path.display(),
            error = %err,
            "skipping duplicate due to metadata fetch error"
          );
        }
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
          book_id = cand.id,
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

impl CalibreConnection {
  fn new(
    calibre: &CalibreConfig,
    paths: &PathsConfig
  ) -> Result<Self> {
    let mut targets = Vec::new();

    if let Some(url) = non_empty_url(
      calibre.library_url.as_deref()
    ) {
      targets.push(CalibreTarget {
        label:       format!(
          "server:{url}"
        ),
        global_opts: build_global_opts(
          &url,
          calibre
            .content_server
            .username
            .as_deref(),
          calibre
            .content_server
            .password
            .as_deref()
        )
      });
    }

    let local_library = non_empty_path(
      calibre.state_path.as_deref()
    )
    .or_else(|| {
      non_empty_path(
        calibre.library_path.as_deref()
      )
    })
    .unwrap_or_else(|| {
      paths.calibre_root.clone()
    });
    let local_str = local_library
      .to_string_lossy()
      .to_string();
    targets.push(CalibreTarget {
      label:       format!(
        "local:{local_str}"
      ),
      global_opts: build_global_opts(
        &local_str, None, None
      )
    });

    if targets.is_empty() {
      bail!(
        "no calibre connection target \
         configured"
      );
    }

    info!(
      target_count = targets.len(),
      first_target = targets
        .first()
        .map(|t| t.label.clone())
        .unwrap_or_default(),
      "calibre dedup connection \
       prepared"
    );
    Ok(Self {
      targets
    })
  }
}

fn build_global_opts(
  library: &str,
  username: Option<&str>,
  password: Option<&str>
) -> Vec<String> {
  let mut opts = vec![
    "--with-library".to_string(),
    library.to_string(),
  ];
  if (library.starts_with("http://")
    || library.starts_with("https://"))
    && username.is_some()
  {
    opts.push("--username".to_string());
    opts.push(
      username
        .unwrap_or_default()
        .to_string()
    );
    if let Some(pass) = password {
      opts
        .push("--password".to_string());
      opts.push(pass.to_string());
    }
  }
  opts
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

  let mut last_error: Option<
    anyhow::Error
  > = None;
  for target in &connection.targets {
    let mut args =
      target.global_opts.clone();
    args.push("list".to_string());
    args.push(
      "--for-machine".to_string()
    );
    args.push("--search".to_string());
    args.push(format!("id:{book_id}"));
    args.push("--fields".to_string());
    args.push(fields.clone());

    match run_calibredb(&args) {
      | Ok(output)
        if output.status_code == 0 =>
      {
        let rows: Vec<Value> =
          serde_json::from_str(
            &output.stdout
          )
          .with_context(
            || {
              format!(
                "parse list output \
                 for target {}",
                target.label
              )
            }
          )?;
        if let Some(row) =
          rows.into_iter().next()
        {
          return Ok(row);
        }
        last_error =
          Some(anyhow::anyhow!(
            "target {} returned no \
             rows for id {}",
            target.label,
            book_id
          ));
      }
      | Ok(output) => {
        last_error =
          Some(anyhow::anyhow!(
            "target {} failed \
             (rc={}): {}",
            target.label,
            output.status_code,
            output.stderr.trim()
          ));
      }
      | Err(err) => {
        last_error =
          Some(anyhow::anyhow!(
            "target {} exec error: {}",
            target.label,
            err
          ));
      }
    }
  }

  Err(last_error.unwrap_or_else(|| {
    anyhow::anyhow!(
      "no calibre target available"
    )
  }))
}

fn remove_book(
  connection: &CalibreConnection,
  book_id: u64
) -> Result<()> {
  let mut last_error: Option<
    anyhow::Error
  > = None;
  for target in &connection.targets {
    let mut args =
      target.global_opts.clone();
    args.push("remove".to_string());
    args.push("--yes".to_string());
    args.push(book_id.to_string());

    match run_calibredb(&args) {
      | Ok(output)
        if output.status_code == 0 =>
      {
        return Ok(())
      }
      | Ok(output) => {
        last_error =
          Some(anyhow::anyhow!(
            "target {} failed \
             (rc={}): {}",
            target.label,
            output.status_code,
            output.stderr.trim()
          ));
      }
      | Err(err) => {
        last_error =
          Some(anyhow::anyhow!(
            "target {} exec error: {}",
            target.label,
            err
          ));
      }
    }
  }

  Err(last_error.unwrap_or_else(|| {
    anyhow::anyhow!(
      "no calibre target available"
    )
  }))
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

fn non_empty_path(
  path: Option<&Path>
) -> Option<PathBuf> {
  path
    .filter(|p| {
      !p.as_os_str().is_empty()
    })
    .map(PathBuf::from)
}

fn non_empty_url(
  url: Option<&str>
) -> Option<String> {
  let v = url.unwrap_or("").trim();
  if v.is_empty() {
    None
  } else {
    Some(v.to_string())
  }
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

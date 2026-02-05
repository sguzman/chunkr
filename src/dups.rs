use std::collections::HashMap;
use std::fs::File;
use std::io::{
  BufReader,
  Read
};
use std::path::{
  Path,
  PathBuf
};
use std::time::Instant;

use anyhow::{
  Context,
  Result
};
use blake3::Hasher;
use clap::Args;
use rayon::prelude::*;
use serde::Serialize;
use tracing::{
  debug,
  info,
  warn
};
use walkdir::{
  DirEntry,
  WalkDir
};

use crate::config::DupsOutputFormat;

#[derive(Debug, Args)]
pub struct DupsArgs {
  /// Calibre library root (falls back
  /// to config.paths.calibre_root)
  #[arg(long)]
  pub library: Option<PathBuf>,

  /// Output format (text or json)
  #[arg(long, value_enum)]
  pub output: Option<DupsOutputFormat>,

  /// Optional path to write the report
  /// (defaults to stdout)
  #[arg(long)]
  pub out: Option<PathBuf>,

  /// Extensions to include (repeatable
  /// flag)
  #[arg(long)]
  pub ext: Vec<String>,

  /// Follow symbolic links when
  /// walking the library tree
  #[arg(long, default_value_t = false)]
  pub follow_symlinks: bool,

  /// Number of hashing threads (0 =
  /// rayon default)
  #[arg(long, default_value_t = 0)]
  pub threads: usize,

  /// Minimum file size in bytes
  #[arg(long, default_value_t = 0)]
  pub min_size: u64,

  /// Include Calibre sidecar files
  /// like metadata.opf/cover.jpg
  #[arg(long, default_value_t = false)]
  pub include_sidecars: bool
}

#[derive(Debug, Clone)]
pub struct DupsSettings {
  pub output: DupsOutputFormat,
  pub out:              Option<PathBuf>,
  pub ext:              Vec<String>,
  pub follow_symlinks:  bool,
  pub threads:          usize,
  pub min_size:         u64,
  pub include_sidecars: bool
}

#[derive(Debug, Clone, Serialize)]
struct FileInfo {
  path:   PathBuf,
  bytes:  u64,
  blake3: String
}

#[derive(Debug, Serialize)]
struct DuplicateGroup {
  bytes:  u64,
  blake3: String,
  files:  Vec<PathBuf>
}

pub fn run(
  config: &crate::config::Config,
  args: &DupsArgs
) -> Result<()> {
  let library_root = args
    .library
    .clone()
    .unwrap_or_else(|| {
      config.paths.calibre_root.clone()
    });

  let ext = if args.ext.is_empty() {
    normalize_exts(&config.dups.ext)
  } else {
    normalize_exts(&args.ext)
  };

  let settings = DupsSettings {
    output: args
      .output
      .unwrap_or(config.dups.output),
    out: args.out.clone().or_else(
      || config.dups.out.clone()
    ),
    ext,
    follow_symlinks: args
      .follow_symlinks
      || config.dups.follow_symlinks,
    threads: if args.threads == 0 {
      config.dups.threads
    } else {
      args.threads
    },
    min_size: if args.min_size == 0 {
      config.dups.min_size
    } else {
      args.min_size
    },
    include_sidecars: args
      .include_sidecars
      || config.dups.include_sidecars
  };

  run_dups(&library_root, &settings)
}

fn normalize_exts(
  inputs: &[String]
) -> Vec<String> {
  inputs
    .iter()
    .map(|s| {
      s.trim()
        .trim_start_matches('.')
        .to_ascii_lowercase()
    })
    .filter(|s| !s.is_empty())
    .collect()
}

pub fn run_dups(
  library: &Path,
  settings: &DupsSettings
) -> Result<()> {
  if settings.threads > 0 {
    info!(
      threads = settings.threads,
      "Configuring Rayon thread pool"
    );
    rayon::ThreadPoolBuilder::new()
      .num_threads(settings.threads)
      .build_global()
      .context(
        "Failed to configure Rayon \
         global thread pool"
      )?;
  }

  let started = Instant::now();

  let exts = if settings.ext.is_empty()
  {
    default_exts()
  } else {
    settings.ext.clone()
  };

  info!(
      library = %library.display(),
      follow_symlinks = settings.follow_symlinks,
      include_sidecars = settings.include_sidecars,
      min_size = settings.min_size,
      exts = ?exts,
      "Starting duplicate scan"
  );

  let candidates = collect_candidates(
    library,
    &exts,
    settings.follow_symlinks,
    settings.min_size,
    settings.include_sidecars
  )?;

  info!(
    count = candidates.len(),
    "Collected candidate files"
  );

  let hashed: Vec<FileInfo> = candidates
        .par_iter()
        .map(|path| hash_one(path))
        .filter_map(|r| match r {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(error = %e, "Skipping file due to error");
                None
            }
        })
        .collect();

  info!(
    count = hashed.len(),
    "Finished hashing files"
  );

  let dupes = find_duplicates(hashed);

  info!(
    groups = dupes.len(),
    elapsed_ms =
      started.elapsed().as_millis(),
    "Done"
  );

  match settings.output {
    | DupsOutputFormat::Text => {
      print_text(
        &dupes,
        settings.out.as_deref()
      )?
    }
    | DupsOutputFormat::Json => {
      print_json(
        &dupes,
        settings.out.as_deref()
      )?
    }
  }

  Ok(())
}

fn default_exts() -> Vec<String> {
  [
    "epub", "pdf", "mobi", "azw",
    "azw3", "djvu", "fb2", "rtf",
    "txt", "doc", "docx", "cbz", "cbr"
  ]
  .into_iter()
  .map(String::from)
  .collect()
}

fn is_sidecar(name: &str) -> bool {
  matches!(
    name,
    "metadata.opf"
      | "cover.jpg"
      | "cover.jpeg"
      | "cover.png"
  )
}

fn want_entry(
  entry: &DirEntry,
  exts: &[String],
  min_size: u64,
  include_sidecars: bool
) -> bool {
  if !entry.file_type().is_file() {
    return false;
  }

  let path = entry.path();

  if min_size > 0 {
    if let Ok(md) = path.metadata() {
      if md.len() < min_size {
        return false;
      }
    }
  }

  let file_name = match path
    .file_name()
    .and_then(|s| s.to_str())
  {
    | Some(s) => s,
    | None => return false
  };

  if include_sidecars
    && is_sidecar(file_name)
  {
    return true;
  }

  let ext = match path
    .extension()
    .and_then(|s| s.to_str())
  {
    | Some(s) => s.to_ascii_lowercase(),
    | None => return false
  };

  exts.iter().any(|e| e == &ext)
}

fn collect_candidates(
  library: &Path,
  exts: &[String],
  follow_symlinks: bool,
  min_size: u64,
  include_sidecars: bool
) -> Result<Vec<PathBuf>> {
  let mut out = Vec::new();

  let walker = WalkDir::new(library)
    .follow_links(follow_symlinks)
    .into_iter();

  for entry in walker {
    let entry = match entry {
      | Ok(e) => e,
      | Err(e) => {
        warn!(error = %e, "WalkDir error");
        continue;
      }
    };

    if want_entry(
      &entry,
      exts,
      min_size,
      include_sidecars
    ) {
      out.push(
        entry.path().to_path_buf()
      );
    } else {
      debug!(path = %entry.path().display(), "Skipping");
    }
  }

  Ok(out)
}

fn hash_one(
  path: &Path
) -> Result<FileInfo> {
  let md = path
    .metadata()
    .with_context(|| {
      format!(
        "Failed to stat {}",
        path.display()
      )
    })?;
  let bytes = md.len();

  let file = File::open(path)
    .with_context(|| {
      format!(
        "Failed to open {}",
        path.display()
      )
    })?;
  let mut reader =
    BufReader::with_capacity(
      1024 * 1024,
      file
    );

  let mut hasher = Hasher::new();
  let mut buf = vec![0u8; 1024 * 1024];

  loop {
    let n = reader
      .read(&mut buf)
      .with_context(|| {
        format!(
          "Failed to read {}",
          path.display()
        )
      })?;
    if n == 0 {
      break;
    }
    hasher.update(&buf[..n]);
  }

  let digest = hasher.finalize();
  let blake3_hex =
    digest.to_hex().to_string();

  Ok(FileInfo {
    path: path.to_path_buf(),
    bytes,
    blake3: blake3_hex
  })
}

fn find_duplicates(
  files: Vec<FileInfo>
) -> Vec<DuplicateGroup> {
  let mut map: HashMap<
    (u64, String),
    Vec<PathBuf>
  > = HashMap::new();

  for f in files {
    map
      .entry((
        f.bytes,
        f.blake3.clone()
      ))
      .or_default()
      .push(f.path);
  }

  let mut groups: Vec<DuplicateGroup> =
    map
      .into_iter()
      .filter_map(
        |(
          (bytes, blake3),
          mut paths
        )| {
          if paths.len() >= 2 {
            paths.sort();
            Some(DuplicateGroup {
              bytes,
              blake3,
              files: paths
            })
          } else {
            None
          }
        }
      )
      .collect();

  groups.sort_by(|a, b| {
    b.files
      .len()
      .cmp(&a.files.len())
      .then_with(|| {
        b.bytes.cmp(&a.bytes)
      })
      .then_with(|| {
        a.blake3.cmp(&b.blake3)
      })
  });

  groups
}

fn print_text(
  groups: &[DuplicateGroup],
  out: Option<&Path>
) -> Result<()> {
  let mut buf = String::new();
  if groups.is_empty() {
    buf.push_str(
      "No duplicates found (by \
       full-file BLAKE3 hash).\n"
    );
  } else {
    buf.push_str(&format!(
      "Duplicate groups: {}\n\n",
      groups.len()
    ));
    for (i, g) in
      groups.iter().enumerate()
    {
      buf.push_str(&format!(
        "== Group {}: {} files | {} \
         bytes | blake3 {} ==\n",
        i + 1,
        g.files.len(),
        g.bytes,
        g.blake3
      ));
      for p in &g.files {
        buf.push_str(&format!(
          "  - {}\n",
          p.display()
        ));
      }
      buf.push('\n');
    }
  }
  write_output(&buf, out)?;
  Ok(())
}

fn print_json(
  groups: &[DuplicateGroup],
  out: Option<&Path>
) -> Result<()> {
  let s = serde_json::to_string_pretty(
    groups
  )?;
  write_output(&s, out)?;
  Ok(())
}

fn write_output(
  contents: &str,
  out: Option<&Path>
) -> Result<()> {
  if let Some(path) = out {
    let mut file =
      std::fs::File::create(path)
        .with_context(|| {
          format!(
            "Failed to create {}",
            path.display()
          )
        })?;
    use std::io::Write;
    file
      .write_all(contents.as_bytes())?;
    file.write_all(b"\n")?;
  } else {
    println!("{contents}");
  }
  Ok(())
}

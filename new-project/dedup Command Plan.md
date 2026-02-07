---
title: dedup Command Plan
type: note
permalink: dedup-command-plan
---

**Purpose**: Reduce duplicate files by keeping one representative per group and
filtering small files **Key Changes**:

1. Add new CLI subcommand `dedup` to main.rs:

```rust
#[derive(Debug, Subcommand)]
enum Commands {
  // ... existing commands ...
  Dedup(DedupArgs),
}
```

2. Create `src/dedup.rs` with:

```rust
pub struct DedupArgs {
  #[arg(long)]
  pub input: PathBuf,

  #[arg(long, default_value_t = 0)]
  pub min_size: u64,
}

pub fn run(config: &Config, args: &DedupArgs) -> anyhow::Result<()> {
  let groups = read_dups_json(&args.input)?;

  let mut cleaned_groups = Vec::new();
  for group in groups {
    // Filter files by min_size
    let filtered = group.files.into_iter()
      .filter(|f| f.metadata().map(|m| m.len()) >= Some(args.min_size))
      .collect::<Vec<_>>();

    if !filtered.is_empty() {
      // Keep smallest file in the group (or any other selection)
      let selected = filtered.into_iter()
        .min_by(|a, b| a.metadata().map(|m1| m1.len()).unwrap_or(0).cmp(&b.metadata().map(|m2| m2.len()).unwrap_or(0)))
        .unwrap();

      cleaned_groups.push(DuplicateGroup {
        bytes: 0,
        blake3: group.blake3.clone(),
        files: vec![selected],
      });
    }
  }

  write_dups_json(&cleaned_groups, &args.input)
}
```

**Documentation Changes**: Add to `docs/reference/ai/README.md`:

```markdown
## Dedup
Remove duplicate entries by keeping one per group and filtering small files.

### Usage
```

dedup [OPTIONS] DUPS_JSON_FILE

```text

### Options
- `--min-size SIZE` Minimum file size in bytes (default: 0)

Example:
```bash
chunkr dedup dups.json --min-size 1024
```

**Config**: No config changes needed - uses CLI arguments for min_size.

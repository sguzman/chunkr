# Hyperfine Benchmarks

Use `hyperfine` to benchmark Cite-Otter against the Ruby AnyStyle CLI and to
capture Rust-only performance baselines.

## Requirements

- Install `hyperfine` and ensure `anystyle` or `anystyle-cli` is on your PATH.
- Ensure `tmp/book.txt` is present (used as the shared benchmark input).
- For parity runs, keep the Ruby repo in `tmp/anystyle`.

## Parity Benchmarks (Ruby Vs Rust)

Run the parity suite with:

```bash
just bench-ruby-parity
```

This writes JSON reports to:

- `target/reports/benchmark-ruby-parity.json`
- `target/reports/benchmark-ruby-parity-training.json` (train/check/delta)

## Rust Baseline Benchmarks

Capture a Rust-only baseline with:

```bash
just bench-rust-baseline
```

This writes JSON output to:

- `target/reports/benchmark-rust-baseline.json`

## Environment Overrides

- `BOOK_PATH` (default: `tmp/book.txt`)
- `RUBY_REPO` (default: `tmp/anystyle`)
- `OUT_DIR` (default: `target/reports`)
- `ANYSTYLE_CMD` (override the AnyStyle CLI command)
- `FAST_RUNS` (default: `3`)
- `TRAINING_RUNS` (default: `1`)
- `ENABLE_TRAINING_BENCHMARKS` (default: `1`)
- `PARSER_PATTERN` (default: `tmp/anystyle/res/parser/core.xml`)
- `FINDER_PATTERN` (default: `tmp/anystyle/res/finder/*.ttx`)
- `RUST_CMD` (default: `cargo run --quiet --bin cite-otter --`)
- `HYPERFINE_ARGS` (extra hyperfine flags, e.g. `--warmup 0`)
- `RUBY_TRAIN_CMD` (override Ruby train command)
- `RUBY_CHECK_CMD` (override Ruby check command)
- `RUBY_DELTA_CMD` (override Ruby delta command)

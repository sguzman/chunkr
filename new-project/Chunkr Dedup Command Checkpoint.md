---
title: Chunkr Dedup Command Checkpoint
type: note
permalink: chunkr-dedup-command-checkpoint
---

- **Goal**: Add `dedup` command to manage duplicate files by keeping one
  representative per group and removing small entries.
- **Established facts**:
  - Chunkr processes CFR docs into structured chunks
  - Existing `dups` command outputs JSON with duplicate groups
  - Dedup uses CLI argument `--min-size` for filtering (no config changes)
  - Keep smallest file per group as representative
- **Decisions made**:
  - Use CLI args instead of config for `min-size`
  - Select smallest file in each group to minimize size
- **Current status**: ✅ All src files summarized as notes ✅ Dedup command design
  documented ❌ Dedup implementation not written ❌ Dedup tests not created
- **Next 5 actions**:
  1. Implement `dedup.rs` with filtering and selection logic
  2. Write unit tests for dedup's size handling
  3. Update docs/README.md with dedup examples
  4. Test dedup on sample dups.json from examples
  5. Integrate dedup into main command flow
- **Open questions**:
  - Should dedup consider file content when selecting representatives?
  - How to handle groups where all files are below min_size?

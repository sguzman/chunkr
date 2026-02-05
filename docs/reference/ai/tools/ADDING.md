# Adding Tools (AI Guidelines)

When adding a new tool to this repo:

1. Update the post-change script and docs

- Add the tool to `scripts/post-change.sh` if it is part of
  validation/formatting.
- Add the tool to `docs/ai/POST-CHANGES.md`.

2. Update the tooling lists

- Add the tool to the tooling lists in `README.md` and `docs/ai/README.md`.

3. Add any required config files

- Include a default config file (if the tool uses one) at the repo root.
- Document the config file location in `README.md` if it affects usage.

4. Wire into Justfile

- Add a `just` target for the tool if it is part of routine checks.

5. Verify build only

- Run `cargo build` after changes.

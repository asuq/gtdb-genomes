# 2026-03-21 multi-value `--gtdb-taxon`

## Commits

- `e886015` `feat(cli): accept multiple taxa per flag`
- `49cf6f1` `docs(cli): align docs with multi-value taxa`

## Why this changed

The README already showed one `--gtdb-taxon` flag followed by multiple taxa.
The parser only accepted one value per flag, so the documented example and the
actual CLI contract had diverged.

The goal of this change was to accept both forms below without changing the
internal workflow interface:

```bash
gtdb-genomes --gtdb-taxon g__Escherichia g__Bacillus --outdir results
gtdb-genomes --gtdb-taxon g__Escherichia --gtdb-taxon g__Bacillus --outdir results
```

The workflow still receives a flat `CliArgs.gtdb_taxa` tuple in first-seen
order with duplicates removed.

## Design choices

- `argparse` now uses `nargs="+"` together with `action="append"` for
  `--gtdb-taxon`. This keeps repeated flags working while allowing one or more
  taxa after each occurrence.
- Taxa are flattened during CLI normalisation instead of changing downstream
  workflow code. This kept the change local to the command-line boundary.
- Each parsed value is validated as one complete GTDB taxon token before it
  reaches the workflow. This preserves the previous safety property that
  unquoted shell-split species input fails at parse time.

## Validation rule

The CLI now accepts a parsed taxon value only when:

- it starts with a recognised GTDB rank prefix: `d__`, `p__`, `c__`, `o__`,
  `f__`, `g__`, or `s__`
- non-species taxa contain no internal whitespace
- species taxa contain internal whitespace, which means users must still quote
  them in the shell

This means `--gtdb-taxon s__Altiarchaeum hamiconexum` still fails, while
`--gtdb-taxon "s__Altiarchaeum hamiconexum"` succeeds.

## Risks and assumptions

- This rule assumes species-level requests are represented as two-part GTDB
  tokens with internal whitespace, which matches the current documentation and
  examples in this repository.
- The README wording for `--gtdb-release` and `--dry-run` was intentionally
  kept concise. The exact runtime contract remains in `docs/usage-details.md`.
- Help text assertions needed to tolerate `argparse` line wrapping instead of
  checking one long literal line.

## Verification

Commands run in the `gtdb-genome` environment:

```bash
mamba run -n gtdb-genome uv run --group dev pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_entrypoints.py
mamba run -n gtdb-genome uv run --group dev pytest -q
```

Results:

- targeted CLI and docs tests: `35 passed in 1.14s`
- full suite: `195 passed in 2.92s`

# Usage Details

This document is the detailed user-facing reference for `gtdb-genomes` CLI
behaviour, output layout, retry rules, GTDB taxonomy data handling, and
runtime contract.

## Table Of Contents

- [Command Form](#command-form)
- [Options](#options)
- [API Key Handling](#api-key-handling)
- [Output Layout](#output-layout)
- [Summary Files](#summary-files)
- [NCBI datasets CLI](#ncbi-datasets-cli)
- [Retry Policy](#retry-policy)
- [Runtime Contract](#runtime-contract)
- [GTDB Taxonomy Data](#bundled-gtdb-taxonomy)
- [Failure Handling](#failure-handling)
- [Known Limitations](#known-limitations)

## Command Form

```text
usage: gtdb-genomes -t GTDB_TAXON [GTDB_TAXON ...] -o OUTDIR [-h] [-r GTDB_RELEASE] [--prefer-genbank] [--version-latest] [-j THREADS] [--ncbi-api-key NCBI_API_KEY] [--include INCLUDE] [--debug] [--keep-tmp] [-d]

Download NCBI genomes by GTDB taxon and GTDB release

mandatory options:
  -t GTDB_TAXON [GTDB_TAXON ...], --gtdb-taxon GTDB_TAXON [GTDB_TAXON ...]
                        Exact GTDB taxon. You can give one or more values
                        after the flag and repeat it as needed. Quote species
                        names with spaces, for example "s__Altiarchaeum
                        hamiconexum"
  -o OUTDIR, --outdir OUTDIR
                        Output directory for the run

optional options:
  -h, --help            show this help message and exit
  -r GTDB_RELEASE, --gtdb-release GTDB_RELEASE
                        GTDB release alias or included release identifier;
                        default: latest
  --prefer-genbank      Prefer paired GenBank accessions discovered from
                        current NCBI metadata and, by default, keep the exact
                        selected versioned accession
  --version-latest      Request the latest available revision in the selected
                        paired GenBank family when explicit pairing is
                        available, otherwise in the selected accession family
                        from current NCBI metadata; requires --prefer-genbank
  -j THREADS, --threads THREADS
                        Choose the worker count used by compatible workflow
                        steps; direct downloads remain serial; default: 8
  --ncbi-api-key NCBI_API_KEY
                        NCBI API key used only for datasets commands;
                        overrides NCBI_API_KEY from the environment; the tool
                        does not write it to its own logs or manifests
  --include INCLUDE     Comma-separated datasets include values; must contain
                        genome
  --debug               Enable debug logging; cannot be used while an NCBI API
                        key is active
  --keep-tmp            Keep intermediate working files
  -d, --dry-run         Resolve inputs without downloading genome payloads;
                        still preflights unzip so real-run archive
                        requirements fail fast
```

Running `gtdb-genomes` with no arguments shows this full help text and exits
successfully.

## Options

### Required options

- `-t`, `--gtdb-taxon`: Give one complete GTDB taxon per value. You can pass
  several values after one flag and repeat the flag as needed. A row is
  selected only when its GTDB lineage contains the requested taxon exactly
  after trimming surrounding whitespace. Matching is case-sensitive, internal
  species whitespace is preserved, and suffix variants stay separate taxa. For
  example, `g__Frigididesulfovibrio` does not match
  `g__Frigididesulfovibrio_A`. Species names contain spaces, so quote them:
  `--gtdb-taxon "s__Altiarchaeum hamiconexum"` or
  `--gtdb-taxon g__Escherichia "s__Escherichia coli"`. Unquoted input such as
  `--gtdb-taxon s__Altiarchaeum hamiconexum` is invalid.

- `-o`, `--outdir`: The output directory must not exist or must already be
  empty.
  The tool does not merge into or overwrite a populated output tree.

### Release and accession choice

- `-r`, `--gtdb-release`: Defaults to `latest`. Accepted local aliases include
  `latest`, `80`, `95`, `214`, `226`, `220.0`, and `release220/220.0`.

  The `latest` alias is resolved from the local manifest row marked with
  `is_latest=true`. GTDB release resolution never contacts GTDB over the
  network.

- `--prefer-genbank`: Disabled by default. When enabled, a requested `GCF_*`
  accession triggers NCBI metadata lookup. The workflow first uses explicit
  paired-assembly metadata from the RefSeq summary record when that metadata
  is complete and usable. If explicit pairing is unavailable, it falls back to
  the current NCBI candidate set for `GCA_*` accessions that share the same
  numeric assembly identifier. By default, the request keeps the exact
  selected versioned accession. This is a live NCBI optimisation, not a
  frozen GTDB-release-preserving transform.

- `--version-latest`: Disabled by default. Requires `--prefer-genbank`. Drops
  the version suffix from the selected accession and asks `datasets` for the
  latest available revision in that accession family from current NCBI
  metadata. When complete explicit paired-assembly metadata are available, the
  latest choice stays inside that paired GenBank family. If explicit pairing
  conflicts with the heuristic family view, the workflow falls back
  conservatively to the original accession. The realised accession may differ
  from the selected RefSeq or GenBank version and may change over time.

### Planning and execution

- Download planning is automatic. There is no user-facing flag to force direct
  or dehydrated mode.

  Rules:

  - supported requests always go through the automatic planner
  - the planner switches to dehydrate when the request contains 1,000 or more
    unique `datasets` request accessions after accession rewriting
  - this planner intentionally stays count-only for this project and does not
    implement the generic `datasets` `> 15 GB` heuristic because the workflow
    targets prokaryote genome downloads and treats the request-accession count as
    the governing operational limit
  - smaller supported requests use batch direct
    `datasets download genome accession --inputfile ... --filename ...` passes
  - direct-mode workflow retries run in waves: every batch in the current wave
    finishes before the next wave starts
  - unresolved multi-request direct batches are bisected only between waves,
    while unresolved singleton request accessions carry forward unchanged
  - metadata-confirmed suppressed-only direct groups get 2 total workflow
    waves; any direct group containing a non-suppressed accession gets 4 total
    workflow waves
  - if paired-`GCA_*` candidate metadata lookup fails or stays incomplete
    during `--prefer-genbank` planning, the workflow falls back to the
    original accession and records the corresponding metadata fallback status
    plus metadata failure rows
  - if a preferred `GCA_*` request remains unresolved after its preferred
    direct passes, the workflow may fall back to the original accession and
    records `downloaded_after_fallback` plus
    `paired_to_gca_fallback_original_on_download_failure`
  - if a batch dehydrated download exhausts its retry budget, or if unzip or
    batch rehydrate fails, the tool falls back to batch direct downloads and
    records `dehydrate_fallback_direct` as the final method used

- `-j`, `--threads`: Sets the worker count for the steps that can use it.
  Default:
  8. Direct downloads remain serial in the current workflow.

- `--keep-tmp`: Keeps intermediate working files instead of cleaning them up
  at the end of the run. This is mainly useful when you need to inspect a
  failed run or keep the downloaded archives and working directories.

### Data selection and debugging

- `--ncbi-api-key`: You can pass an API key here or set `NCBI_API_KEY` in the
  environment. If both are present, the explicit flag wins. The tool passes
  only the effective key to child `datasets` processes through the child
  process environment and does not use it for GTDB release resolution, local
  taxonomy loading, or any other use.

- `--include`: Defaults to `genome`.

  The value is passed to `datasets download genome accession --include` after
  a light validation step. In `gtdb-genomes`, `genome` is mandatory and the
  accepted values are `genome`, `gff3`, and `protein`. The upstream Datasets
  CLI accepts more include values; see
  [Download a genome data package](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/how-tos/genomes/download-genome/)
  for the broader `--include` behaviour.

  Examples:

  - `genome`
  - `genome,gff3`
  - `genome,gff3,protein`

- `--debug`: Enables debug-level logging, prints redacted command traces, and
  writes a redacted `OUTPUT/debug.log` for real runs. Console logs prefix each
  record with `HH:MM:SS` and colourise level labels on interactive terminals,
  while `debug.log` stays plain text. It cannot be combined with an effective
  NCBI API key because upstream `datasets` debug output may expose the API key
  header. `--debug --dry-run` is allowed when no effective NCBI API key is
  active, but dry runs keep debug output on the console and do not create
  `OUTPUT/debug.log`.

- `-d`, `--dry-run`: Resolves inputs without creating the final output tree or
  downloading genome payloads. It may resolve the local GTDB release, read
  the included GTDB taxonomy TSVs and the local release manifest, preflight
  `unzip` so the runtime contract matches real runs, and perform NCBI metadata
  lookup when `--prefer-genbank` is enabled and the selected rows include
  supported non-`UBA*` accessions. Zero-match runs and unsupported-`UBA*`-only
  runs still avoid NCBI calls, but dry runs still preflight `unzip` before
  they exit.

## API Key Handling

Using `NCBI_API_KEY` from the environment is the normal workflow path.
`--ncbi-api-key` is an explicit override. The effective key is passed only to
child `datasets` processes through the child environment.

The tool:

- never writes the API key into manifests or its own debug log
- redacts recognised key-bearing forms and known literal API-key values from
  recorded command traces and error messages
- forbids `--debug` while an effective NCBI API key is active because upstream
  `datasets` debug output can expose the raw API key header

Known limitation:

- if a user types the API key directly on the shell command line, shell
  history or inspection of the parent `gtdb-genomes` process may still expose
  it outside the control of this tool, so `NCBI_API_KEY` in the environment
  is the safer default

## Output Layout

```text
OUTPUT/
|-- accession_map.tsv
|-- download_failures.tsv
|-- duplicated_genomes.tsv
|-- run_summary.log
|-- taxon_summary.tsv
|-- debug.log                  # only when --debug is used
`-- taxa/
    |-- g__Escherichia/
    |   |-- taxon_accessions.tsv
    |   `-- GCA_000005845.2/
    `-- s__Escherichia_coli/
        |-- taxon_accessions.tsv
        `-- GCA_000005845.2/
```

Layout rules:

- manifests are written directly under `OUTPUT/`
- per-taxon accession manifests are written directly under each taxon directory
- there is no shared `OUTPUT/genomes/` directory
- duplicate genomes across requested taxa are copied into each matching taxon
  directory and logged
- populated output directories are rejected instead of being resumed in place
- each accession directory keeps the full downloaded payload requested through
  `datasets`
- versioned request accessions must resolve to the exact realised accession
  directory after extraction
- only versionless request accessions, such as the stem requests emitted by
  `--version-latest`, may accept a unique same-family realised version during
  post-extraction payload discovery

Taxon slugs preserve the GTDB taxon text where practical, replace unsafe
characters with `_`, and append a short hash suffix only when two taxa would
otherwise collide.

## Summary Files

- `run_summary.log`
  - one human-readable summary per run
  - records requested and resolved release, chosen method, actual concurrency,
    worker usage, counts, output path, and exit code as labelled lines
  - fixed keys: [run_summary.log](summary-files/run_summary.log.txt)
- `taxon_summary.tsv`
  - one row per requested taxon
  - records accession counts, duplicate-copy count, and output directory
  - fixed columns: [taxon_summary.tsv](summary-files/taxon_summary.tsv.txt)
- `accession_map.tsv`
  - one row per unique accession outcome
  - records the realised accession first, then the grouped requested taxa,
    grouped GTDB accessions, grouped selected and requested accession tokens,
    conversion status, grouped output paths, and download status
  - fixed columns: [accession_map.tsv](summary-files/accession_map.tsv.txt)
- `download_failures.tsv`
  - one row per terminal failed accession
  - records the accession identifier, grouped taxa, grouped GTDB accessions,
    whether the accession was suppressed, failure stage, error type, redacted
    reason, and terminal status
  - fixed columns: [download_failures.tsv](summary-files/download_failures.tsv.txt)
- `duplicated_genomes.tsv`
  - one row per realised accession that appears in more than one requested
    taxon
  - records the accession, grouped requested taxa, taxon count, and grouped
    output paths
  - fixed columns: [duplicated_genomes.tsv](summary-files/duplicated_genomes.tsv.txt)
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - one row per accession assigned to that taxon
  - records lineage, accession mapping, output path, and whether the accession
    is duplicated across taxa
  - fixed columns: [taxon_accessions.tsv](summary-files/taxon_accessions.tsv.txt)

## NCBI datasets CLI

`gtdb-genomes` does not download genomes directly from Python code. It delegates
NCBI-facing work to the NCBI `datasets` CLI. Upstream project:
[ncbi/datasets](https://github.com/ncbi/datasets).

The tool uses `datasets` for:

- `datasets summary genome accession` during metadata lookup
- direct batch `datasets download genome accession --inputfile ... --filename ...`
  passes for smaller requests
- batch dehydrated `datasets download genome accession --inputfile ...` runs for
  larger requests
- `datasets rehydrate` after a dehydrated batch download

GTDB release resolution and GTDB taxonomy loading remain local. Runtime
release selection does not contact GTDB over the network.

`unzip` is required because `datasets` produces zip archives that
`gtdb-genomes` extracts before reorganising the final output tree.

Tool requirements are resolved after GTDB release loading and taxonomy
selection. Missing external tools therefore affect only the execution paths
that actually need them.

## Retry Policy

Every internet-facing `datasets` step gets one initial attempt plus up to three
retries, using fixed backoff delays of 5 s, 15 s, and 45 s.

This applies to:

- `datasets summary genome accession`
- direct batch `datasets download genome accession --inputfile ... --filename ...`
- batch dehydrated `datasets download genome accession --inputfile ...`
- `datasets rehydrate`

Local unzip, local file parsing, and manifest writing are not retried.

Direct-mode layout resolution adds a separate workflow-level retry scheduler on
top of the command retry budget. A supported direct request starts with
`direct_batch_1`, and later numbered `direct_batch_N` or
`direct_fallback_batch_N` labels reflect execution order, not a fixed retry cap.
The scheduler runs in waves: every batch in the current wave completes before
any next-wave retry or split begins. Unresolved multi-request batches are
bisected only between waves, while unresolved singleton request accessions are
retried unchanged in the next wave.

Workflow-level direct wave budgets depend on suppression status:

- direct groups containing any non-suppressed accession get 4 total workflow
  waves
- suppressed-only direct groups get 2 total workflow waves

This wave policy applies to both ordinary direct runs and
dehydrated-to-direct fallback. Metadata-confirmed suppressed genomes may no
longer be downloadable from NCBI, especially in older GTDB releases.

When upstream `datasets` output includes parseable percentages, real runs log
10% progress milestones for direct downloads, dehydrated batch downloads, and
`datasets rehydrate`. These messages are log-based hints from upstream output,
not synthetic estimates, so some runs or platforms may emit only the usual
start and completion logs.

## Runtime Contract

Exit codes:

- `0`: full success
- `2`: CLI usage or validation error
- `3`: local GTDB data error
- `4`: zero matches for all requested taxa
- `5`: external tool or preflight error
- `6`: partial failure with at least one successful genome
- `7`: planning or runtime failure with no successful genomes
- `8`: local final-output materialisation failure
- `9`: unexpected internal failure

Status values:

- `conversion_status`
  - `unchanged_original`
  - `paired_to_gca`
  - `metadata_lookup_failed_fallback_original`
  - `paired_gca_metadata_incomplete_fallback_original`
  - `paired_gca_conflict_fallback_original`
  - `paired_gca_suppressed_fallback_original`
  - `paired_to_gca_fallback_original_on_download_failure`
  - `failed_no_usable_accession`
- `download_status`
  - `downloaded`
  - `downloaded_after_fallback`
  - `failed`
- `download_failures.tsv.stage`
  - `preflight`
  - `metadata_lookup`
  - `preferred_download`
  - `fallback_download`
  - `layout`
  - `rehydrate`
- `download_failures.tsv.status`
  - `retry_scheduled`
  - `retry_exhausted`
  - `unsupported_input`

Fixed key and column references for all summary files live under
[Summary Files](#summary-files) and the linked per-file references.

## Bundled GTDB Taxonomy

GTDB taxonomy tables are stored as compressed `.tsv.gz` files and are
decompressed transparently at read time. Runtime release resolution stays
local and does not fetch from GTDB.

Source checkout layout:

```text
data/gtdb_taxonomy/releases.tsv
data/gtdb_taxonomy/<resolved_release>/
```

`releases.tsv` remains plain text by design so the manifest stays easy to
inspect and validate. It carries both the runtime release mapping columns and
build-only UQ mirror metadata used by the bootstrap flow.

Fresh source checkouts do not track the generated `<resolved_release>/`
payload directories in Git. Before GTDB-dependent maintainer or source-
checkout runs, build the local runtime payload with:

```bash
uv run python -m gtdb_genomes.bootstrap_taxonomy
```

The bootstrap step downloads the configured taxonomy files from the HTTPS UQ
mirror release directory recorded in `releases.tsv`, verifies each source file
against the release `MD5SUM` or `MD5SUM.txt` listing, and materialises the
local `.tsv.gz` runtime layout. That source-checkout bootstrap authenticity
boundary is limited by the upstream-published MD5 listing. This bootstrap path
is for maintainers and source checkouts; packaged runtimes already include the
generated files. Community packaging and downstream redistribution should use
the tagged release `sdist`, not a repository snapshot.

Maintainers can refresh the build-only mirror metadata for the existing release
rows with:

```bash
uv run python -m gtdb_genomes.refresh_taxonomy_manifest
```

Built wheels, sdists, and Conda packages already include the generated GTDB
taxonomy files, so installed runtimes stay offline and do not need a
post-install bootstrap step. Missing taxonomy for a requested release is
treated as a local bootstrap or packaging error. Packaged runtime integrity is
validated locally from the recorded SHA-256 and expected row counts in
`releases.tsv`. Internal callers that need an explicit release gate should use
`resolve_and_validate_release()`, which now performs that full payload
validation before runtime taxonomy loading.

Built wheels and sdists also advertise `Requires-External` hints for
`ncbi-datasets-cli (>=18.4.0,<18.22.0)` and `unzip (>=6.0,<7.0)`. Those
metadata hints do not replace the CLI preflight, which remains the
authoritative runtime check.

Contributor setup lives in [CONTRIBUTING.md](../CONTRIBUTING.md). The pytest matrix runs on Linux, macOS, and Windows. Clean packaged-runtime and real-data validation currently run on Linux. Bioconda recipe-template notes live in [packaging/bioconda/README.md](../packaging/bioconda/README.md).

Published distribution archives include MIT-licensed project code plus GTDB
taxonomy data under CC BY-SA 4.0. The taxonomy files are shipped as separate
`.tsv.gz` package data generated from the UQ mirror and are not relicensed by
this project. See `NOTICE` and `licenses/CC-BY-SA-4.0.txt` for attribution and
licence details.

## Failure Handling

The tool keeps successfully retrieved genomes and summary files even when some
requested genomes fail. It records unsuccessful attempts in
`download_failures.tsv` and exits non-zero for incomplete runs. Legacy `UBA*`
accessions are warned about, skipped, and recorded as failed in manifests for
non-dry runs.

When `--prefer-genbank` or `--version-latest` is enabled, reproducibility is
limited by current NCBI metadata. Use `run_summary.log` timestamps,
`accession_decision_sha256`, `selected_accession`,
`download_request_accession`, and `final_accession`
from the accession manifests as the audit trail for those live decisions.

## Known Limitations

- GenBank preference depends on NCBI metadata exposing a matching assembly
  identifier
- very large requests still depend on upstream `datasets` performance and NCBI
  availability
- direct mode may need several batch passes before all payloads resolve
- published distribution size grows because GTDB taxonomy releases ship with
  the package

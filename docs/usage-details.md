# Usage Details

This document is the detailed user-facing reference for `gtdb-genomes` CLI
behaviour, output layout, retry rules, bundled-data handling, and runtime
contract.

## Command Form

```bash
gtdb-genomes \
  --gtdb-taxon g__Escherichia \
  --outdir results
```

## Options

### Mandatory options

- `--gtdb-taxon`: Each occurrence accepts one or more complete GTDB taxon
  tokens, and the flag may also be repeated. A row is selected when its GTDB
  lineage contains the requested GTDB token exactly after trimming surrounding
  whitespace only. Matching is case-sensitive, internal species whitespace is
  preserved, and suffix variants are separate taxa. For example,
  `g__Frigididesulfovibrio` does not match `g__Frigididesulfovibrio_A`.
  Species taxa contain spaces and must be quoted in the shell, for example
  `--gtdb-taxon "s__Altiarchaeum hamiconexum"` or
  `--gtdb-taxon g__Escherichia "s__Escherichia coli"`. Unquoted shell input
  such as `--gtdb-taxon s__Altiarchaeum hamiconexum` is invalid.

- `--outdir`: Output directory must either not exist or exist as an empty
  directory. The tool does not merge into or overwrite a populated output tree.

### Optional options

- `--gtdb-release`: Defaults to `latest`. Accepts bundled aliases such as
  `latest`, `80`, `95`, `214`, `226`, `220.0`, or `release220/220.0`.

  `latest` is resolved from the bundled manifest row marked with
  `is_latest=true`. GTDB release resolution never contacts GTDB over the
  network.

- `--prefer-genbank`: Disabled by default. When enabled, a requested `GCF_*`
  accession triggers NCBI metadata lookup and first uses explicit
  paired-assembly metadata from the RefSeq summary record when it is complete
  and usable. If explicit pairing is unavailable, the workflow falls back to
  the current NCBI candidate set for `GCA_*` accessions that share the same
  numeric assembly identifier. The download request then keeps the exact
  selected versioned accession by default. This is a live NCBI optimisation,
  not a frozen GTDB-release-preserving transform.

- `--version-latest`: Disabled by default. Requires `--prefer-genbank`. Drops
  the version suffix from the selected accession and asks `datasets` for the
  latest available revision in that accession family from current NCBI
  metadata, which may differ from the originally selected RefSeq or GenBank
  version and may change over time.

- download strategy is automatic only.

  Rules:

  - supported requests always go through the automatic planner
  - the planner switches to dehydrate when the request contains 1,000 or more
    unique `datasets` request tokens after accession rewriting
  - this planner intentionally stays count-only for this project and does not
    implement the generic `datasets` `> 15 GB` heuristic because the workflow
    targets prokaryote genome downloads and treats the request-token count as
    the governing operational limit
  - smaller supported requests use batch direct
    `datasets download genome accession --inputfile ... --filename ...` passes
  - direct mode retries only the still-unresolved request accessions in later
    batch passes
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

- `--threads`: Choose how many CPUs to use for the supported workflow steps.
  Default: 8. Direct downloads remain serial in the current workflow.

- `--ncbi-api-key`: This option expects an NCBI API key. The CLI also honours
  ambient `NCBI_API_KEY`, and `--ncbi-api-key` overrides that ambient value
  when both are present. The tool passes only the effective key to child
  `datasets` processes through the child process environment and does not use
  it for GTDB release resolution, local taxonomy loading, or any other
  service.

- `--include`: Defaults to `genome`.

  `--include` is passed through to
  `datasets download genome accession --include` after light validation.
  `genome` is mandatory in every accepted value.

  Examples:

  - `genome`
  - `genome,gff3`
  - `genome,gff3,protein`

- `--debug`

  Debug mode:

  - enables debug-level logging
  - emits redacted command traces
  - writes a redacted `OUTPUT/debug.log`
  - cannot be combined with an effective NCBI API key because upstream
    `datasets` debug output may expose the API key header

  `--debug --dry-run` is allowed when no effective NCBI API key is active, but
  dry-run keeps debug output on the console only and does not create
  `OUTPUT/debug.log`.

- `--dry-run`: Resolves inputs without creating the final output tree.

  It may:

  - resolve the bundled GTDB release
  - read bundled GTDB taxonomy TSVs and the local release manifest
  - preflight `unzip` early so real-run archive requirements fail fast
  - perform NCBI metadata lookup when `--prefer-genbank` is enabled and the
    selected rows include supported non-`UBA*` accessions

  Zero-match runs and unsupported-`UBA*`-only runs still avoid NCBI calls, but
  dry-runs still preflight `unzip` before they exit.

## API Key Handling

Ambient `NCBI_API_KEY` is the normal workflow path. `--ncbi-api-key` is an
explicit override and is passed only to child `datasets` processes through the
child environment.

The tool:

- never writes the API key into manifests or its own debug log
- redacts recognised key-bearing forms and known literal API-key values from
  recorded command traces and error messages
- forbids `--debug` while an effective NCBI API key is active because upstream
  `datasets` debug output can expose the raw API key header

Known limitation:

- if a user types the API key directly on the shell command line, shell history
  or inspection of the parent `gtdb-genomes` process may still expose it
  outside the control of this tool, so ambient `NCBI_API_KEY` is preferred

## Output Layout

```text
OUTPUT/
|-- accession_map.tsv
|-- download_failures.tsv
|-- run_summary.tsv
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

Taxon slugs preserve the GTDB token text where practical, replace unsafe
characters with `_`, and append a short hash suffix only when two taxa would
otherwise collide.

## Summary Files

- `run_summary.tsv`
  - one row per run
  - records requested and resolved release, chosen method, actual concurrency,
    worker usage, counts, output path, and exit code
  - `download_method_requested` is an internal provenance field and is always
    `auto`
- `taxon_summary.tsv`
  - one row per requested taxon
  - records matched rows, accession counts, duplicate-copy count, and output
    directory
- `accession_map.tsv`
  - one row per taxon-accession mapping
  - records lineage, original GTDB accession, final accession, conversion
    status, final method used, output path, and download status
  - `download_batch` records the batch pass that produced the row, for example
    `direct_batch_1`, `direct_fallback_batch_1`, or `dehydrated_batch`
  - unsupported legacy `UBA*` rows leave `download_method_used` and
    `download_batch` blank because no download step ran
- `download_failures.tsv`
  - one row per recorded failed attempt
  - records collapsed taxon context, the attempted accession or accession set,
    the final accession or accession set when the failed step has a known final
    outcome, stage, retry counters, redacted error message, and final failure
    status
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - one row per accession assigned to that taxon
  - records lineage, accession mapping, output path, and whether the accession
    is duplicated across taxa

When a failure comes from one shared metadata, batch download, or rehydrate
command, the affected taxa and accessions are collapsed into semicolon-joined
values instead of being repeated once per accession.

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

GTDB release resolution and GTDB taxonomy loading remain local. Runtime release
selection does not contact GTDB over the network.

`unzip` is required because `datasets` produces zip archives that
`gtdb-genomes` extracts before reorganising the final output tree.

Tool requirements are resolved after GTDB release loading and taxonomy
selection. Missing external tools therefore affect only the supported execution
paths that actually need them.

## Retry Policy

Every internet-facing `datasets` step gets one initial attempt plus up to three
retries, using fixed backoff delays of 5 s, 15 s, and 45 s.

This applies to:

- `datasets summary genome accession`
- direct batch `datasets download genome accession --inputfile ... --filename ...`
- batch dehydrated `datasets download genome accession --inputfile ...`
- `datasets rehydrate`

Local unzip, local file parsing, and manifest writing are not retried.

Direct-mode layout resolution adds one more workflow-level retry loop on top of
the command retry budget. One supported direct request starts with
`direct_batch_1` and may continue through `direct_batch_4`, keeping partial
successes and retrying only unresolved request accessions. Rows that still map
from a preferred `GCA_*` request may then enter `direct_fallback_batch_1` to
`direct_fallback_batch_4` against the original accession.

## Runtime Contract

Exit codes:

- `0`: full success
- `2`: CLI usage or validation error
- `3`: bundled GTDB data error
- `4`: zero matches for all requested taxa
- `5`: external tool or preflight error
- `6`: partial failure with at least one successful genome
- `7`: planning or runtime failure with no successful genomes
- `8`: local final-output materialisation failure

Status values:

- `conversion_status`
  - `unchanged_original`
  - `paired_to_gca`
  - `metadata_lookup_failed_fallback_original`
  - `paired_gca_metadata_incomplete_fallback_original`
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
- `download_failures.tsv.final_status`
  - `retry_scheduled`
  - `retry_exhausted`
  - `unsupported_input`

Fixed TSV columns:

- `run_summary.tsv`
  - `run_id`, `started_at`, `finished_at`, `requested_release`,
    `resolved_release`, `download_method_requested`, `download_method_used`,
    `threads_requested`, `download_concurrency_used`,
    `rehydrate_workers_used`, `include`, `prefer_genbank`, `version_latest`,
    `package_version`, `git_revision`, `datasets_version`, `unzip_version`,
    `release_manifest_sha256`, `bacterial_taxonomy_sha256`,
    `archaeal_taxonomy_sha256`, `debug_enabled`, `requested_taxa_count`,
    `matched_rows`, `unique_gtdb_accessions`, `final_accessions`,
    `successful_accessions`, `failed_accessions`, `output_dir`, `exit_code`
- `taxon_summary.tsv`
  - `requested_taxon`, `taxon_slug`, `matched_rows`,
    `unique_gtdb_accessions`, `final_accessions`, `successful_accessions`,
    `failed_accessions`, `duplicate_copies_written`, `output_dir`
- `accession_map.tsv`
  - `requested_taxon`, `taxon_slug`, `resolved_release`, `taxonomy_file`,
    `lineage`, `gtdb_accession`, `ncbi_accession`, `selected_accession`,
    `download_request_accession`, `final_accession`,
    `accession_type_original`, `accession_type_final`, `conversion_status`,
    `download_method_used`, `download_batch`, `output_relpath`,
    `download_status`
  - `ncbi_accession` records the original requested accession, while
    `download_request_accession` records the terminal exact token passed to
    `datasets` for that row. `final_accession` is the realised versioned
    accession from the extracted payload on successful downloads. Unsupported
    legacy `UBA*` rows leave `download_method_used` and `download_batch`
    blank because the workflow skips execution for them.
- `download_failures.tsv`
  - `requested_taxon`, `taxon_slug`, `gtdb_accession`,
    `attempted_accession`, `final_accession`, `stage`, `attempt_index`,
    `max_attempts`, `error_type`, `error_message_redacted`, `final_status`
  - `attempted_accession` is failure-path provenance and records the exact
    token or semicolon-joined accession set passed to `datasets`, including
    earlier preferred-accession attempts before fallback.
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - `requested_taxon`, `taxon_slug`, `lineage`, `gtdb_accession`,
    `ncbi_accession`, `selected_accession`, `download_request_accession`,
    `final_accession`, `conversion_status`, `output_relpath`,
    `download_status`, `duplicate_across_taxa`

## Bundled GTDB Taxonomy

GTDB taxonomy tables are consumed at runtime as compressed `.tsv.gz` files and
are decompressed transparently at read time. Runtime release resolution stays
local and does not fetch from GTDB.

Source checkout layout:

```text
data/gtdb_taxonomy/releases.tsv
data/gtdb_taxonomy/<resolved_release>/
```

`releases.tsv` remains plain text by design so the manifest stays easy to
inspect and validate. It now carries both the runtime release mapping columns
and build-only UQ mirror metadata used by the bootstrap flow.

Fresh source checkouts do not track the generated `<resolved_release>/`
payload directories in Git. Before GTDB-dependent source-checkout runs, build
the local runtime payload with:

```bash
uv run python -m gtdb_genomes.bootstrap_taxonomy
```

The bootstrap step downloads the configured taxonomy files from the HTTPS UQ
mirror release directory recorded in `releases.tsv`, verifies each source file
against the release `MD5SUM` or `MD5SUM.txt` listing, and materialises the
local `.tsv.gz` runtime layout. That source-checkout bootstrap authenticity
boundary is limited by the upstream-published MD5 listing.

Maintainers can refresh the build-only mirror metadata for the existing release
rows with:

```bash
uv run python -m gtdb_genomes.refresh_taxonomy_manifest
```

Built wheels, sdists, and Conda packages already include the generated taxonomy
payload, so installed package runtimes remain offline and do not need a
post-install bootstrap step. Missing taxonomy for a requested release is
treated as a local bootstrap or packaging error. Packaged runtime integrity is
validated locally from the bundled SHA-256 and expected row counts recorded in
`releases.tsv`. Internal callers that need an explicit release gate should use
`resolve_and_validate_release()`, which now performs that full bundled-payload
validation before runtime taxonomy loading.

Published distribution archives include MIT-licensed project code plus bundled
GTDB taxonomy data under CC BY-SA 4.0. The bundled taxonomy payload is shipped
as separate `.tsv.gz` package data generated from the UQ mirror and is not
relicensed by this project. See `NOTICE` and `licenses/CC-BY-SA-4.0.txt` for
attribution and licence details.

## Failure Handling

The tool keeps successfully retrieved genomes and summary files even when some
requested genomes fail. It records unsuccessful attempts in
`download_failures.tsv` and exits non-zero for incomplete runs. Legacy `UBA*`
accessions are warned about, skipped, and recorded as failed in manifests for
non-dry runs.

## Known Limitations

- GenBank preference depends on NCBI metadata exposing a matching assembly
  identifier
- very large requests still depend on upstream `datasets` performance and NCBI
  availability
- direct mode may need several batch passes before all payloads resolve
- published distribution size grows because bundled GTDB taxonomy releases ship
  locally

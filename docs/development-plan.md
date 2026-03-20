# Development Plan

This document is the engineering handoff for implementing `gtdb-genomes` after the documentation phase is complete.

The current phase is documentation-only. No Python package, wrapper script, or implementation modules should be created until this plan is accepted.

## Phase 1: Project scaffold and tooling

### Goal

Create the minimal project skeleton for a `uv`-managed Python application and establish the base CLI shape without implementing the full download workflow.

### Concrete tasks

- create `pyproject.toml` for a Python 3.12+ project
- use `hatchling` as the build backend
- create the package layout under `src/gtdb_genomes/`
- add the repo-local wrapper command that executes through `uv`
- add startup checks for required external tools: `datasets` and `unzip`
- define argument parsing for the planned CLI options

### Acceptance criteria

- running the future wrapper with `--help` shows the documented options
- the package can be executed with `uv run python -m gtdb_genomes.cli --help`
- missing external tools produce clear, non-secret-bearing error messages

### Notable risks or assumptions

- the eventual build backend must work both with repo-local `uv` usage and with Conda packaging
- the Bioconda package must install a normal entrypoint rather than the repo-local `uv` wrapper

## Phase 2: Bundled GTDB release discovery and taxonomy data

### Goal

Implement reliable GTDB release resolution across historical naming variants using bundled taxonomy assets shipped with the repository and future packages.

### Concrete tasks

- define a bundled manifest such as `data/gtdb_taxonomy/releases.tsv`
- normalise supported `--release` inputs to a concrete bundled release identifier
- map each bundled release identifier to local bacterial and archaeal taxonomy TSV paths
- support historical filename variants rather than one fixed pattern
- read taxonomy TSVs from `data/gtdb_taxonomy/<resolved_release>/`
- treat missing bundled taxonomy data as a local installation or packaging error
- resolve `latest` from the bundled local manifest rather than from GTDB over the network

### Acceptance criteria

- representative older and newer GTDB releases resolve correctly from bundled local data
- first run succeeds without GTDB network access for supported releases
- `latest` resolves to a concrete bundled release from the local manifest

### Notable risks or assumptions

- the bundled manifest must be maintained carefully during project release preparation
- some releases may not present both bacterial and archaeal files in the same way and that must be reflected in the bundled manifest

## Phase 3: Taxon filtering and accession selection

### Goal

Parse GTDB taxonomy tables, match requested taxa by descendant membership, and produce the initial accession set.

### Concrete tasks

- load taxonomy TSV files with Polars
- parse accession and lineage columns safely
- support repeatable `--taxon`
- implement descendant membership matching
- derive taxon directory slugs by replacing whitespace and characters outside `A-Za-z0-9._-` with `_`
- collapse repeated underscores in taxon slugs
- append an 8-character hash suffix only when two taxa would otherwise collide
- merge matches across taxa while keeping per-taxon membership information
- deduplicate the accession set for download planning

### Acceptance criteria

- repeated taxa produce a combined accession set without losing taxon membership mapping
- descendant matching behaves as documented for rank tokens such as `d__`, `g__`, and `s__`
- taxon slug generation is deterministic and collision-safe
- the accession list and taxon mapping can be exported into summary tables

### Notable risks or assumptions

- taxonomy rows may contain unusual spacing or formatting and should be normalised carefully
- overlapping taxon requests are expected and must be handled intentionally

## Phase 4: NCBI metadata lookup and `GCA` preference

### Goal

Refine the GTDB accession set by preferring paired `GCA` accessions when NCBI metadata makes that possible.

### Concrete tasks

- query NCBI assembly metadata for selected accessions
- detect paired GenBank and RefSeq relationships
- replace `GCF_*` accessions with paired `GCA_*` accessions when available
- preserve the original accession when no paired `GCA` accession exists
- record conversion status for later summaries
- do not retry metadata lookups automatically

### Acceptance criteria

- paired `GCA` accessions are preferred when present
- original accessions are retained when pairing is unavailable
- summary output records original accession, final accession, and conversion status

### Notable risks or assumptions

- NCBI metadata fields may vary slightly across records and should be read robustly
- the design prioritises completeness over strict `GCA` conversion

## Phase 5: Download orchestration and concurrency control

### Goal

Select the correct `datasets` workflow automatically and keep direct-mode
retries deterministic.

### Concrete tasks

- remove the public strategy flag and keep strategy selection internal
- run `datasets --preview` for supported requests during automatic planning
- switch to dehydrate/rehydrate for requests with at least 1,000 genomes or
  more than 15 GB
- implement direct mode as batch-input passes using
  `datasets download genome accession --inputfile ... --filename ...`
- keep partial successes from each direct batch pass and retry only unresolved
  request accessions
- preserve original-accession fallback after preferred `GCA_*` retries are
  exhausted
- require `genome` to be present in every allowed `--include` value
- map `--threads` to rehydrate worker count
- support `--include` passthrough and `--api-key` forwarding with redaction
- allow `--dry-run` to resolve releases from the bundled manifest, read bundled taxonomy data, and query accession metadata, but prohibit GTDB network access, genome downloads, and output-tree creation
- retry only `datasets download genome accession` and `datasets rehydrate`
- use one initial attempt plus up to 3 retries with fixed backoff delays of 5 s, 15 s, and 45 s

### Acceptance criteria

- automatic planning chooses the documented path for small and large requests
- direct mode uses one batch command per pass and records `direct_batch_N`
  output provenance
- preferred `GCA_*` failures may still succeed through original-accession
  fallback
- dehydrate mode uses one package download followed by controlled rehydration
- command construction respects the documented `--include`, `--dry-run`, retry, and `--threads` behaviour

### Notable risks or assumptions

- upstream `datasets` behaviour may change between versions
- preview output parsing must be robust to minor format changes
- keeping partial successes while retrying only unresolved accessions requires
  careful manifest bookkeeping

## Phase 6: Unzip and output reorganisation

### Goal

Convert raw `datasets` output into the documented final directory structure.

### Concrete tasks

- unzip downloaded archives into working directories
- locate assembly-specific content within the `ncbi_dataset` layout
- fail fast if `--output` exists and is non-empty
- create `OUTPUT/` summary files directly under the output root
- create per-taxon directories under `OUTPUT/taxa/<taxon_slug>/`
- copy each genome into every taxon directory where it belongs
- preserve the full accession payload requested through `datasets` in each final accession directory
- write per-taxon accession manifests directly inside each taxon directory
- support `--keep-temp` for preserving intermediate files

### Acceptance criteria

- final output matches the documented tree
- there is no shared `OUTPUT/genomes/` directory
- duplicate genomes appear in each relevant taxon directory
- accession directories keep the full downloaded payload, not only FASTA files
- duplicate-copy actions are recorded in logs

### Notable risks or assumptions

- the `datasets` output tree may differ slightly between direct and dehydrated workflows
- copying duplicates may increase disk usage substantially for broad taxon selections

## Phase 7: Logging, debug mode, and secret redaction

### Goal

Provide clear operational logging and strong secret hygiene.

### Concrete tasks

- define normal and debug logging formats
- add redaction helpers for API keys and command traces
- write `OUTPUT/debug.log` only when `--debug` is enabled
- log duplicate-copy events, download decisions, and failure summaries
- ensure failure TSVs contain only redacted error messages
- ensure errors do not leak secrets

### Acceptance criteria

- API keys never appear in normal logs, debug logs, manifests, or bundled-data indexes
- `--debug` produces a more detailed redacted log without changing functional behaviour
- failure messages remain actionable without exposing sensitive values

### Notable risks or assumptions

- shell history and process inspection remain outside the control of the tool
- third-party command errors may need additional redaction before display

## Phase 8: Testing

### Goal

Build a test suite that validates behaviour without depending on large live downloads by default.

### Concrete tasks

- add unit tests for release resolution and filename discovery
- add fixture-based tests for taxonomy parsing and taxon matching
- add tests for accession conversion logic
- add tests for direct vs dehydrate decision rules
- add tests for direct batch retries, no-progress termination, and original
  accession fallback
- add tests for `--include` validation and dry-run boundaries
- add tests for download-only retry logic
- add tests for output reorganisation and duplicate-copy handling
- add tests for secret redaction and debug logging
- use fake or stubbed `datasets` command behaviour where practical

### Acceptance criteria

- the documented behaviours are covered by automated tests
- tests can run locally without requiring a live large-scale NCBI download
- edge cases around duplicates, missing pairs, and partial failures are covered

### Notable risks or assumptions

- a small number of integration tests may still be useful later, but they should be separate from default unit tests
- fixture maintenance will be needed if upstream output formats change

## Phase 9: Packaging and release preparation

### Goal

Prepare the project for future distribution without changing the documented behaviour.

### Concrete tasks

- align package metadata with the root README
- finalise the console entrypoint name as `gtdb-genomes`
- complete the Bioconda recipe with real version, source URL, and checksum
- include bundled GTDB taxonomy data and the bundled local release manifest in distributed packages
- verify dependency availability for Conda packaging
- ensure the Conda package installs a normal entrypoint instead of the repo-local `uv` wrapper
- review user-facing documentation for release readiness

### Acceptance criteria

- packaging metadata is consistent across the Python project and Bioconda recipe
- the Bioconda recipe can be completed with concrete release metadata
- packaged installations include the bundled GTDB taxonomy data needed for offline release resolution
- end-user documentation matches the shipped CLI behaviour

### Notable risks or assumptions

- some dependencies, especially the NCBI `datasets` CLI package name and channel source, may need confirmation during packaging
- packaging should happen after the implementation is stable enough to justify a tagged release

## Fixed TSV Schemas

These TSV column sets are part of the implementation contract and should not be left to ad hoc design during coding.

### `run_summary.tsv`

- `run_id`
- `started_at`
- `finished_at`
- `requested_release`
- `resolved_release`
- `download_method_requested`
- `download_method_used`
- `threads_requested`
- `download_concurrency_used`
- `rehydrate_workers_used`
- `include`
- `prefer_gca`
- `debug_enabled`
- `requested_taxa_count`
- `matched_rows`
- `unique_gtdb_accessions`
- `final_accessions`
- `successful_accessions`
- `failed_accessions`
- `output_dir`
- `exit_code`

### `taxon_summary.tsv`

- `requested_taxon`
- `taxon_slug`
- `matched_rows`
- `unique_gtdb_accessions`
- `final_accessions`
- `successful_accessions`
- `failed_accessions`
- `duplicate_copies_written`
- `output_dir`

### `accession_map.tsv`

- `requested_taxon`
- `taxon_slug`
- `resolved_release`
- `taxonomy_file`
- `lineage`
- `gtdb_accession`
- `final_accession`
- `accession_type_original`
- `accession_type_final`
- `conversion_status`
- `download_method_used`
- `download_batch`
- `output_relpath`
- `download_status`

`final_accession` is left blank when no usable accession ultimately exists. In that case, `conversion_status` and `download_status` carry the outcome semantics.

### `download_failures.tsv`

- `requested_taxon`
- `taxon_slug`
- `gtdb_accession`
- `final_accession`
- `stage`
- `attempt_index`
- `max_attempts`
- `error_type`
- `error_message_redacted`
- `final_status`

### `taxon_accessions.tsv`

- `requested_taxon`
- `taxon_slug`
- `lineage`
- `gtdb_accession`
- `final_accession`
- `conversion_status`
- `output_relpath`
- `download_status`
- `duplicate_across_taxa`

## Edge-Case Contract

This section is the authoritative source of truth for validation, failure handling, fallback rules, output-creation thresholds, fixed status vocabularies, and exit codes.

### Input normalisation and validation

- trim leading and trailing whitespace for `--taxon` and `--release`
- require exact case-sensitive GTDB taxon matching after trimming
- reject empty `--taxon` after trimming with exit code `2`
- deduplicate repeated `--taxon` values after trimming, preserving first-seen order in summaries
- require `--threads` to be a positive integer; `0` or negative values fail with exit code `2`
- fail with exit code `2` if `--output` exists and is non-empty
- resolve release aliases by exact match against bundled manifest aliases after trimming

### Bundled GTDB data failures

- missing bundled manifest fails with exit code `3`
- unreadable bundled manifest fails with exit code `3`
- missing referenced taxonomy TSVs fail with exit code `3`
- unreadable bundled taxonomy TSVs fail with exit code `3`
- no GTDB network fetch is ever attempted
- `latest` resolves to the newest bundled release from the local manifest only

### `--dry-run` and `--debug`

- `--dry-run` creates no `OUTPUT/` tree and no TSV files
- `--debug --dry-run` is allowed
- when `--debug` and `--dry-run` are combined, debug output is console-only
- `OUTPUT/debug.log` is never created in dry-run mode

### Output-creation threshold

- exit codes `2`, `3`, and `5` create no output tree
- exit codes `4`, `6`, and `7` create `OUTPUT/` and root TSVs
- zero-match runs create requested taxon directories with header-only `taxon_accessions.tsv`
- zero-match runs also create all root TSVs
- in zero-match runs, `accession_map.tsv` and `download_failures.tsv` are header-only

### `auto` preview failure

- automatic planning requires a successful `datasets --preview` for supported
  requests
- if `datasets --preview` fails, the run exits with code `5`
- preview failure is treated as an external-tool or preflight error, not as code `7`

### `GCA` fallback

- if metadata lookup for a specific accession fails, fall back immediately to the original GTDB accession without retrying metadata lookup
- if preferred `GCA` is selected, that preferred accession must consume its
  direct batch retry phase before fallback begins
- the preferred phase means `direct_batch_1` plus up to three further
  preferred direct passes
- only after preferred-`GCA` retries are exhausted may the tool retry the
  original accession
- the original accession then receives its own fallback batch retry phase
- if fallback succeeds, keep the genome and record the failed preferred attempt in `download_failures.tsv`
- `accession_map.tsv` records one row per requested taxon and final usable accession result
- failed intermediate preferred-`GCA` attempts are recorded only in `download_failures.tsv`

### Duplicate semantics

- `duplicate_copies_written` counts extra copy operations beyond the first placement of a genome across requested taxa
- `duplicate_across_taxa` is `true` only when an accession belongs to more than one requested taxon in the same run

### Fixed enumerations

Exit codes:

- `0`: full success
- `2`: CLI usage or validation error
- `3`: bundled GTDB data error
- `4`: zero matches for all requested taxa
- `5`: external tool or preflight error
- `6`: partial failure with at least one successful genome
- `7`: runtime failure with no successful genomes

`conversion_status` values:

- `unchanged_original`
- `paired_to_gca`
- `metadata_lookup_failed_fallback_original`
- `paired_to_gca_fallback_original_on_download_failure`
- `failed_no_usable_accession`

`download_status` values:

- `downloaded`
- `downloaded_after_fallback`
- `failed`

`stage` values:

- `preflight`
- `metadata_lookup`
- `preview`
- `preferred_download`
- `fallback_download`
- `layout`
- `rehydrate`

`final_status` values:

- `retry_scheduled`
- `retry_exhausted`

## Cross-Cutting Decisions

These decisions are fixed across all phases:

- command name: `gtdb-genomes`
- documentation phase first, implementation later
- repeatable `--taxon`
- no `--taxa-file`, `--domain`, or `--api-key-env`
- default `--include genome`
- every allowed `--include` value must contain `genome`
- `--debug` writes `OUTPUT/debug.log`
- `--dry-run` may resolve releases from the bundled manifest, read bundled taxonomy data, and query metadata, but must not contact GTDB, download genome payloads, or create the output tree
- taxonomy TSVs are bundled with the software under the repo and future packages
- fail fast when `--output` exists and is non-empty
- no shared `OUTPUT/genomes/`
- manifests are written directly under `OUTPUT/` and directly under each taxon directory
- accession directories preserve the full downloaded payload requested for that accession
- duplicated genomes are copied into each taxon folder and logged
- keep successful outputs on partial failure, but exit non-zero
- retry only download operations, with one initial attempt plus up to 3 retries
- direct mode uses one batch command per pass and may retry unresolved
  accessions across preferred and fallback phases

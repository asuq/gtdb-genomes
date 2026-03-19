# gtdb-genomes

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections using bundled
GTDB taxonomy tables and the NCBI `datasets` CLI.

The runtime model is split deliberately:

- packaged and Conda-installed use runs the normal `gtdb-genomes` command
- source-checkout development uses `uv` for local dependency management and
  execution

`uv` is a development tool only. End users of a packaged installation should
not need `uv` at runtime.

## Workflow

The tool:

1. Resolves a GTDB release from the bundled release manifest.
2. Loads the bundled GTDB taxonomy TSV files for that release.
3. Selects genomes whose lineage contains one or more requested GTDB taxa.
4. Uses the accession recorded in the GTDB TSV as the starting accession set.
5. Optionally prefers paired GenBank assemblies when a matching `GCA_*`
   accession shares the same numeric assembly identifier as the original
   `GCF_*` accession.
6. Uses the NCBI `datasets` command to resolve metadata and download genomes.
7. Chooses direct download or batch dehydrate/rehydrate based on request size.
8. Unzips the downloaded payload and reorganises it into per-taxon folders.

Completeness has priority over GenBank preference. If a paired GenBank
accession is unavailable or metadata lookup exhausts its retry budget, the
original accession is kept.

> Caution
>
> Some legacy GTDB releases include genome accessions starting with `UBA`.
> These legacy accessions are not supported by NCBI and are not supported by
> `gtdb-genomes`. When selected, the tool warns and skips them. Check
> BioProject `PRJNA417962`, since most `UBA` genomes are assigned through that
> bioproject.

## Prerequisites

Packaged runtime use requires:

- `datasets`
- `unzip`

Source-checkout development additionally uses:

- `uv`

Any installation path that runs the real downloader, including `pip install .`
or a local wheel install, still requires `datasets` and `unzip` on `PATH`.

## Command Form

```bash
gtdb-genomes --release latest --taxon g__Escherichia --output results
```

The CLI includes:

- `--release`
- repeatable `--taxon`
- `--output`
- `--prefer-genbank` / `--no-prefer-genbank`
- `--download-method {auto,direct,dehydrate}`
- `--threads`
- `--ncbi-api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

The interface does not include:

- `--taxa-file`
- `--domain`
- `--ncbi-api-key-env`

## Option Notes

### `--release`

Accepts bundled aliases such as `latest`, `80`, `95`, `214`, `226`, `220.0`,
or `release220/220.0`.

`latest` is resolved from the bundled manifest row marked with `is_latest=true`.
GTDB release resolution never contacts GTDB over the network.

### `--taxon`

Repeatable. A row is selected when its GTDB lineage contains the requested GTDB
token exactly after trimming. Matching is case-sensitive.

### `--prefer-genbank`

Enabled by default. When a requested accession is `GCF_*`, the tool inspects
NCBI metadata and prefers a `GCA_*` accession only when it shares the same
numeric assembly identifier. If several matching `GCA_*` versions exist, the
highest version is chosen.

### `--download-method`

Defaults to `auto`.

Rules:

- direct mode downloads one accession per `datasets download genome accession`
  job, with concurrency limited to `min(threads, 5)`
- dehydrate mode writes one accession file and runs one batch
  `datasets download genome accession --inputfile ... --dehydrated` job
- auto mode switches to dehydrate when the request contains at least 1,000
  accessions or when `datasets --preview` reports more than 15 GB

If a batch dehydrated download exhausts its retry budget, or if unzip or batch
rehydrate fails, the tool falls back to per-accession direct downloads and
records `dehydrate_fallback_direct` as the final method used.

### `--threads`

Defaults to all available CPU threads.

Concurrency rules:

- direct-mode network concurrency is `min(threads, 5, accession_count)`
- batch dehydrated download concurrency is always `1`
- `datasets rehydrate --max-workers` uses `min(threads, 30)`

### `--include`

Defaults to `genome`.

`--include` is passed through to `datasets download genome accession --include`
after light validation. `genome` is mandatory in every accepted value.

Examples:

- `genome`
- `genome,gff3`
- `genome,gff3,protein`

### `--debug`

Debug mode:

- enables debug-level logging
- emits redacted command traces
- writes a redacted `OUTPUT/debug.log`

`--debug --dry-run` is allowed, but dry-run keeps debug output on the console
only and does not create `OUTPUT/debug.log`.

### `--dry-run`

`--dry-run` resolves inputs without creating the final output tree.

It may:

- resolve the bundled GTDB release
- read bundled GTDB taxonomy TSVs and the local release manifest
- perform NCBI metadata lookup when `--prefer-genbank` is enabled
- run `datasets --preview` when `--download-method auto` is used

It must not:

- contact GTDB over the network
- download genome payloads
- run dehydrate or rehydrate
- create the final `OUTPUT/` tree

Dry-run tool requirements are conditional:

- `--dry-run --no-prefer-genbank --download-method direct` can run from bundled
  GTDB data only
- dry-run with `--prefer-genbank` requires `datasets`
- dry-run with `--download-method auto` requires `datasets`

### `--output`

The output directory must either not exist or exist as an empty directory. The
tool does not merge into or overwrite a populated output tree.

## Retry Policy

Every internet-facing `datasets` step gets one initial attempt plus up to three
retries, using fixed backoff delays of 5 s, 15 s, and 45 s.

This applies to:

- `datasets summary genome accession`
- `datasets download genome accession --preview`
- direct `datasets download genome accession`
- batch dehydrated `datasets download genome accession --inputfile ...`
- `datasets rehydrate`

Local unzip, local file parsing, and manifest writing are not retried.

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
- `taxon_summary.tsv`
  - one row per requested taxon
  - records matched rows, accession counts, duplicate-copy count, and output
    directory
- `accession_map.tsv`
  - one row per taxon-accession mapping
  - records lineage, original GTDB accession, final accession, conversion
    status, final method used, output path, and download status
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

## Runtime Contract

Exit codes:

- `0`: full success
- `2`: CLI usage or validation error
- `3`: bundled GTDB data error
- `4`: zero matches for all requested taxa
- `5`: external tool or preflight error
- `6`: partial failure with at least one successful genome
- `7`: runtime failure with no successful genomes

Status values:

- `conversion_status`
  - `unchanged_original`
  - `paired_to_gca`
  - `metadata_lookup_failed_fallback_original`
  - `paired_to_gca_fallback_original_on_download_failure`
  - `failed_no_usable_accession`
- `download_status`
  - `downloaded`
  - `downloaded_after_fallback`
  - `failed`
- `download_failures.tsv.stage`
  - `preflight`
  - `metadata_lookup`
  - `preview`
  - `preferred_download`
  - `fallback_download`
  - `rehydrate`
- `download_failures.tsv.final_status`
  - `retry_scheduled`
  - `retry_exhausted`
  - `fallback_exhausted`
  - `unsupported_input`

Fixed TSV columns:

- `run_summary.tsv`
  - `run_id`, `started_at`, `finished_at`, `requested_release`,
    `resolved_release`, `download_method_requested`, `download_method_used`,
    `threads_requested`, `download_concurrency_used`,
    `rehydrate_workers_used`, `include`, `prefer_genbank`, `debug_enabled`,
    `requested_taxa_count`, `matched_rows`, `unique_gtdb_accessions`,
    `final_accessions`, `successful_accessions`, `failed_accessions`,
    `output_dir`, `exit_code`
- `taxon_summary.tsv`
  - `requested_taxon`, `taxon_slug`, `matched_rows`,
    `unique_gtdb_accessions`, `final_accessions`, `successful_accessions`,
    `failed_accessions`, `duplicate_copies_written`, `output_dir`
- `accession_map.tsv`
  - `requested_taxon`, `taxon_slug`, `resolved_release`, `taxonomy_file`,
    `lineage`, `gtdb_accession`, `final_accession`,
    `accession_type_original`, `accession_type_final`, `conversion_status`,
    `download_method_used`, `download_batch`, `output_relpath`,
    `download_status`
- `download_failures.tsv`
  - `requested_taxon`, `taxon_slug`, `gtdb_accession`,
    `attempted_accession`, `final_accession`, `stage`, `attempt_index`,
    `max_attempts`, `error_type`, `error_message_redacted`, `final_status`
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - `requested_taxon`, `taxon_slug`, `lineage`, `gtdb_accession`,
    `final_accession`, `conversion_status`, `output_relpath`,
    `download_status`, `duplicate_across_taxa`

## Bundled GTDB Taxonomy

GTDB taxonomy tables ship with the software as compressed `.tsv.gz` files and
are decompressed transparently at read time. They are loaded from bundled data,
not fetched at runtime.

Bundled data layout:

```text
data/gtdb_taxonomy/<resolved_release>/
data/gtdb_taxonomy/releases.tsv
```

`releases.tsv` remains plain text by design so the bundled manifest stays easy
to inspect and validate.

First run does not contact GTDB. Missing bundled taxonomy for a requested
release is treated as a local installation or packaging error.

The project code and packaging are distributed under the MIT licence. Bundled
GTDB taxonomy data remains subject to the applicable upstream terms and
attribution requirements. See `NOTICE` for the bundled-data note.

## Representative Usage Examples

Small direct download:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Escherichia \
  --output results/escherichia
```

Large request that is expected to use batch dehydrate mode:

```bash
gtdb-genomes \
  --release 214 \
  --taxon d__Bacteria \
  --download-method auto \
  --threads 12 \
  --output results/bacteria
```

Prefer paired GenBank accessions and request extra annotation:

```bash
gtdb-genomes \
  --release latest \
  --taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --include genome,gff3 \
  --output results/methanobrevibacter
```

Bundled-data-only dry-run:

```bash
gtdb-genomes \
  --release 95 \
  --taxon "s__Thermoflexus hugenholtzii" \
  --download-method direct \
  --no-prefer-genbank \
  --dry-run \
  --output /tmp/gtdb_dry_run
```

Enable debug logging:

```bash
gtdb-genomes \
  --release 95 \
  --taxon g__Bacteroides \
  --debug \
  --output results/bacteroides
```

Pass an NCBI API key directly to the command:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Salmonella \
  --ncbi-api-key "${NCBI_API_KEY}" \
  --output results/salmonella
```

## API Key Handling

`--ncbi-api-key` expects an NCBI API key. The tool passes it only to the
upstream `datasets` command and does not use it for GTDB release resolution,
local taxonomy loading, or any other service.

The tool forwards `--ncbi-api-key` to `datasets --api-key` without writing it
to project files.

It:

- never prints the API key in logs
- never writes the API key into manifests or debug output
- redacts the API key from recorded command traces and error messages

Known limitation:

- if a user types the API key directly on the shell command line, shell history
  or process inspection may still expose it outside the control of this tool

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
- direct download concurrency is intentionally limited to `min(--threads, 5)`
- package size grows because bundled GTDB taxonomy releases ship locally

## Development And Packaging

Supported workflows:

- source-checkout development through `uv run gtdb-genomes ...` or
  `uv run python -m gtdb_genomes ...`
- packaged installation, including future Bioconda packaging, through the
  normal `gtdb-genomes ...` command

The Bioconda recipe template in this repository assumes the packaged runtime
uses the standard console entrypoint and `conda-forge::ncbi-datasets-cli`.

## Additional Documents

- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Bioconda recipe template](packaging/bioconda/meta.yaml)

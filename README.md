# gtdb-genomes

`gtdb-genomes` is a command-line tool for downloading genomes from NCBI based on GTDB taxa and GTDB release taxonomy tables.

The project uses a split runtime model:

- packaged and Conda-installed use runs the normal `gtdb-genomes` command
- source-checkout development uses `uv` for local dependency management and execution

## What The Tool Will Do

The planned workflow is:

1. Resolve a GTDB release, including historical GTDB release layouts.
2. Load the relevant bundled GTDB taxonomy TSV files from the local data store.
3. Select genomes whose GTDB lineage contains one or more requested taxa.
4. Use the accession recorded in the GTDB TSV as the starting accession set.
5. Prefer paired `GCA` accessions when NCBI metadata provides a GenBank counterpart.
6. Download genomes from NCBI with the `datasets` command-line tool.
7. Choose direct download or dehydrate/rehydrate based on request size.
8. Unzip the downloaded archive and reorganise the output into per-taxon folders.

Completeness has priority over strict `GCA` conversion. If a paired `GCA` accession is unavailable, the original accession is retained.

## Prerequisites

Packaged runtime use requires:

- `datasets`
- `unzip`

Source-checkout development additionally uses:

- `uv`

`uv` is a development tool only. End users of a packaged installation should not need `uv` at runtime.

## Command Form

```bash
gtdb-genomes --release latest --taxon g__Escherichia --output results
```

The interface includes:

- `--release`
- repeatable `--taxon`
- `--output`
- `--prefer-gca` / `--no-prefer-gca`
- `--download-method {auto,direct,dehydrate}`
- `--threads`
- `--api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

The design explicitly does not include:

- `--taxa-file`
- `--domain`
- `--api-key-env`

## Option Defaults

### `--release`

Accepts values such as `latest`, `80`, `95`, `214`, `226`, `220.0`, or `release220/220.0`. The implementation will normalise these into a concrete GTDB release path.

`latest` is planned to resolve from a bundled local release manifest rather than from GTDB over the network.

### `--taxon`

Repeatable. Each requested taxon is matched by descendant membership. In practice, a genome is selected when its GTDB lineage contains the requested GTDB token.

### `--prefer-gca`

Enabled by default. The tool will try to replace `GCF_*` accessions with paired `GCA_*` accessions when NCBI metadata exposes that relationship. If no paired `GCA` accession exists, the original accession will still be downloaded.

### `--download-method`

Defaults to `auto`.

The planned rules are:

- use direct download for smaller requests
- switch to dehydrate/rehydrate when the request contains at least 1,000 genomes
- also switch to dehydrate/rehydrate when `datasets --preview` reports more than 15 GB

Only genome download operations are retried automatically. The planned retry policy is one initial attempt plus up to 3 retries for `datasets download genome accession` and `datasets rehydrate`, with fixed backoff delays of 5 s, 15 s, and 45 s.

### `--threads`

Defaults to all available CPU threads.

The planned concurrency rules are:

- direct download may shard the accession list across multiple `datasets download genome accession` jobs
- direct-mode network concurrency is `min(threads, 5)`
- dehydrate mode uses one package download, then `datasets rehydrate --max-workers` for local file retrieval
- rehydrate worker count is planned as `min(threads, 30)`

### `--include`

Defaults to `genome`.

`--include` controls which file classes the upstream `datasets` command should fetch in addition to its standard metadata package. Planned examples include:

- `genome`
- `genome,gff3`
- `genome,gff3,protein`

The planned implementation will validate the argument lightly, then pass it through to `datasets download genome accession --include` rather than translating it into custom internal presets.

`genome` is mandatory in every allowed `--include` value. Values such as `none` are intentionally not supported because the final output must remain genome-centric.

### `--debug`

When enabled, the planned tool will:

- log at debug level
- record per-step timings
- emit redacted command traces
- write a redacted `OUTPUT/debug.log`

Debug mode is separate from `--keep-temp`. Temporary files will still be removed unless `--keep-temp` is also set.

### `--dry-run`

`--dry-run` is a resolution-only mode.

It may:

- resolve the requested GTDB release
- read bundled GTDB taxonomy TSV files and the bundled local release manifest
- perform NCBI metadata lookups needed for accession mapping
- decide which download method would be used

It must not:

- contact GTDB over the network
- download genome payloads
- run dehydrate or rehydrate
- create the final `OUTPUT/` tree

### `--output`

The output directory must either not exist or exist as an empty directory.

The planned implementation will fail fast when `--output` already exists and is non-empty. It will not merge into or overwrite an existing populated output tree.

## Output Layout

The planned output structure is:

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
    |   |-- GCA_000005845.2/
    |   `-- GCF_000026265.1/
    `-- s__Escherichia_coli/
        |-- taxon_accessions.tsv
        `-- GCA_000005845.2/
```

Important layout decisions:

- manifests are written directly under `OUTPUT/`
- per-taxon manifest files are written directly under each taxon directory
- taxon directories use a filesystem-safe taxon slug
- there is no shared `OUTPUT/genomes/` directory
- if the same genome belongs to more than one requested taxon, it is copied into each matching taxon directory
- duplicate-copy events are planned to be logged explicitly
- each `OUTPUT/taxa/<taxon_slug>/<assembly_accession>/` directory keeps the full downloaded accession payload requested through `datasets`, not only FASTA files

Taxon slug rule:

- preserve the GTDB token text as far as practical
- replace whitespace and characters outside `A-Za-z0-9._-` with `_`
- collapse repeated underscores
- append a short hash suffix only when two taxa would otherwise map to the same directory name

The planned run will keep successful outputs when some genomes fail, but it will write failure records and exit non-zero for partial completion.

### Summary Files

The root TSV files are intended to be stable machine-readable outputs:

- `run_summary.tsv`
  - one row for the overall run
  - records run metadata, chosen method, thread settings, counts, output path, and final exit code
- `taxon_summary.tsv`
  - one row per requested taxon
  - records matched rows, accession counts, success and failure counts, duplicate-copy count, and output directory
- `accession_map.tsv`
  - one row per taxon-accession mapping
  - records lineage, original GTDB accession, final accession, conversion status, download batch, output path, and download status
- `download_failures.tsv`
  - one row per failed accession attempt
  - records stage, attempt index, redacted error message, and final failure status
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - one row per accession assigned to that taxon
  - records lineage, accession mapping, output path, and whether the accession is duplicated across taxa

Fixed column sets are defined in [Step-wise development plan](docs/development-plan.md).

Exact exit codes, fallback rules, zero-match behaviour, and fixed status values are defined in the `Edge-Case Contract` section of [Step-wise development plan](docs/development-plan.md).

## Bundled GTDB Taxonomy

GTDB taxonomy TSV files are planned to ship with the software rather than being downloaded at runtime.

Planned bundled data location:

```text
data/gtdb_taxonomy/<resolved_release>/
```

The design also includes a bundled local release manifest, for example:

```text
data/gtdb_taxonomy/releases.tsv
```

This manifest is intended to map:

- accepted release inputs such as `80`, `95`, `214`, `226`, and `220.0`
- the canonical bundled release identifier
- the bundled taxonomy file paths for that release

The tool must resolve supported releases from this local manifest. First run is therefore not expected to contact GTDB.

If bundled taxonomy data for a requested release is missing, that is treated as a local installation or packaging error rather than a trigger to fetch data from GTDB.

## Representative Usage Examples

These examples describe the intended interface. They do not work yet because the tool is not implemented.

Small direct download:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Escherichia \
  --output results/escherichia
```

Large request expected to use dehydrate/rehydrate:

```bash
gtdb-genomes \
  --release 214 \
  --taxon d__Bacteria \
  --download-method auto \
  --threads 12 \
  --output results/bacteria
```

Prefer paired `GCA` accessions and request extra annotation:

```bash
gtdb-genomes \
  --release latest \
  --taxon s__Methanobrevibacter smithii \
  --prefer-gca \
  --include genome,gff3 \
  --output results/methanobrevibacter
```

Enable debug logging:

```bash
gtdb-genomes \
  --release 95 \
  --taxon g__Bacteroides \
  --debug \
  --output results/bacteroides
```

Pass an NCBI API key directly to the planned command:

```bash
gtdb-genomes \
  --release latest \
  --taxon g__Salmonella \
  --api-key "${NCBI_API_KEY}" \
  --output results/salmonella
```

## API Key Handling

The planned implementation will accept `--api-key` and pass it to the upstream `datasets` command without writing it to project files.

The tool is intended to:

- never print the API key in logs
- never save the API key in manifests, bundled-data indexes, or debug output
- redact the API key from recorded command traces and error messages

Known limitation:

- if a user types the API key directly on the shell command line, the shell history or operating-system process inspection may still expose it outside the control of this tool

## Failure Handling

The planned behaviour is:

- keep all successfully retrieved genomes and summary files
- record unsuccessful resolutions or downloads in `download_failures.tsv`
- exit with a non-zero status if any requested genome ultimately fails

This makes partial results usable without hiding incomplete runs.

## Known Limitations In The Planned Design

- the repository currently contains documents only, not executable code
- GTDB release resolution must support historical naming changes across releases using bundled taxonomy metadata
- `GCA` preference depends on paired accession metadata being available from NCBI
- very large requests will still depend on upstream `datasets` performance and NCBI service availability
- direct download concurrency is intentionally limited to `min(--threads, 5)` to avoid excessive server load
- package size will grow because all supported GTDB taxonomy releases are bundled locally

## Development And Packaging

Use cases supported by the project are:

- source-checkout development through `uv run gtdb-genomes ...` or `uv run python -m gtdb_genomes ...`
- packaged installation, including Bioconda, through the normal `gtdb-genomes ...` command

Bioconda is expected to install a Conda-native `gtdb-genomes` command into the active environment so that activation is sufficient to place it on `PATH`.

The current Bioconda material in this repository remains a template for future packaging work.

## Additional Documents

- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Bioconda recipe template](packaging/bioconda/meta.yaml)

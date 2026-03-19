# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genome)](https://github.com/asuq/gtdb-genome/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections using bundled
GTDB taxonomy tables for local taxon resolution and the NCBI `datasets` CLI for
all NCBI metadata and genome download operations.

## Installation

The checked-in Bioconda recipe at
[packaging/bioconda/meta.yaml](packaging/bioconda/meta.yaml) is a template for
future packaging and is not a release-ready install path.

```bash
uv sync --group dev
uv run gtdb-genomes --help
```

## Command

```bash
gtdb-genomes --gtdb-release latest --gtdb-taxon g__Escherichia --outdir results
```

### Mandatory options

- `--gtdb-release`: Accepts bundled aliases such as `latest`, `80`, `95`,
  `214`, `226`, `220.0`, or `release220/220.0`.

  `latest` is resolved from the bundled manifest row marked with
  `is_latest=true`. GTDB release resolution never contacts GTDB over the
  network.

- `--gtdb-taxon`: Repeatable. A row is selected when its GTDB lineage contains
  the requested GTDB token exactly after trimming. Matching is case-sensitive.

- `--outdir`: Output directory must either not exist or exist as an empty
  directory. The tool does not merge into or overwrite a populated output tree.


### Optional options
- `--prefer-genbank`
- `--download-method {auto,direct,dehydrate}`
- `--threads`
- `--ncbi-api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

Check `gtdb-genomes --help` for details and [usage-details](docs/usage-details.md) on optional options.

## Examples

Small direct download:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon g__Escherichia \
  --outdir results/escherichia
```

Prefer paired GenBank accessions and request extra annotation:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --include genome,gff3 \
  --ncbi-api-key "${NCBI_API_KEY}" \
  --outdir results/methanobrevibacter
```

Bundled-data-only dry-run:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --download-method direct \
  --dry-run \
  --outdir /tmp/gtdb_dry_run
```

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

## Summary Files

- `run_summary.tsv`: records requested and resolved release, chosen method, actual concurrency, worker usage, counts, output path, and exit code
- `taxon_summary.tsv`: records matched rows, accession counts, duplicate-copy count, and output directory
- `accession_map.tsv`: records lineage, original GTDB accession, final accession, conversion status, final method used, output path, and download status
- `download_failures.tsv`: records collapsed taxon context, the attempted accession or accession set, the final accession or accession set when the failed step has a known final outcome, stage, retry counters, redacted error message, and final failure status
- `OUTPUT/taxa/<taxon_slug>/taxon_accessions.tsv`
  - records lineage, accession mapping, output path, and whether the accession is duplicated across taxa

When a failure comes from one shared metadata, batch download, or rehydrate
command, the affected taxa and accessions are collapsed into semicolon-joined
values instead of being repeated once per accession.


## Workflow

The tool:

1. Resolves a GTDB release from the bundled release manifest.
2. Loads the bundled GTDB taxonomy tables for that release.
3. Selects genomes whose lineage contains one or more requested GTDB taxa.
4. Starts from the accession recorded in the GTDB taxonomy table.
5. Optionally prefers paired GenBank assemblies when `--prefer-genbank` is set.
6. Uses the NCBI `datasets` command for metadata lookup and genome download.
7. Chooses direct download or batch dehydrate/rehydrate based on request size.
8. Unzips and reorganises the downloaded payload into per-taxon folders.

Detailed CLI behaviour, retry rules, output layout, runtime contract, and
bundled-data notes are documented in
[Usage details](docs/usage-details.md).

> [!IMPORTANT]
> `--ncbi-api-key` expects an NCBI API key. The tool passes it only to the
> upstream `datasets` command and does not use it for GTDB release resolution,
> local taxonomy loading, or any other service.

> [!NOTE]
> Some legacy GTDB releases include genome accessions starting with `UBA`.
> These legacy accessions are not supported by NCBI and are not supported by
> `gtdb-genomes`. When selected, the tool warns and skips them. Check
> BioProject `PRJNA417962`, since most `UBA` genomes are assigned through that
> bioproject.

## Development And Packaging

Supported workflows:

- source-checkout development through `uv run gtdb-genomes ...` or
  `uv run python -m gtdb_genomes ...`
- the checked-in Bioconda recipe is a template for future packaging, not a
  published install path from this repository snapshot

`uv` is a development tool only. Packaged runtime use should not depend on it.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Bioconda recipe template](packaging/bioconda/meta.yaml)

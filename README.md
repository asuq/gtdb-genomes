# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![Pytest: Linux | macOS | Windows](https://img.shields.io/badge/pytest-Linux%20%7C%20macOS%20%7C%20Windows-4c8eda.svg)](https://github.com/asuq/gtdb-genomes/actions/workflows/ci.yml)
[![CI](https://github.com/asuq/gtdb-genomes/actions/workflows/ci.yml/badge.svg)](https://github.com/asuq/gtdb-genomes/actions/workflows/ci.yml)
[![Live validation](https://github.com/asuq/gtdb-genomes/actions/workflows/live-validation.yml/badge.svg)](https://github.com/asuq/gtdb-genomes/actions/workflows/live-validation.yml)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genomes)](https://github.com/asuq/gtdb-genomes/releases)
[![CITATION.cff](https://img.shields.io/badge/CITATION-cff-blue.svg)](https://github.com/asuq/gtdb-genomes/blob/main/CITATION.cff)
[![Code licence: MIT](https://img.shields.io/badge/code-MIT-green.svg)](LICENSE)
[![Bundled data licence: CC BY-SA 4.0](https://img.shields.io/badge/bundled%20data-CC--BY--SA%204.0-blue.svg)](licenses/CC-BY-SA-4.0.txt)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections.

It uses GTDB taxonomy tables and [NCBI datasets CLI](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/getting_started/).

The detailed runtime contract, output layout, retry rules, and bundled-data notes live in [docs/usage-details.md](docs/usage-details.md).


## Installation

The first public Bioconda release is pending a tagged source release and
verified source archive. The checked-in recipe remains a draft template and is
not yet a public installation path.

The packaged runtime is validated against:

- `ncbi-datasets-cli >=18.4.0,<18.22.0`
- `unzip >=6.0,<7.0`

The CLI checks these versions during preflight and exits with code `5` when
the installed runtime falls outside this supported window.

The pytest matrix runs on Linux, macOS, and Windows. Clean packaged-runtime
and real-data validation currently run on Linux. For now, use the
source-checkout workflow in Development And Packaging below.


## Quick Start

```bash
gtdb-genomes --gtdb-taxon g__Escherichia --outdir results
```


## Command options

See [docs/usage-details.md](docs/usage-details.md) for the full CLI contract.
In short:

### Required:
- `--gtdb-taxon`: repeatable, matches exact GTDB lineage strings
- `--outdir`:  must be empty or absent

### Optional:
- `--gtdb-release`: gtdb release number, defaults to `latest`
- `--prefer-genbank`: prefers paired GenBank accessions and keeps the exact selected version by default
- `--version-latest`: paired with `--prefer-genbank`, opts into the latest available revision within the selected GenBank family, e.g. `GCA_000005845.2` -> `GCA_000005845.3` if the latter is available
- `--threads`: number of threads to run, defaults to 8
- `--include`: locally supported tokens are `genome`, `gff3`, and `protein`, e.g. `genome,gff3,protein`, see [NCBI datasets documentation](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/how-tos/genomes/download-genome/#choosing-which-data-files-to-include-in-the-data-package)
- `--ncbi-api-key`: NCBI API key, passed only to the `datasets` command
- `--dry-run`: supported with automatic planning, prints the planned download list without downloading

## Examples

Small download, quotation required for species-level taxon names with spaces:

```bash
gtdb-genomes \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --outdir results
```

Prefer paired GenBank accessions, keep the exact selected version, and request
extra annotation:

```bash
gtdb-genomes \
  --gtdb-taxon "p__Pseudomonadota" "c__Alphaproteobacteria" \
  --prefer-genbank \
  --include genome,gff3 \
  --ncbi-api-key "${NCBI_API_KEY}" \
  --outdir results
```

Opt into the latest available revision within the selected GenBank family:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --version-latest \
  --outdir results/methanobrevibacter-latest
```

Supported dry-run with automatic planning:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --dry-run \
  --outdir /tmp/gtdb_dry_run
```

## Operational Notes

- Taxon matching is exact-token and case-sensitive.
- `--outdir` must be empty or absent before each run.
- If one genome matches multiple requested taxa, the downloaded package is copied into each matching taxon directory.
- Automatic planning switches to `dehydrate` only above 1,000 unique `datasets`
  request tokens; best-effort preview no longer changes the chosen method or
  aborts planning by itself.
- Direct downloads remain serial in the current workflow.
- `--include` accepts only `genome`, `gff3`, and `protein`; see [docs/usage-details.md](docs/usage-details.md) for the full runtime contract.

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

For detailed summary-file definitions, retry rules, runtime codes, and bundled
data notes, see [docs/usage-details.md](docs/usage-details.md).

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

- source-checkout development through `uv`
- packaged wheel and sdist validation in CI on Linux
- maintainer manifest refresh through
  `uv run python -m gtdb_genomes.refresh_taxonomy_manifest`
- a draft Bioconda recipe template is kept at
  `packaging/bioconda/meta.yaml.template`
  and is quarantined until a tagged release archive and final SHA256 are available

Source checkouts use the development workflow:

```bash
uv sync --group dev
uv run python -m gtdb_genomes.bootstrap_taxonomy
uv run gtdb-genomes --help
```

A Git checkout tracks only `data/gtdb_taxonomy/releases.tsv`. The bootstrap
step downloads the configured taxonomy files from the HTTPS UQ mirror recorded
in the manifest, verifies them against the published `MD5SUM` listing, and
materialises the local `data/gtdb_taxonomy/<release>/*.tsv.gz` layout used by
a source checkout and source build. That bootstrap authenticity boundary is
therefore limited by the upstream MD5 listing; packaged runtime integrity uses
the bundled local SHA-256 and row-count manifest instead.

`uv` is a development tool only. Packaged runtime use should not depend on it.

## Licence

The project code and packaging glue are released under the MIT licence.
Published source and wheel archives bundle GTDB taxonomy tables under `data/gtdb_taxonomy`,
and those bundled data files remain under CC BY-SA 4.0 rather than MIT.

The Git checkout tracks only the plain-text `data/gtdb_taxonomy/releases.tsv` manifest.
Source checkouts materialise the generated `.tsv.gz` taxonomy payload
with `uv run python -m gtdb_genomes.bootstrap_taxonomy`.
Attribution and redistribution details for the bundled data are recorded in [NOTICE](NOTICE)
and the included [CC BY-SA 4.0 licence text](licenses/CC-BY-SA-4.0.txt).
The generated taxonomy payload is not relicensed by this project.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Bioconda draft template](packaging/bioconda/meta.yaml.template)
- [Bioconda packaging notes](packaging/bioconda/README.md)

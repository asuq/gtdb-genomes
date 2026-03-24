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

It uses bundled GTDB taxonomy tables and
[NCBI datasets CLI](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/getting_started/).

The README gives the short version.
The detailed guide covers the
[Runtime Contract](docs/usage-details.md#runtime-contract),
[Output Layout](docs/usage-details.md#output-layout),
[Retry Policy](docs/usage-details.md#retry-policy), and
[Bundled GTDB Taxonomy](docs/usage-details.md#bundled-gtdb-taxonomy).


## Installation

The first public Bioconda release is not ready yet.
It still needs a tagged source archive and a verified checksum.
The checked-in recipe is a draft, not a published installation path.

The packaged runtime is currently checked with:

- `polars >=1.31.0,<2.0.0`
- `ncbi-datasets-cli >=18.4.0,<18.22.0`
- `unzip >=6.0,<7.0`

For packaging and redistribution details, see
[Bundled GTDB Taxonomy](docs/usage-details.md#bundled-gtdb-taxonomy).


## Quick Start

```bash
gtdb-genomes --gtdb-taxon g__Escherichia --outdir results
```


## Command options

Short version:

- `--gtdb-taxon`: exact GTDB taxon token(s)
- `--outdir`: must be empty or absent
- `--gtdb-release`: defaults to `latest`
- `--prefer-genbank` and `--version-latest`: live NCBI metadata modes
- `--include`: locally supported values are `genome`, `gff3`, and `protein`
- `--threads`, `--dry-run`, `--ncbi-api-key`, and `--debug` are also available

For full option behaviour, see [Options](docs/usage-details.md#options),
[API Key Handling](docs/usage-details.md#api-key-handling),
[Retry Policy](docs/usage-details.md#retry-policy),
[Runtime Contract](docs/usage-details.md#runtime-contract), and
[Summary Files](docs/usage-details.md#summary-files).

## Examples

Small download. Quote species names that contain spaces:

```bash
gtdb-genomes \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --outdir results
```

Prefer paired GenBank accessions from current NCBI metadata, keep the exact
selected version, and ask for extra annotation:

```bash
export NCBI_API_KEY="your-ncbi-api-key"
gtdb-genomes \
  --gtdb-taxon "p__Pseudomonadota" "c__Alphaproteobacteria" \
  --prefer-genbank \
  --include genome,gff3 \
  --outdir results
```

Ask for the latest available revision in the selected GenBank family from
current NCBI metadata:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --version-latest \
  --outdir results/methanobrevibacter-latest
```

Dry-run with automatic planning:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
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

Each run writes top-level manifests and one directory per requested taxon under
`OUTPUT/taxa/`.
For detailed layout rules and summary-file definitions, see
[Output Layout](docs/usage-details.md#output-layout) and
[Summary Files](docs/usage-details.md#summary-files).

## Contribution

Contributor setup and source-checkout notes are in
[CONTRIBUTING.md](CONTRIBUTING.md).

For runtime and packaging boundaries, see
[Runtime Contract](docs/usage-details.md#runtime-contract) and
[Bundled GTDB Taxonomy](docs/usage-details.md#bundled-gtdb-taxonomy). For
Bioconda template
notes, see [packaging/bioconda/README.md](packaging/bioconda/README.md).

## Licence

The project code and packaging glue are released under the MIT licence.
Published release archives also bundle GTDB taxonomy data under CC BY-SA 4.0.
See [NOTICE](NOTICE) and
[licenses/CC-BY-SA-4.0.txt](licenses/CC-BY-SA-4.0.txt) for attribution and
licence details.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Contributing](CONTRIBUTING.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Bioconda draft template](packaging/bioconda/meta.yaml.template)
- [Bioconda packaging notes](packaging/bioconda/README.md)

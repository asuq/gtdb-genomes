# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genome)](https://github.com/asuq/gtdb-genome/releases)
[![Code licence: MIT](https://img.shields.io/badge/code-MIT-green.svg)](LICENSE)
[![Bundled data licence: CC BY-SA 4.0](https://img.shields.io/badge/bundled%20data-CC--BY--SA%204.0-blue.svg)](licenses/CC-BY-SA-4.0.txt)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections.

The detailed runtime contract, output layout, retry rules, and bundled-data notes live in [docs/usage-details.md](docs/usage-details.md).

## Quick Start

```bash
gtdb-genomes --gtdb-taxon g__Escherichia --outdir results
```

## Installation

The checked-in Bioconda recipe at
[packaging/bioconda/meta.yaml](packaging/bioconda/meta.yaml) is prepared for
the first public release, but it still awaits the published release sdist
archive and final SHA256 checksum before it can be submitted.

```bash
uv sync --group dev
uv run python -m gtdb_genomes.bootstrap_taxonomy
uv run gtdb-genomes --help
```

The bootstrap step downloads the configured GTDB taxonomy files from the UQ
mirror, verifies them against the release `MD5SUM` listing, and materialises
the local `data/gtdb_taxonomy/<release>/*.tsv.gz` layout used by a source
checkout. Built wheels, sdists, and Conda packages already include that
generated payload and do not need a post-install bootstrap step.


## Command options

See [docs/usage-details.md](docs/usage-details.md) for the full CLI contract.
In short:

- `--gtdb-taxon`: repeatable, matches exact GTDB lineage strings
- `--outdir`:  must be empty or absent
- `--gtdb-release`: (optional) gtdb release number, defaults to `latest`
- `--prefer-genbank`: (optional) prefers paired GenBank accessions and keeps the exact selected version by default
- `--version-latest`: (optional) paired with `--prefer-genbank`, opts into the latest available revision within the selected GenBank family, e.g. `GCA_000005845.2` -> `GCA_000005845.3` if the latter is available
- `--threads`: (optional) number of threads to run, defaults to 8
- `--include`: (optional) downloads extra annotation files from NCBI, e.g. `genome,gff3,protein`, see [NCBI datasets docuemntation](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/how-tos/genomes/download-genome/#choosing-which-data-files-to-include-in-the-data-package)
- `--ncbi-api-key`: (optional) NCBI API key, passed only to the `datasets` command
- `--dry-run`: (optional) supported with automatic planning, prints the planned download list without downloading

## Examples

Small download, quotation required for species-level taxon names with spaces:

```bash
gtdb-genomes \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --outdir results
```

Prefer paired GenBank accessions, keep the exact selected version, and request
extra annotation:

taxa can be selected at any GTDB rank

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

- source-checkout development through `uv run gtdb-genomes ...` or
  `uv run python -m gtdb_genomes ...` after
  `uv run python -m gtdb_genomes.bootstrap_taxonomy`
- maintainer manifest refresh through
  `uv run python -m gtdb_genomes.refresh_taxonomy_manifest`
- the checked-in Bioconda recipe is prepared for the first public release, but
  it still needs the published release archive URL and SHA256 checksum before
  submission

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
- [Bioconda recipe](packaging/bioconda/meta.yaml)

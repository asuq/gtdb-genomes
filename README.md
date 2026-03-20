# gtdb-genomes

[![Python >=3.12](https://img.shields.io/badge/python-%3E%3D3.12-3776AB.svg)](https://www.python.org/downloads/)
[![GitHub release](https://img.shields.io/github/v/release/asuq/gtdb-genome)](https://github.com/asuq/gtdb-genome/releases)
[![Code licence: MIT](https://img.shields.io/badge/code-MIT-green.svg)](LICENSE)
[![Bundled data licence: CC BY-SA 4.0](https://img.shields.io/badge/bundled%20data-CC--BY--SA%204.0-blue.svg)](licenses/CC-BY-SA-4.0.txt)

`gtdb-genomes` downloads NCBI genomes from GTDB taxon selections using bundled
GTDB taxonomy tables for local taxon resolution and the NCBI `datasets` CLI for
all NCBI metadata and genome download operations.

## Licensing

The project code and packaging glue are released under the MIT licence.
Published source and wheel archives also bundle GTDB taxonomy tables under
`data/gtdb_taxonomy`, and those bundled data files remain under
CC BY-SA 4.0 rather than MIT.

The bundled GTDB taxonomy payload is shipped as separate `.tsv.gz` release
tables for runtime use and packaging convenience. Attribution and redistribution
details for that bundled data are recorded in [NOTICE](NOTICE) and the included
[CC BY-SA 4.0 licence text](licenses/CC-BY-SA-4.0.txt). The bundled taxonomy
payload is not relicensed by this project.

## Installation

The checked-in Bioconda recipe at
[packaging/bioconda/meta.yaml](packaging/bioconda/meta.yaml) is prepared for
the first public release, but it still awaits the published release sdist
archive and final SHA256 checksum before it can be submitted.

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
  the requested GTDB token exactly after trimming surrounding whitespace only.
  Matching is case-sensitive, internal species whitespace is preserved, and
  suffix variants are separate taxa. For example,
  `g__Frigididesulfovibrio` does not match `g__Frigididesulfovibrio_A`.
  Species taxa contain spaces and must be quoted in the shell, for example
  `--gtdb-taxon "s__Altiarchaeum hamiconexum"`. Unquoted shell input such as
  `--gtdb-taxon s__Altiarchaeum hamiconexum` is invalid.

- `--outdir`: Output directory must either not exist or exist as an empty
  directory. The tool does not merge into or overwrite a populated output tree.


### Optional options
- `--prefer-genbank`
- `--version-fixed`
- `--threads`
- `--ncbi-api-key`
- `--include`
- `--debug`
- `--keep-temp`
- `--dry-run`

`--prefer-genbank` selects the preferred accession family from NCBI metadata
and, by default, asks `datasets` for the latest available revision in that
family. The downloaded version may differ from the RefSeq version.
Use `--version-fixed` with `--prefer-genbank` to keep the exact selected
version.

Download strategy is always automatic. Smaller supported requests use batch
direct `datasets download genome accession --inputfile ... --filename ...`
passes, while larger requests switch to batch dehydrate/rehydrate.

`--threads` chooses how many CPUs to use for the run. Default: `8`.

Check `gtdb-genomes --help` for details and [usage-details](docs/usage-details.md) on optional options.

## Examples

Small download where automatic planning stays on the direct path:

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

Pin the exact selected GenBank version instead of requesting the latest
revision:

```bash
gtdb-genomes \
  --gtdb-release latest \
  --gtdb-taxon "s__Methanobrevibacter smithii" \
  --prefer-genbank \
  --version-fixed \
  --outdir results/methanobrevibacter-fixed
```

Supported dry-run with automatic planning:

```bash
gtdb-genomes \
  --gtdb-release 95 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --dry-run \
  --outdir /tmp/gtdb_dry_run
```

Dry-runs now check `unzip` early so real-run archive requirements fail fast.

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
5. Optionally prefers the paired GenBank family when `--prefer-genbank` is set,
   then requests either the latest revision in that family or the exact
   selected version when `--version-fixed` is also set.
6. Uses the NCBI `datasets` command for metadata lookup and genome download.
7. Chooses the download strategy automatically based on request size.
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
- the checked-in Bioconda recipe is prepared for the first public release, but
  it still needs the published release archive URL and SHA256 checksum before
  submission

`uv` is a development tool only. Packaged runtime use should not depend on it.

## Additional Documents

- [Usage details](docs/usage-details.md)
- [Real-data validation guide](docs/real-data-validation.md)
- [Pipeline concept](docs/pipeline-concept.md)
- [Step-wise development plan](docs/development-plan.md)
- [Bioconda recipe](packaging/bioconda/meta.yaml)

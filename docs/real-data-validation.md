# Real-Data Validation Guide

This guide turns the release-variant real-data test plan into a repeatable
validation workflow for `gtdb-genomes`.

It is split into three passes:

1. local release-coverage dry-runs
2. local source-checkout real runs
3. remote packaged-runtime validation on a clean Linux + mamba machine

The bundled release set currently covers:

- `80`, `83`: early bacterial-only legacy releases
- `86`: first dual-table release with `bac_taxonomy_*` and `arc_taxonomy_*`
- `89`, `95`, `202`: `bac120` + `ar122`
- `207`, `214`, `220`, `226/latest`: `bac120` + `ar53`

## Real-Data Anchors

These case anchors were checked against the bundled taxonomy data:

- `80 / g__Acholeplasma_C`: one supported genome plus one unsupported `UBA*`
- `80 / g__UBA10030`: one unsupported `UBA*` genome only
- `83 / s__Thermoflexus hugenholtzii`: one genome
- `86 / g__Methanobrevibacter`: six genomes
- `95 / g__Thermoflexus + s__Thermoflexus hugenholtzii`: duplicate-across-taxa
- `202 / g__Bacteroides`: 1025 genomes, suitable for `auto` -> dehydrate
- `207 / g__Methanobrevibacter`: 47 genomes
- `latest / s__Thermoflexus hugenholtzii`: four genomes

## Runner Scripts

Use the provided bash runners:

- local: `bin/run-real-data-tests-local.sh`
- remote: `bin/run-real-data-tests-remote.sh`

Both runners:

- execute cases sequentially
- write each case output under `/tmp/gtdb-realtests/.../<case-id>/`
- capture evidence under `/tmp/gtdb-realtests/.../_evidence/<case-id>/`
- record command lines, stdout, stderr, exit codes, elapsed time, output size,
  copied root TSVs, and one `find .../taxa` directory sample for successful
  real runs

Each runner accepts optional case IDs. Without arguments, it runs the default
mandatory suite.

Examples:

```bash
bin/run-real-data-tests-local.sh
bin/run-real-data-tests-local.sh A1 A6 B4
bin/run-real-data-tests-remote.sh
bin/run-real-data-tests-remote.sh C1 C4 C5
```

## Local Prerequisites

Local validation assumes source-checkout execution through a prepared local
project environment.

The local runner defaults to:

- `UV_CACHE_DIR=/tmp/gtdb_uv_cache`
- `uv run --no-sync gtdb-genomes ...`

Optional local launcher fallback:

- `LOCAL_LAUNCHER_MODE=module` to run
  `${REPO_ROOT}/.venv/bin/python -m gtdb_genomes ...`

Required commands by case family:

- `A1`, `A2`, `A3`, `A4`, `A5`, `A7`, `A8`, `A9`: `uv` only
- `A6`: `uv` plus `datasets`
- `B1` to `B6`: `uv`, `datasets`, and `unzip`

Required environment:

- `NCBI_API_KEY` for metadata-heavy cases such as `B2` and `B6`

The local runner uses:

- `LOCAL_TEST_ROOT`, default `/tmp/gtdb-realtests/local-YYYYMMDD`

Local environment notes:

- offline bundled-data dry-runs remain valid without NCBI access
- `A6` and all `B*` cases require outbound DNS and network access to
  `api.ncbi.nlm.nih.gov`
- the default runner does not add `--debug` to `A6`, because upstream
  `datasets` debug output can print the raw API-key header

## Remote Prerequisites

Remote validation assumes:

- Linux
- `mamba`
- a clean runtime environment
- installed package wheel
- no `uv` in the runtime path

Suggested remote setup:

```bash
mamba create -n gtdb-genome-test python=3.12 pip unzip ncbi-datasets-cli
mamba activate gtdb-genome-test
python -m pip install /path/to/dist/gtdb_genomes-0.1.0-py3-none-any.whl
which gtdb-genomes
gtdb-genomes --help
python -c "from gtdb_genomes.release_resolver import get_release_manifest_path; path = get_release_manifest_path(); assert path.is_file(), path"
```

The remote runner uses:

- `REMOTE_TEST_ROOT`, default `/tmp/gtdb-realtests/remote-YYYYMMDD`
- `RUN_OPTIONAL_LARGE=1` to include the optional `C7` stress case

Required environment:

- `NCBI_API_KEY` for `C2`, `C3`, `C5`, and `C7`

## Case Matrix

### Local dry-run sweep

- `A1`: `80 / g__Acholeplasma_C`
- `A2`: `83 / s__Thermoflexus hugenholtzii`
- `A3`: `86 / g__Methanobrevibacter`
- `A4`: `89 / s__Thermoflexus hugenholtzii`
- `A5`: `95 / g__Thermoflexus + s__Thermoflexus hugenholtzii`
- `A6`: `202 / g__Bacteroides`
- `A7`: `207 / g__Methanobrevibacter`
- `A8`: `release220/220.0 / s__Thermoflexus hugenholtzii`
- `A9`: `latest / g__Methanobrevibacter`

Acceptance:

- exit code `0`
- no output tree
- only `A1` should warn about `PRJNA417962`
- only `A6` is expected to need preview
- `A1`, `A2`, `A3`, `A4`, `A5`, `A7`, `A8`, and `A9` are valid offline local
  checks when the prepared local launcher is available

### Local real runs

- `B1`: `83 / s__Thermoflexus hugenholtzii`
- `B2`: `86 / g__Methanobrevibacter`
- `B3`: `95 / g__Thermoflexus + s__Thermoflexus hugenholtzii`
- `B4`: `80 / g__Acholeplasma_C`
- `B5`: `80 / g__UBA10030`
- `B6`: `207 / g__Methanobrevibacter`

Acceptance highlights:

- `B1`, `B3`, `B6`: exit `0`
- `B2`: exit `6`, because release `86 / g__Methanobrevibacter` includes one
  legacy `UBA*` accession alongside supported genomes
- `B4`: exit `6` and `unsupported_input` in `download_failures.tsv`
- `B5`: exit `7`, manifests present, no payload directories
- `B3`: duplicate rows show `duplicate_across_taxa=true`
- successful direct cases may still record retry-history rows in
  `download_failures.tsv`; treat `failed_accessions=0` in `run_summary.tsv` as
  the success gate instead of requiring a header-only failure TSV

### Remote packaged-runtime runs

- `C1`: `latest / s__Thermoflexus hugenholtzii`
- `C2`: `89 / s__Thermoflexus hugenholtzii`
- `C3`: `207 / g__Methanobrevibacter`
- `C4`: `80 / g__Acholeplasma_C`
- `C5`: `202 / g__Bacteroides`
- `C6`: `release220/220.0 / s__Thermoflexus hugenholtzii`
- `C7`: optional `214 / g__Bacteroides`

Acceptance highlights:

- `C1`, `C2`, `C3`: exit `0`
- `C4`: exit `6` and `unsupported_input`
- `C5`: `download_method_used` is `dehydrate` or
  `dehydrate_fallback_direct`
- `C6`: exit `0`, no output tree
- `C7`: run only with large free disk and a long window
- successful direct cases may still retain retry-history rows in
  `download_failures.tsv`; prioritise the shell exit code and
  `run_summary.tsv.failed_accessions`

## Evidence Layout

For a root such as `/tmp/gtdb-realtests/local-YYYYMMDD`:

```text
/tmp/gtdb-realtests/local-YYYYMMDD/
|-- A1/
|-- B1/
`-- _evidence/
    |-- case-results.tsv
    `-- A1/
        |-- command.sh
        |-- stdout.log
        |-- stderr.log
        |-- combined.log
        |-- summary.txt
        |-- run_summary.tsv
        |-- taxon_summary.tsv
        |-- accession_map.tsv
        |-- download_failures.tsv
        `-- taxa-find.txt
```

## Review Order

If a case fails, review in this order:

1. shell exit code
2. `run_summary.tsv`
3. `download_failures.tsv`
4. one affected `taxon_accessions.tsv`
5. stderr and debug output

Interpretation:

- `4`: zero taxonomy matches
- `5`: preflight or environment failure
- `6`: partial success, must be audited
- `7`: matches existed but no usable genomes were produced

When `A6` or any `B*` case fails with DNS or connection errors before download
work starts, treat that as an external environment problem rather than a
runner bug.

## Capacity Guidance

Use these practical minima:

- local dry-runs: negligible disk
- small real runs: at least 10 GB free
- `C5`: at least 100 GB free
- `C7`: at least 250 GB free and a maintenance window

Do not run heavy cases in parallel.

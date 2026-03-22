# Real-Data Validation Guide

This guide turns the release-variant real-data test plan into a repeatable
validation workflow for `gtdb-genomes`.

Supported external-tool window for the first public release:

- `ncbi-datasets-cli >=18.4.0,<18.22.0`
- `unzip >=6.0,<7.0`

GitHub Actions validates both ends of the supported `datasets` window:
`ncbi-datasets-cli=18.4.0` in Validation A and `ncbi-datasets-cli=18.21.0`
in the remaining validation, live-validation, and release jobs. All jobs keep
`unzip=6.0` inside that supported window.

It is split into three passes:

1. local release-coverage dry-runs
2. local source-checkout real runs
3. remote packaged-runtime validation on a clean Linux + mamba machine

The bundled release set currently covers:

- `80`, `83`: early bacterial-only legacy releases
- `86`: first dual-table release with `bac_taxonomy_*` and `arc_taxonomy_*`
- `89`, `95`, `202`: `bac120` + `ar122`
- `207`, `214`, `220`, `226`: `bac120` + `ar53`

## Real-Data Anchors

These case anchors were checked against the bundled taxonomy data:

- `80 / g__Acholeplasma_C`: one supported genome plus one unsupported `UBA*`
- `80 / g__UBA10030`: one unsupported `UBA*` genome only
- `83 / s__Thermoflexus hugenholtzii`: one genome
- `86 / g__Methanobrevibacter`: six genomes
- `95 / g__Thermoflexus + s__Thermoflexus hugenholtzii`: duplicate-across-taxa
- `202 / g__Bacteroides`: 1025 genomes, suitable for `auto` -> dehydrate
- `207 / g__Methanobrevibacter`: 47 genomes
- `226 / s__Thermoflexus hugenholtzii`: four genomes

## Runner Scripts

Use the provided bash runners:

- local: `bin/run-real-data-tests-local.sh`
- remote: `bin/run-real-data-tests-remote.sh`
- remote wrapper: `bin/run-real-data-tests-server.sh`

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
bin/run-real-data-tests-server.sh
bin/run-real-data-tests-server.sh full
bin/run-real-data-tests-remote.sh
bin/run-real-data-tests-remote.sh C1 C4 C5 C6
```

## Local Prerequisites

Local validation assumes source-checkout execution through a prepared and
synced local project environment with generated GTDB taxonomy payloads.

The local runner defaults to:

- `UV_CACHE_DIR=/tmp/gtdb_uv_cache`
- `uv run gtdb-genomes ...`

Optional local launcher fallback:

- `LOCAL_LAUNCHER_MODE=module` to run
  `${REPO_ROOT}/.venv/bin/python -m gtdb_genomes ...`

Required commands by case family:

- `A1` to `A9`: `uv`, `datasets`, and `unzip`
- `B1` to `B6`: `uv`, `datasets`, and `unzip`

Required bootstrap step:

```bash
uv run python -m gtdb_genomes.bootstrap_taxonomy
```

This downloads the GTDB taxonomy payloads from the pinned UQ mirror metadata in
`data/gtdb_taxonomy/releases.tsv`, verifies each source file against the
release `MD5SUM` or `MD5SUM.txt` listing, and materialises the local
`data/gtdb_taxonomy/<release>/*.tsv.gz` runtime layout used by the source
checkout.

Optional environment:

- `NCBI_API_KEY` for metadata-heavy cases such as `B2` and `B6`

The local runner passes `NCBI_API_KEY` to the CLI as `--ncbi-api-key` for the
cases that provide it.

The local runner uses:

- `LOCAL_TEST_ROOT`, default a unique path such as
  `/tmp/gtdb-realtests/local-YYYYMMDD-XXXXXX`

Local environment notes:

- Dry-runs preflight `unzip` early so real-run archive requirements fail fast.
- zero-match and unsupported-`UBA*`-only dry-runs remain valid without NCBI
  access
- the documented `A*` release-coverage dry-runs and all `B*` cases require
  outbound DNS and network access to
  `api.ncbi.nlm.nih.gov`
- the default runner does not add `--debug` to `A6`, because upstream
  `datasets` debug output can print the raw API-key header

## Remote Prerequisites

Remote validation assumes:

- Linux
- `mamba`
- a clean runtime environment
- installed package wheel
- no `uv` on `PATH`

Suggested remote setup:

```bash
mamba create -n gtdb-genome-test python=3.12 pip unzip=6.0 ncbi-datasets-cli=18.4.0
mamba activate gtdb-genome-test
python -m pip install /path/to/dist/gtdb_genomes-0.1.0-py3-none-any.whl
which gtdb-genomes
gtdb-genomes --help
python -c "from gtdb_genomes.release_resolver import get_release_manifest_path; path = get_release_manifest_path(); assert path.is_file(), path"
```

If the remote environment exposes `python3` rather than `python`, the remote
runner uses that automatically.

The remote runner uses:

- `REMOTE_TEST_ROOT`, default a unique path such as
  `/tmp/gtdb-realtests/remote-YYYYMMDD-XXXXXX`
- `RUN_OPTIONAL_LARGE=1` to include the optional `C7` stress case

Required environment:

- `NCBI_API_KEY` for `C7`

Optional environment:

- `NCBI_API_KEY` for `C2` and `C3`

The remote runner passes `NCBI_API_KEY` to the installed command as
`--ncbi-api-key` when it is provided. `C5` now runs without the key and uses
it opportunistically when present.

## GitHub CI Coverage

The main GitHub Actions CI workflow runs:

- `A1` to `A9`
- `B1` to `B6`
- `C1`, `C2`, `C3`, `C4`, `C5`, and `C6`

The pytest matrix covers Python `3.12`, `3.13`, and `3.14`.

Before `pytest`, `A`, `B`, `C`, and the separate live-validation source-checkout
runner, GitHub Actions runs:

- `uv run python -m gtdb_genomes.bootstrap_taxonomy`

The packaged-runtime `C` coverage is split into separate build and runtime
jobs. The build job uses `uv` to bootstrap and build the wheel, while the
runtime job installs that wheel into a clean mamba environment with no `uv` on
`PATH` before running the remote cases. The runtime sanity check also loads the
bundled taxonomy tables so the installed payload is exercised, not just the
manifest header. In workflow terms, that check resolves `latest` and then
calls `load_release_taxonomy()`.

The CI workflow excludes:

- `C7`

The dedicated release workflow validates both the wheel and `sdist`
packaged-runtime paths with the same build-and-clean-runtime split and reuses
`C5` as part of the packaged-runtime validation gate.

## Remote Server Quickstart

Use this path when you want to prove that the packaged `gtdb-genomes`
command works on another server, rather than validating `uv run` from a source
checkout.

### 1. Build and copy the wheel from the local machine

Build the wheel on the development machine, then copy the wheel to the remote
server. Copy the remote validation scripts as well unless the server already
has a repo checkout containing `bin/`.

```bash
uv build
ls dist/*.whl
scp dist/*.whl user@remote:/tmp/gtdb-genome-remote/
scp bin/run-real-data-tests-server.sh \
  user@remote:/tmp/gtdb-genome-remote/
scp bin/run-real-data-tests-remote.sh \
  bin/real-data-test-common.sh \
  user@remote:/tmp/gtdb-genome-remote/
```

### 2. Create the clean remote runtime

SSH to the remote server and create a fresh packaged-runtime environment:

```bash
ssh user@remote
mamba create -n gtdb-genome-test python=3.12 pip unzip=6.0 ncbi-datasets-cli=18.4.0
mamba activate gtdb-genome-test
python -m pip install /tmp/gtdb-genome-remote/gtdb_genomes-0.1.0-py3-none-any.whl
which gtdb-genomes
gtdb-genomes --help
```

Run the same packaged-data sanity check used by remote `C0-manifest`:

```bash
gtdb-genomes \
  --gtdb-release 226 \
  --gtdb-taxon g__DefinitelyNotReal \
  --outdir /tmp/gtdb-realtests/c0-manifest-output \
  --dry-run
```

This command should exit with code `4`. That is the expected result for the
deliberately missing taxon, and it proves that the installed wheel can load the
bundled release manifest and taxonomy data without relying on a source
checkout.

### 3. Minimum smoke test

Start with a `C6`-style dry-run. It validates CLI wiring and packaged bundled
data without creating an output tree:

```bash
gtdb-genomes \
  --gtdb-release release220/220.0 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --dry-run \
  --outdir /tmp/gtdb-realtests/remote-smoke-c6
```

Then run a `C1` live smoke test. This confirms that the installed command can
perform a real download on the server and does not require `NCBI_API_KEY`.
Automatic strategy selection should keep this case on the direct path:

```bash
gtdb-genomes \
  --gtdb-release 226 \
  --gtdb-taxon "s__Thermoflexus hugenholtzii" \
  --threads 2 \
  --include genome \
  --outdir /tmp/gtdb-realtests/remote-smoke-c1
```

### 4. Full remote matrix

Once the smoke test passes, use `bin/run-real-data-tests-server.sh` as the
preferred on-server entrypoint. It wraps the existing remote runner with simple
presets. If the server already has a repo checkout, run the script from that
checkout. Otherwise, use the copied scripts and keep the installed wheel on
`PATH` as the command under test.

Optional environment:

- `REMOTE_TEST_ROOT` to override the default unique suite root
- `NCBI_API_KEY` for `C2` and `C3`
- `RUN_OPTIONAL_LARGE=1` to include the optional `C7` stress case
- `REAL_DATA_PYTHON_FAULTHANDLER=1` to prefix remote case commands with
  `PYTHONFAULTHANDLER=1`
- `REAL_DATA_DEBUG_SAFE=1` to append `--debug` only to no-key cases such as
  `C1`, `C4`, and `C6`

`C5` runs without `NCBI_API_KEY` and uses it opportunistically when present.

Required environment for `full-large` coverage:

- `NCBI_API_KEY` for `C7`

Examples:

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)
bash /tmp/gtdb-genome-remote/run-real-data-tests-server.sh
```

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)
bash /tmp/gtdb-genome-remote/run-real-data-tests-server.sh full
```

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)
export NCBI_API_KEY="your-ncbi-api-key"
RUN_OPTIONAL_LARGE=1 \
  bash /tmp/gtdb-genome-remote/run-real-data-tests-server.sh full-large
```

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)
bash /tmp/gtdb-genome-remote/run-real-data-tests-server.sh C1 C5 C6
```

### 5. Investigation mode for a failing remote case

If a remote real-data case fails on one runtime, keep the normal CLI behaviour
unchanged and rerun the runner in investigation mode.

Recommended sequence:

```bash
export REMOTE_TEST_ROOT=/tmp/gtdb-realtests/remote-$(date +%Y%m%d)-debug
export REAL_DATA_PYTHON_FAULTHANDLER=1
export REAL_DATA_DEBUG_SAFE=1
bash /tmp/gtdb-genome-remote/run-real-data-tests-server.sh C1
```

Then compare:

- `_evidence/C1/debug.log` when present
- `_evidence/C1/stderr.log`
- copied `run_summary.tsv`

### Expected results

- `C6`: exit `0`, no output tree
- `C1`: exit `0`, output present
- `C4`: exit `6`, `unsupported_input` in `download_failures.tsv`
- `C5`: pass on exit `0`, or on exit `6` when all remaining failures are
  metadata-confirmed suppressed assemblies; `download_method_used` is
  `dehydrate` or `dehydrate_fallback_direct`

### Evidence to inspect on failure

Review these paths under the selected `REMOTE_TEST_ROOT`:

- `_evidence/tool-versions.txt`
- `_evidence/case-results.tsv`
- per-case `summary.txt`
- per-case `stdout.log`
- per-case `stderr.log`
- per-case `combined.log`
- per-case `debug.log` when `REAL_DATA_DEBUG_SAFE=1` is enabled for a real run
- copied `run_summary.tsv`
- copied `download_failures.tsv`
- copied `taxa-find.txt`

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
- `A9`: `226 / g__Methanobrevibacter`

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

- `C1`: `226 / s__Thermoflexus hugenholtzii`
- `C2`: `89 / s__Thermoflexus hugenholtzii`
- `C3`: `207 / g__Methanobrevibacter`
- `C4`: `80 / g__Acholeplasma_C`
- `C5`: `202 / g__Bacteroides`
- `C6`: `release220/220.0 / s__Thermoflexus hugenholtzii`
- `C7`: optional `214 / g__Bacteroides`

Acceptance highlights:

- `C1`, `C2`, `C3`: exit `0`
- `C4`: exit `6` and `unsupported_input`
- `C5`: pass on exit `0`, or on exit `6` when all remaining failures are
  metadata-confirmed suppressed assemblies; `download_method_used` is
  `dehydrate` or `dehydrate_fallback_direct`
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
    |-- tool-versions.txt
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
- `8`: local output materialisation failed after planning or download

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

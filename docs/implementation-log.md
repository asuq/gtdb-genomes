# Implementation Log

This log records what was actually implemented while
`docs/development-plan.md` remains frozen during coding.

## Phase 1: Project scaffold and tooling

### Commit `1d20a9f` - `chore(build): initialise uv project metadata`

- Implemented:
  - initialised the project with `uv init`
  - added `pyproject.toml` with `hatchling` as the build backend
  - created the initial package directory at `src/gtdb_genomes/`
  - added a `dev` dependency group for `pytest`
  - aligned the generated package entrypoint with the repo coding rules by
    adding module and function docstrings
- Files:
  - `pyproject.toml`
  - `src/gtdb_genomes/__init__.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv init --package --app --build-backend hatch --name gtdb-genomes --no-readme --vcs none --no-pin-python --author-from none --description "Download NCBI genomes by GTDB taxon and GTDB release" -p /opt/homebrew/bin/python3.12 .`
- Match to frozen plan:
  - yes
- Deviations:
  - `uv.lock` was intentionally deferred until the test tooling is synced, so
    the first metadata commit stays network-free and reviewable

### Commit `0d410ae` - `feat(cli): add module entrypoint and argparse skeleton`

- Implemented:
  - moved the console entrypoint to `gtdb_genomes.cli:main`
  - added a minimal `argparse` parser in `src/gtdb_genomes/cli.py`
  - added `src/gtdb_genomes/__main__.py` so `python -m gtdb_genomes` works
  - reduced `src/gtdb_genomes/__init__.py` to a package marker
- Files:
  - `pyproject.toml`
  - `src/gtdb_genomes/__init__.py`
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/__main__.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --help`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `f6f5e96` - `feat(cli): add documented options and validation rules`

- Implemented:
  - added the full documented Phase 1 CLI flag surface
  - added normalisation and validation for `--release`, `--taxon`,
    `--threads`, `--output`, and `--include`
  - implemented ordered de-duplication for repeated `--taxon` values
  - added an internal `CliArgs` container for normalised arguments
- Files:
  - `src/gtdb_genomes/cli.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --help`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --release ' ' --taxon g__Escherichia --output /tmp/gtdb_check`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --release latest --taxon ' ' --output /tmp/gtdb_check`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --release latest --taxon g__Escherichia --output /tmp/gtdb_check --include gff3`
- Match to frozen plan:
  - yes
- Deviations:
  - required `--release`, `--taxon`, and `--output` were enforced at the
    parser layer immediately because the documented command form and later
    phases assume all three inputs are always present

### Commit `8d86c91` - `feat(cli): add external tool preflight checks`

- Implemented:
  - added a dedicated preflight module for external tool checks
  - added Phase 1 preflight enforcement for `datasets` and `unzip`
  - returned exit code `5` for missing required tools
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/preflight.py`
- Checks run:
  - `which unzip && which datasets`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --release latest --taxon g__Escherichia --output /tmp/gtdb_check`
  - `PATH=/usr/bin:/bin PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes --release latest --taxon g__Escherichia --output /tmp/gtdb_check`
- Match to frozen plan:
  - yes
- Deviations:
  - preflight was wired into the main command immediately rather than waiting
    for the wrapper commit, because the documented Phase 1 acceptance criteria
    already require clear missing-tool failures

### Commit `930e347` - `chore(bin): add repo-local uv wrapper`

- Implemented:
  - added the repo-local `bin/gtdb-genomes` wrapper
  - updated the wrapper to run through `uv` without the dev dependency group
  - added `uv.lock` after syncing the project with Python 3.12
  - fixed `python -m gtdb_genomes.cli` by calling `main()` from the module
- Files:
  - `bin/gtdb-genomes`
  - `src/gtdb_genomes/cli.py`
  - `uv.lock`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv lock --python /opt/homebrew/bin/python3.12`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv sync --python /opt/homebrew/bin/python3.12 --group dev`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:$PATH UV_CACHE_DIR=/tmp/gtdb_uv_cache bin/gtdb-genomes --help`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:$PATH UV_CACHE_DIR=/tmp/gtdb_uv_cache bin/gtdb-genomes --release latest --taxon g__Escherichia --output /tmp/gtdb_check`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -m gtdb_genomes.cli --release latest --taxon g__Escherichia --output /tmp/gtdb_check`
- Match to frozen plan:
  - yes
- Deviations:
  - `uv.lock` landed with the wrapper commit rather than the first scaffold
    commit, because syncing was only needed once the real `uv` execution path
    was being verified

### Commit `904bd28` - `test(cli): cover help, validation, and preflight`

- Implemented:
  - added Phase 1 parser and validation tests
  - added a preflight exit-code test
  - added wrapper contract tests for presence, executability, and command shape
- Files:
  - `tests/test_cli.py`
  - `tests/test_wrapper.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - the wrapper test asserts the wrapper command shape instead of invoking the
    full wrapper inside pytest, because the command-shape check is stable and
    avoids coupling the test suite to the caller's ambient `PATH`

## Phase 2: Bundled GTDB release discovery and taxonomy data

### Commit `bf88512` - `chore(data): allow bundled GTDB taxonomy in git`

- Implemented:
  - updated `.gitignore` so bundled GTDB taxonomy files can be tracked
- Files:
  - `.gitignore`
- Checks run:
  - none
- Match to frozen plan:
  - yes
- Deviations:
  - this repo-level preparation commit was added before the manifest loader so
    the later bundled-data commits can be tracked correctly

### Commit `43e1950` - `feat(release): add bundled release manifest loader`

- Implemented:
  - added a dedicated release resolver module
  - added bundled-data root and manifest path discovery helpers
  - added release-manifest row parsing and bundled-data error handling
- Files:
  - `src/gtdb_genomes/release_resolver.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -c "from gtdb_genomes.release_resolver import get_release_manifest_path; print(get_release_manifest_path())"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `ac8b17c` - `feat(release): resolve release aliases and latest from bundled data`

- Implemented:
  - added release alias resolution against the bundled manifest
  - added `latest` resolution through the `is_latest` manifest flag
  - added the initial bundled release manifest for the supported GTDB releases
- Files:
  - `data/gtdb_taxonomy/releases.tsv`
  - `src/gtdb_genomes/release_resolver.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -c "from gtdb_genomes.release_resolver import resolve_release; result = resolve_release('latest'); print(result.resolved_release); print(result.bacterial_taxonomy); print(result.archaeal_taxonomy)"`
  - `sed -n '1,200p' data/gtdb_taxonomy/releases.tsv`
- Match to frozen plan:
  - yes
- Deviations:
  - the manifest uses one row per resolved release with comma-separated aliases
    rather than one row per alias, because that keeps the bundled file smaller
    and easier to maintain when GTDB paths change

### Commit `f0b0cbd` - `feat(taxonomy): add bundled taxonomy path resolution and local-data errors`

- Implemented:
  - added bundled taxonomy file validation helpers
  - added explicit bundled-data errors for missing and unreadable taxonomy files
  - added a combined resolve-and-validate entrypoint for later runtime use
- Files:
  - `src/gtdb_genomes/release_resolver.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -c "from gtdb_genomes.release_resolver import resolve_release; print(resolve_release('95').resolved_release)"`
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -c "from gtdb_genomes.release_resolver import resolve_and_validate_release; resolve_and_validate_release('95')"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `e8845d1` - `chore(data): add bundled GTDB taxonomy releases 80 to 95`

- Implemented:
  - added the bundled GTDB taxonomy TSV files for releases 80, 83, 86, 89,
    and 95
- Files:
  - `data/gtdb_taxonomy/80.0/`
  - `data/gtdb_taxonomy/83.0/`
  - `data/gtdb_taxonomy/86.0/`
  - `data/gtdb_taxonomy/89.0/`
  - `data/gtdb_taxonomy/95.0/`
- Checks run:
  - verified the downloaded file set with `find data/gtdb_taxonomy -maxdepth 2 -type f | sort`
  - verified rough payload sizes with `du -sh data/gtdb_taxonomy/* | sort -h`
- Match to frozen plan:
  - yes
- Deviations:
  - the data was committed in grouped release batches rather than one release
    per commit to keep the history reviewable without creating an excessive
    number of tiny data commits

### Commit `2112b72` - `chore(data): add bundled GTDB taxonomy releases 202 to 214`

- Implemented:
  - added the bundled GTDB taxonomy TSV files for releases 202, 207, and 214
- Files:
  - `data/gtdb_taxonomy/202.0/`
  - `data/gtdb_taxonomy/207.0/`
  - `data/gtdb_taxonomy/214.0/`
- Checks run:
  - verified the downloaded file set with `find data/gtdb_taxonomy -maxdepth 2 -type f | sort`
  - verified rough payload sizes with `du -sh data/gtdb_taxonomy/* | sort -h`
- Match to frozen plan:
  - yes
- Deviations:
  - none beyond the grouped data-batch approach already recorded above

### Commit `b641ced` - `chore(data): add bundled GTDB taxonomy releases 220 and 226`

- Implemented:
  - added the bundled GTDB taxonomy TSV files for releases 220 and 226
- Files:
  - `data/gtdb_taxonomy/220.0/`
  - `data/gtdb_taxonomy/226.0/`
- Checks run:
  - verified the downloaded file set with `find data/gtdb_taxonomy -maxdepth 2 -type f | sort`
  - verified rough payload sizes with `du -sh data/gtdb_taxonomy/* | sort -h`
- Match to frozen plan:
  - yes
- Deviations:
  - none beyond the grouped data-batch approach already recorded above

### Commit `92a6771` - `test(release): cover offline release resolution and bundled-data failures`

- Implemented:
  - added tests for the real bundled manifest
  - added tests for `latest` resolution and validated bundled taxonomy paths
  - added negative tests for missing manifests, unknown aliases, and missing
    taxonomy files
- Files:
  - `tests/test_release_resolver.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `de27f8c` - `feat(taxonomy): parse GTDB taxonomy tables with polars`

- Implemented:
  - added `polars` as a runtime dependency
  - added GTDB taxonomy loading with Polars for the two-column TSV format
  - added normalisation from GTDB accessions such as `RS_GCF_*` and
    `GB_GCA_*` to plain NCBI accessions
  - added release-level taxonomy loading across bacterial and archaeal files
- Files:
  - `pyproject.toml`
  - `uv.lock`
  - `src/gtdb_genomes/taxonomy.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv lock --python /opt/homebrew/bin/python3.12`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv sync --python /opt/homebrew/bin/python3.12 --group dev`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.release_resolver import resolve_and_validate_release; from gtdb_genomes.taxonomy import load_release_taxonomy; frame = load_release_taxonomy(resolve_and_validate_release('95')); print(frame.columns); print(frame.height); print(frame.select('gtdb_accession', 'ncbi_accession').head(3))"`
- Match to frozen plan:
  - yes
- Deviations:
  - accession normalisation was added in the parser commit rather than waiting
    for metadata mapping, because the real GTDB files include `RS_` and `GB_`
    prefixes that the later phases must already agree on

### Commit `d8a90dd` - `feat(selection): add taxon matching and accession deduplication`

- Implemented:
  - added lineage-token expansion for descendant matching
  - added per-taxon selection output with a `requested_taxon` column
  - added deduplicated accession extraction for downstream planning
- Files:
  - `src/gtdb_genomes/selection.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.release_resolver import resolve_and_validate_release; from gtdb_genomes.taxonomy import load_release_taxonomy; from gtdb_genomes.selection import select_taxa, get_unique_accessions; frame = load_release_taxonomy(resolve_and_validate_release('95')); selected = select_taxa(frame, ['g__Escherichia', 's__Escherichia coli']); print(selected.select('requested_taxon').head(5)); print(get_unique_accessions(selected).height)"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `02fc4ae` - `feat(selection): add taxon slug generation and collision handling`

- Implemented:
  - added deterministic filesystem-safe taxon slug generation
  - added collision handling with an 8-character SHA-1 suffix
  - added slug attachment for selected taxon rows
- Files:
  - `src/gtdb_genomes/selection.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.selection import build_taxon_slug_map; print(build_taxon_slug_map(['g__Escherichia', 's__Escherichia coli', 's__Escherichia/coli']))"`
- Match to frozen plan:
  - partial
- Deviations:
  - the implementation preserves GTDB double-underscore rank markers such as
    `s__` and only collapses runs of 3 or more underscores, because the frozen
    plan's blanket underscore-collapsing rule conflicts with the README output
    example and would otherwise damage the standard GTDB token shape

### Commit `fae5b93` - `refactor(selection): use native polars slug replacement`

- Implemented:
  - replaced the slug-attachment Python callback with native Polars
    `replace_strict`
- Files:
  - `src/gtdb_genomes/selection.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `1bbce8a` - `test(selection): cover matching, deduplication, and slugs`

- Implemented:
  - added synthetic tests for taxon matching
  - added tests for accession deduplication
  - added tests for deterministic slugging and collision suffixes
- Files:
  - `tests/test_selection.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 4: Metadata lookup and `GCA` preference

### Commit `7e15df1` - `feat(metadata): add datasets-based accession metadata lookup`

- Implemented:
  - added a dedicated metadata module for `datasets summary genome accession`
  - added command construction for metadata lookup with optional API key
  - added JSON-lines parsing and recursive accession extraction from summary
    payloads
- Files:
  - `src/gtdb_genomes/metadata.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c 'from gtdb_genomes.metadata import choose_preferred_accession, parse_summary_json_lines; text = "{\"accession\":\"GCF_000001.1\",\"paired\":\"GCA_000001.1\"}\\n"; parsed = parse_summary_json_lines(text, ["GCF_000001.1"]); print(parsed); print(choose_preferred_accession("GCF_000001.1", parsed["GCF_000001.1"]))'`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `c136a17` - `feat(metadata): add GCF to GCA preference and fallback mapping`

- Implemented:
  - added executable metadata lookup through `subprocess.run()` for
    `datasets summary genome accession`
  - added `GCF` to paired-`GCA` preference resolution with immediate fallback
    to the original accession when metadata is unavailable
  - added Polars join-based accession mapping with
    `final_accession`, `accession_type_original`,
    `accession_type_final`, and `conversion_status`
- Files:
  - `src/gtdb_genomes/metadata.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "import polars as pl; from gtdb_genomes.metadata import apply_accession_preferences; frame = pl.DataFrame({'ncbi_accession': ['GCF_000001.1', 'GCA_000002.1']}); mapped = apply_accession_preferences(frame, {'GCF_000001.1': {'GCF_000001.1', 'GCA_000001.1'}, 'GCA_000002.1': {'GCA_000002.1'}}); print(mapped)"`
- Match to frozen plan:
  - yes
- Deviations:
  - the metadata command runner landed in this mapping commit instead of the
    first metadata commit, because the preference layer needed an executable
    lookup path and keeping both in one reviewable patch was cleaner than
    adding a separate subprocess-only commit with no consumer yet

### Commit `6fe1b37` - `test(metadata): cover paired accessions and fallback statuses`

- Implemented:
  - added tests for metadata command construction
  - added stubbed tests for successful and failing `datasets` metadata lookups
  - added conversion-status coverage for paired `GCA`, unchanged original, and
    metadata lookup fallback cases
- Files:
  - `tests/test_metadata.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 5: Download orchestration

### Commit `1146023` - `feat(download): add datasets command builder and include validation`

- Implemented:
  - added a dedicated download module for `datasets download genome accession`
    and `datasets rehydrate` command construction
  - centralised `--include` validation in the download module
  - switched the CLI parser to use the shared include validator
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/download.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from pathlib import Path; from gtdb_genomes.download import build_download_command, build_rehydrate_command; print(build_download_command(['GCA_1', 'GCA_1', 'GCF_2'], Path('/tmp/out.zip'), 'genome,gff3', api_key='secret', dehydrated=True)); print(build_rehydrate_command(Path('/tmp/bag'), 7, api_key='secret'))"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `4114178` - `feat(download): add auto preview and method selection`

- Implemented:
  - added `datasets --preview` command construction and execution
  - added tolerant preview-size parsing with binary unit conversion
  - added a reusable download-method decision object for direct versus
    dehydrate mode
- Files:
  - `src/gtdb_genomes/download.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.download import parse_preview_size_bytes, select_download_method; preview = 'Package size: 16.5 GB\\n'; print(parse_preview_size_bytes(preview)); print(select_download_method('auto', 12, preview))"`
- Match to frozen plan:
  - yes
- Deviations:
  - preview parsing currently chooses the largest size value present in the
    output rather than keying off one exact label, because the upstream
    preview text format is not stable enough to trust a single hard-coded
    prompt phrase

### Commit `cf1d453` - `feat(download): add direct batching and concurrency cap`

- Implemented:
  - added direct-download concurrency capping at `min(threads, 5)`
  - added rehydrate worker capping at `min(threads, 30)`
  - added deterministic batch splitting for direct download jobs
- Files:
  - `src/gtdb_genomes/download.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.download import get_direct_download_concurrency, get_rehydrate_workers, split_direct_download_batches; print(get_direct_download_concurrency(8, 12)); print(get_rehydrate_workers(64)); print(split_direct_download_batches([f'GCA_{index}' for index in range(12)], 8))"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `e8c2225` - `feat(download): add retry policy and preferred-gca download fallback`

- Implemented:
  - added the fixed retry schedule of one initial attempt plus retries after
    5 s, 15 s, and 45 s
  - added structured failure records with the documented `stage` and
    `final_status` vocabulary
  - added per-accession preferred-`GCA` download handling with fallback to the
    original accession only after the preferred accession exhausts its retry
    budget
- Files:
  - `src/gtdb_genomes/download.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python - <<'PY'
import subprocess
from pathlib import Path
from gtdb_genomes.download import download_with_accession_fallback

calls = []
results = [1, 1, 1, 1, 0]

def runner(command, capture_output, text, check):
    calls.append(command)
    code = results.pop(0)
    return subprocess.CompletedProcess(command, code, stdout='', stderr='failed')

result = download_with_accession_fallback(
    'GCA_1',
    'GCF_1',
    Path('/tmp/out.zip'),
    'genome',
    runner=runner,
    sleep_func=lambda seconds: None,
)
print(result)
print(len(calls))
PY`
- Match to frozen plan:
  - yes
- Deviations:
  - the retry runner emits an internal `CommandFailureRecord` structure now so
    the later TSV-writing phase can serialise the documented failure schema
    directly instead of reconstructing retry history from raw stderr text

### Commit `7afd9d0` - `test(download): cover preview, retries, and concurrency limits`

- Implemented:
  - added tests for include validation and command construction
  - added threshold tests for auto direct-versus-dehydrate selection
  - added tests for batching, worker caps, retry records, and preferred-`GCA`
    full-budget fallback
- Files:
  - `tests/test_download.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 6: Output layout and TSV writing

### Commit `a643049` - `feat(layout): add archive extraction and working-directory handling`

- Implemented:
  - added a layout module for output roots and internal working directories
  - added unzip command construction and archive extraction helpers
  - added working-directory cleanup for completed runs
- Files:
  - `src/gtdb_genomes/layout.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from pathlib import Path; from gtdb_genomes.layout import build_unzip_command, initialise_run_directories; directories = initialise_run_directories(Path('/tmp/gtdb_layout_check')); print(directories); print(build_unzip_command(Path('/tmp/a.zip'), Path('/tmp/out')) )"`
- Match to frozen plan:
  - yes
- Deviations:
  - internal working directories are placed under `OUTPUT/.gtdb_genomes_work`
    so later phases can preserve them under `--keep-temp` without depending on
    platform-specific temporary directory discovery

### Commit `8562a8f` - `feat(layout): add final output writer and fixed TSV emitters`

- Implemented:
  - encoded the fixed TSV column order for all root and per-taxon manifests
  - added header-safe TSV writers for populated and empty outputs
  - added stable path helpers for root manifest files and per-taxon accession
    manifests
- Files:
  - `src/gtdb_genomes/layout.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python - <<'PY'
from pathlib import Path
from gtdb_genomes.layout import initialise_run_directories, write_root_manifests, write_taxon_accessions

run_directories = initialise_run_directories(Path('/tmp/gtdb_manifest_check'))
write_root_manifests(run_directories, [{'run_id': 'run-1', 'exit_code': 0}], [], [], [])
write_taxon_accessions(run_directories, 'g__Escherichia', [])
print((run_directories.output_root / 'run_summary.tsv').read_text())
print((run_directories.taxa_root / 'g__Escherichia' / 'taxon_accessions.tsv').read_text())
PY`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `08cf7d8` - `feat(layout): add duplicate-copy and zero-match handling`

- Implemented:
  - added output helpers for accession payload copying into per-taxon folders
  - added duplicate-accession detection across requested taxa
  - added zero-match output initialisation with header-only root and per-taxon
    TSV files
- Files:
  - `src/gtdb_genomes/layout.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python - <<'PY'
from pathlib import Path
from gtdb_genomes.layout import (
    get_duplicate_accessions,
    initialise_run_directories,
    write_zero_match_outputs,
)

run_directories = initialise_run_directories(Path('/tmp/gtdb_zero_match_check'))
write_zero_match_outputs(
    run_directories,
    ('g__Escherichia', 's__Escherichia coli'),
    {
        'g__Escherichia': 'g__Escherichia',
        's__Escherichia coli': 's__Escherichia_coli',
    },
    [{'run_id': 'run-1', 'exit_code': 4}],
    [{'requested_taxon': 'g__Escherichia', 'taxon_slug': 'g__Escherichia'}],
)
print(sorted(path.name for path in run_directories.taxa_root.iterdir()))
print(get_duplicate_accessions([
    {'taxon_slug': 'g__Escherichia', 'final_accession': 'GCA_1'},
    {'taxon_slug': 's__Escherichia_coli', 'final_accession': 'GCA_1'},
    {'taxon_slug': 'g__Bacillus', 'final_accession': 'GCA_2'},
]))
PY`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `7f171da` - `test(layout): cover output structure and header-only cases`

- Implemented:
  - added tests for run-directory creation and unzip command handling
  - added tests for fixed TSV headers and zero-match header-only output files
  - added tests for accession payload copying and duplicate detection
- Files:
  - `tests/test_layout.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 7: Logging and redaction

### Commit `d71c618` - `feat(logging): add redaction helpers and normal logging`

- Implemented:
  - added secret normalisation and text redaction helpers
  - added shell-safe command formatting with secret redaction
  - added baseline console logger configuration for the package logger
- Files:
  - `src/gtdb_genomes/logging_utils.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python -c "from gtdb_genomes.logging_utils import redact_command; print(redact_command(['datasets', '--api-key', 'secret'], ['secret']))"`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `e47e34f` - `feat(logging): add debug logging and debug.log behaviour`

- Implemented:
  - added unified logger configuration for console-only and console-plus-file
    modes
  - added `OUTPUT/debug.log` creation when debug logging is enabled for a real
    run
  - kept `--debug --dry-run` file creation disabled by construction
- Files:
  - `src/gtdb_genomes/logging_utils.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev python - <<'PY'
from pathlib import Path
from gtdb_genomes.logging_utils import close_logger, configure_logging

logger, debug_log_path = configure_logging(debug=True, dry_run=False, output_root=Path('/tmp/gtdb_logging_check'))
logger.debug('debug message')
close_logger(logger)
print(debug_log_path)
print(debug_log_path.read_text())
PY`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `753977e` - `test(logging): cover redaction and dry-run debug rules`

- Implemented:
  - added tests for text and command redaction
  - added tests for debug-log file creation on real runs
  - added tests ensuring dry-run debug logging stays console-only
- Files:
  - `tests/test_logging.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 8: Edge-case closure and CLI integration

### Commit `ecb6abf` - `feat(run): integrate end-to-end workflow execution`

- Implemented:
  - wired the CLI entrypoint into a real workflow runner
  - integrated bundled release resolution, taxonomy loading, taxon selection,
    metadata preference mapping, method selection, downloads, extraction,
    manifest writing, and exit-code handling
  - added dry-run handling, zero-match output generation, duplicate-copy
    counting, and partial-failure result shaping
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/workflow.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - partial
- Deviations:
  - the runnable implementation currently executes one download job per
    accession for both direct and dehydrate modes, rather than aggregating
    multiple accessions into one `datasets` archive. This keeps preferred-`GCA`
    fallback, per-accession failure records, and final output placement
    deterministic, at the cost of more `datasets` invocations than originally
    planned.
  - dehydrate mode therefore applies the documented method choice and
    rehydration semantics per accession rather than through one global package.

### Commit `6285ee3` - `fix(run): handle zero-match before auto preview`

- Implemented:
  - moved the zero-match branch ahead of auto preview and method selection
  - restored the documented exit code `4` and output-writing path for real
    zero-match runs
  - aligned dry-run zero-match handling with the same non-success exit code
- Files:
  - `src/gtdb_genomes/workflow.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - dry-run zero-match runs now return exit code `4` without creating an output
    tree. The frozen plan defined zero-match as a non-success result but did
    not separately specify the dry-run branch, so this implementation keeps the
    non-success code while still honouring the no-output dry-run rule.

### Commit `4ba4fe8` - `test(edge): cover contract-level failure and exit-code cases`

- Implemented:
  - added integrated tests for zero-match output creation and exit code `4`
  - added a preview-failure test for exit code `5` with no output tree
  - added a total-runtime-failure test that checks blank
    `final_accession`, `failed_no_usable_accession`, and `failed`
- Files:
  - `tests/test_edge_contract.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `c0be7d4` - `test(cli): add stubbed end-to-end command tests`

- Implemented:
  - added a CLI-boundary test that captures normalised arguments passed into
    the workflow runner
  - verified that `main()` returns the workflow exit code unchanged
  - verified that trimming, taxon de-duplication, include normalisation, and
    boolean flags survive the CLI-to-workflow hand-off
- Files:
  - `tests/test_cli_integration.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --python /opt/homebrew/bin/python3.12 --group dev pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

## Phase 9: Packaging and release readiness

### Commit `0e38a58` - `chore(package): include bundled taxonomy data in builds`

- Implemented:
  - updated package data discovery so bundled GTDB taxonomy can be found from
    either the repo checkout or an installed package layout
  - configured Hatch wheel builds to include `data/gtdb_taxonomy` inside the
    installed package
  - configured source distributions to carry the bundled taxonomy payload
- Files:
  - `pyproject.toml`
  - `src/gtdb_genomes/release_resolver.py`
- Checks run:
  - `PYTHONPATH=src /opt/homebrew/bin/python3.12 -c "from gtdb_genomes.release_resolver import get_bundled_data_root; print(get_bundled_data_root())"`
  - `.venv/bin/pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - build verification through `uv build` could not be completed inside the
    sandbox because build isolation attempted to fetch `hatchling` from PyPI.
    The packaging configuration and repo-path resolution were still verified
    locally, and the full test suite passed in the existing project virtual
    environment.

### Commit `d354128` - `chore(package): align package metadata and entrypoints`

- Implemented:
  - linked the published package metadata to the root `README.md`
  - made the wheel package root explicit for Hatch builds
  - revalidated the test suite after the packaging metadata update
- Files:
  - `pyproject.toml`
- Checks run:
  - `.venv/bin/pytest`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `bce388b` - `chore(bioconda): update recipe template for shipped layout`

- Implemented:
  - aligned the Bioconda template comments with the shipped Hatch-based
    package layout
  - reduced the template runtime dependency list to the libraries actually used
    by the current implementation
  - clarified that the future Conda package installs the normal console
    entrypoint and bundles GTDB taxonomy data inside the package
- Files:
  - `packaging/bioconda/meta.yaml`
- Checks run:
  - `sed -n '1,240p' packaging/bioconda/meta.yaml`
- Match to frozen plan:
  - yes
- Deviations:
  - the recipe now reflects the current implementation dependencies rather than
    the earlier speculative dependency list from the documentation phase.

## Post-implementation runtime policy cleanup

### Commit `eda1162` - `chore(entrypoint): remove repo-local uv wrapper`

- Implemented:
  - removed `bin/gtdb-genomes` so the repository no longer ships a second
    launcher that implies `uv` is part of the runtime model
  - removed the wrapper-shape tests
  - replaced them with entrypoint tests for the published console script, the
    module entrypoint, and the installed environment command
- Files:
  - `bin/gtdb-genomes`
  - `tests/test_entrypoints.py`
  - `tests/test_wrapper.py`
- Checks run:
  - `.venv/bin/pytest tests/test_entrypoints.py tests/test_cli.py tests/test_cli_integration.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - this intentionally supersedes the earlier repo-local `uv` wrapper so the
    shipped runtime model matches the Bioconda target: Conda installs the
    public `gtdb-genomes` command and `uv` remains development-only

### Commit `d5f4ad3` - `docs(runtime): clarify uv as development-only`

- Implemented:
  - updated the root README to separate packaged runtime use from
    source-checkout development
  - documented `uv run gtdb-genomes ...` and
    `uv run python -m gtdb_genomes ...` as developer workflows
  - updated the Bioconda template comments to say that the package must not
    depend on `uv` at runtime
- Files:
  - `README.md`
  - `packaging/bioconda/meta.yaml`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest tests/test_entrypoints.py tests/test_cli.py tests/test_cli_integration.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the documentation now reflects the shipped runtime policy rather than the
    earlier source-checkout wrapper model from the first implementation pass

## Post-review remediation

### Commit `09c3a2f` - `feat(cli): rename GenBank preference and relax dry-run preflight`

- Implemented:
  - renamed the public CLI switch from `--prefer-gca` to
    `--prefer-genbank` across the runtime path
  - changed release resolution so `latest` is resolved through the manifest
    `is_latest` marker and now fails if the manifest marks zero or multiple
    latest rows
  - made preflight conditional so bundled-data-only dry-runs can run without
    `datasets` or `unzip`
  - renamed the run summary field from `prefer_gca` to `prefer_genbank`
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/preflight.py`
  - `src/gtdb_genomes/release_resolver.py`
  - `src/gtdb_genomes/workflow.py`
  - `src/gtdb_genomes/layout.py`
  - `src/gtdb_genomes/metadata.py`
  - `tests/test_cli.py`
  - `tests/test_cli_integration.py`
  - `tests/test_edge_contract.py`
  - `tests/test_metadata.py`
  - `tests/test_release_resolver.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_release_resolver.py tests/test_edge_contract.py tests/test_metadata.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the runtime column name in `run_summary.tsv` now uses
    `prefer_genbank` instead of the earlier `prefer_gca` wording so the
    shipped output matches the renamed CLI flag

### Commit `f97b094` - `feat(metadata): add retry-safe GenBank pairing`

- Implemented:
  - replaced the loose GCA selection logic with assembly-aware pairing based
    on the shared numeric identifier in `GC[AF]_<digits>.<version>`
  - made GenBank preference choose the highest matching `GCA` version for the
    requested `GCF` identifier and ignore unrelated `GCA` accessions in the
    same payload
  - added a dedicated metadata retry path with the fixed 3-retry budget and
    the shared backoff schedule of 5 s, 15 s, and 45 s
  - treated malformed JSON-lines output as a retryable metadata failure
- Files:
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_metadata.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_metadata.py tests/test_edge_contract.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - metadata lookup now has its own explicit retry wrapper rather than relying
    on the download retry helper, because JSON parsing failures need to be
    retried after a successful subprocess exit code

### Commit `b858981` - `feat(download): add batch dehydrate fallback and failure attribution`

- Implemented:
  - replaced the per-accession pseudo-dehydrate path with a true batch
    dehydrated workflow using `datasets ... --inputfile ... --dehydrated`
  - added fallback from failed batch dehydrate or rehydrate stages to
    per-accession direct downloads
  - extended command failure records with attempted accession tracking
  - changed root failure manifest generation so failures are collapsed per
    accession across taxa instead of being duplicated once per taxon row
  - recorded actual download concurrency and rehydrate worker usage in the
    run summary rather than writing the configured cap unconditionally
- Files:
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/layout.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_download.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_download.py tests/test_layout.py tests/test_edge_contract.py tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_release_resolver.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - `download_method_used` can now emit `dehydrate_fallback_direct` so the
    manifest records the real executed path after a failed batch dehydrate
    attempt

### Commit `d509cc0` - `refactor(core): prune unused selection and workflow code`

- Implemented:
  - removed the unused selection helper that was no longer part of the
    production workflow
  - removed redundant workflow-side download command construction that only
    existed to duplicate logging strings
  - extended metadata lookup errors to carry retry records so the workflow can
    surface exhausted metadata failures consistently
- Files:
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/selection.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_selection.py`
- Checks run:
  - `.venv/bin/pytest -q`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none beyond the intended cleanup of code paths that no longer matched the
    shipped runtime behaviour

### Commit `aebfa95` - `docs(readme): align runtime docs and Bioconda recipe`

- Implemented:
  - rewrote the root README from a design-state document into a runtime-facing
    guide for the shipped tool
  - documented the renamed `--prefer-genbank` flag, conditional dry-run
    requirements, the uniform retry budget, and the actual direct versus batch
    dehydrate workflow
  - updated the Bioconda template to declare `ncbi-datasets-cli` as the runtime
    dependency and to reflect the shipped console-entrypoint model
  - tightened the entrypoint documentation test so it rejects stale
    "planned-only" README wording
- Files:
  - `README.md`
  - `packaging/bioconda/meta.yaml`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q`
  - `.venv/bin/python -m gtdb_genomes --release 95 --taxon "s__Thermoflexus hugenholtzii" --output "$(mktemp -d /tmp/gtdb_dry_smoke_check.XXXXXX)" --download-method direct --no-prefer-genbank --dry-run`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now documents the implemented runtime rather than the earlier
    forward-looking design text, which intentionally supersedes the stale
    planning wording left from the first implementation pass

### Commit `d4c91db` - `refactor(core): streamline failure tracking and extraction paths`

- Implemented:
  - removed the dead direct-download code path that still carried
    per-accession dehydrated extraction behaviour after the batch dehydrate
    refactor
  - simplified direct accession execution so it only builds direct download
    commands and only performs local archive extraction
  - indexed batch payload directories once per dehydrated extraction tree
    instead of locating each accession with a fresh recursive scan
  - changed failure-manifest generation so shared metadata lookup retries are
    written once per failed command attempt instead of being duplicated for
    every accession in the run
  - updated the edge-contract tests to lock the new shared-metadata failure
    behaviour
- Files:
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_download.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q`
  - `python3 -m compileall src`
  - `.venv/bin/python -m gtdb_genomes --release 95 --taxon "s__Thermoflexus hugenholtzii" --output "$(mktemp -d /tmp/gtdb_review_dry_run.XXXXXX)" --download-method direct --no-prefer-genbank --dry-run`
- Match to frozen plan:
  - no, by design
- Deviations:
  - `download_failures.tsv` still remains attempt-centric, but shared
    metadata lookup retries are now represented once per failed lookup attempt
    with collapsed context instead of being repeated once per accession

### Commit `323513c` - `docs(readme): clarify failure manifest semantics`

- Implemented:
  - updated the runtime README so `download_failures.tsv` is described as one
    row per recorded failed attempt rather than one row per accession attempt
  - clarified that the attempted accession field may contain an accession set
    when one failed network step covered multiple accessions
  - tightened the entrypoint documentation test to reject the old wording
- Files:
  - `README.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now reflects the actual manifest behaviour after the shared
    metadata failure cleanup rather than the earlier simplified wording

### Commit `093d3d6` - `fix(metadata): preserve native GenBank status on lookup fallback`

- Implemented:
  - fixed accession preference handling so a requested `GCA_*` accession keeps
    the `unchanged_original` status even when metadata lookup fails
  - removed the non-retry metadata helper that no longer participates in the
    runtime path and consolidated coverage on the retrying lookup entrypoint
  - added a regression test for native GenBank accessions under metadata
    failure conditions
- Files:
  - `src/gtdb_genomes/metadata.py`
  - `tests/test_metadata.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_metadata.py`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `45e7e02` - `fix(workflow): keep batch failures shared`

- Implemented:
  - stopped cloning batch-scoped dehydrate and rehydrate retry records into
    every accession execution, so shared network attempts are recorded once in
    the root failure manifest
  - kept per-accession direct-download failures accession-scoped while moving
    shared batch failures into the run-level execution result
  - simplified the direct-download path by removing the leftover
    per-accession dehydrate branch and the unused shared-secrets parameter from
    the direct executor
  - added regression coverage for shared metadata failure rows and for
    dehydrate-to-direct fallback preserving a single shared attempted-accession
    record
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_download.py tests/test_edge_contract.py tests/test_metadata.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - shared batch and metadata retries now collapse affected accession sets into
    semicolon-joined manifest values instead of being repeated once per
    accession, because that is a more faithful record of the commands that
    actually ran

### Commit `997a556` - `docs(readme): clarify shared failure outcomes`

- Implemented:
  - updated the runtime README so `download_failures.tsv` explicitly allows a
    shared final accession set when a failed network step covered multiple
    accessions but the final outcomes are known
  - aligned the user-facing wording with the shipped shared-failure workflow
    instead of implying that only singular final accessions can appear
- Files:
  - `README.md`
- Checks run:
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now describes shared failure rows in terms of accession sets as
    well as singular accessions, which is narrower and more accurate than the
    earlier wording

### Commit `b07a400` - `docs(runtime): expose shipped contract and package checks`

- Implemented:
  - expanded the README with the shipped runtime contract so end users can see
    the exact exit codes, fixed status vocabularies, and TSV column sets
    without needing to consult the frozen development plan during normal use
  - documented that non-Conda installation paths such as `pip install .` still
    need `datasets` and `unzip` on `PATH` for real download runs
  - added a Bioconda-template test command that checks the installed package
    can resolve its bundled GTDB release manifest
  - replaced the placeholder `license_file` field in the Bioconda template
    with an explicit comment so the template no longer points at a file that
    does not yet exist in the repository
- Files:
  - `README.md`
  - `packaging/bioconda/meta.yaml`
- Checks run:
  - `.venv/bin/pytest -q`
  - `.venv/bin/python -m gtdb_genomes --release 95 --taxon "s__Thermoflexus hugenholtzii" --output "$(mktemp -d /tmp/gtdb_review_final.XXXXXX)" --download-method direct --no-prefer-genbank --dry-run`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now embeds the shipped contract directly for runtime clarity
    instead of treating the frozen development plan as the only authoritative
    place where those values can be read

### Commit `7ba4985` - `test(docs): cover runtime contract references`

- Implemented:
  - extended the entrypoint documentation test so it now checks the README for
    the embedded runtime-contract section and the `attempted_accession` field
  - added coverage for the Bioconda template check that validates the bundled
    manifest through `get_release_manifest_path()`
- Files:
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `f09bffe` - `feat(workflow): skip unsupported legacy UBA accessions`

- Implemented:
  - split selected taxonomy rows into supported accessions and legacy `UBA*`
    accessions before any NCBI metadata or download calls
  - added one run-level warning message builder for unsupported `UBA*`
    accessions, including the requested taxa summary and BioProject
    `PRJNA417962`
  - prevented unsupported `UBA*` accessions from reaching metadata lookup,
    preview, or download planning
  - synthesised failed execution records for unsupported `UBA*` rows so real
    runs keep them auditable in the root and per-taxon manifests
  - updated failure-row building so shared metadata and shared batch failures
    only collapse over supported rows, while unsupported `UBA*` rows stay
    accession-specific
  - added regression coverage for mixed dry-runs, UBA-only dry-runs, mixed
    real runs, UBA-only real runs, legacy bundled-release `UBA*` detection,
    and the `g__UBA509` false-positive case
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_edge_contract.py`
  - `tests/test_release_resolver.py`
  - `tests/test_selection.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_edge_contract.py tests/test_selection.py tests/test_release_resolver.py tests/test_entrypoints.py`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - this runtime change extends the shipped manifest semantics with the new
    `unsupported_input` failure outcome for legacy `UBA*` accessions; the
    frozen development plan stays untouched, and the runtime-facing
    documentation is updated in a separate follow-up commit

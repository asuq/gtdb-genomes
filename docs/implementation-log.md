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

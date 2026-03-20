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

### Commit `93dc518` - `refactor(selection): streamline exact taxon selection`

- Implemented:
  - moved requested-taxon trimming into a dedicated
    `src/gtdb_genomes/taxon_normalisation.py` helper so the CLI and selection
    code share one exact normalisation rule
  - replaced the per-request full-table scan in `selection.py` with one
    tokenise, explode, and join pass that preserves request order and row order
    while avoiding repeated lineage scans
  - normalised slug-map construction with the same trimmed requested taxa used
    by `select_taxa()` so direct callers cannot select with surrounding
    whitespace and then fail during slug attachment
  - moved the `run_workflow` import inside `main()` so importing the CLI for
    parser-level or integration tests does not eagerly import the whole workflow
  - updated CLI and integration tests to patch the workflow entry point at its
    real import location after the lazy-import change
  - added a regression proving that trimmed species taxa still receive the
    correct slug after selection
- Why:
  - correctness: direct callers previously had a gap between selected taxa and
    slug-map keys when surrounding whitespace was present
  - performance and scalability: the previous selector scanned the full
    tokenised taxonomy once per requested taxon, which was unnecessary for
    larger request sets
  - maintainability and clarity: duplicated taxon trimming logic made it easier
    for future callers to drift away from the exact-match contract
  - workflow integration: the lazy CLI import keeps parser tests and thin CLI
    entry points from loading heavy workflow dependencies earlier than needed
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/selection.py`
  - `src/gtdb_genomes/taxon_normalisation.py`
  - `tests/test_cli.py`
  - `tests/test_cli_integration.py`
  - `tests/test_selection.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection.py tests/test_selection_real_fixtures.py tests/test_cli.py tests/test_cli_integration.py tests/test_download.py tests/test_layout.py tests/test_metadata.py tests/test_release_resolver.py tests/test_real_data_scripts.py`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest`
- Match to review goals:
  - correctness, performance or scalability, maintainability, clarity, and
    workflow integration
- Deviations:
  - none

### Commit `2672fba` - `fix(workflow): harden bundled data and subprocess failures`

- Implemented:
  - added shared subprocess timeout and message helpers in
    `src/gtdb_genomes/subprocess_utils.py`
  - updated download retries to treat timeouts as retryable transient failures
    and missing executables as immediate spawn failures, while preserving the
    existing retry accounting
  - updated metadata summary lookups with the same timeout or spawn handling
    and added an injectable runner so the failure paths can be unit-tested
  - updated archive extraction to convert timeout and spawn failures into
    `LayoutError` instead of uncaught subprocess exceptions
  - wrapped bundled taxonomy parsing errors so malformed readable files raise
    `BundledDataError` and therefore return the documented CLI exit code `3`
  - widened the workflow bundled-data guard so taxonomy-load failures are
    handled consistently with manifest and path-validation failures
  - changed the remote real-data smoke check from a Python import probe to a
    `gtdb-genomes --dry-run` invocation, which validates the installed console
    entry point and bundled data without depending on whichever `python` binary
    appears first on `PATH`
  - added regressions for timeout, spawn-error, malformed-taxonomy, and remote
    smoke-check behaviour
- Why:
  - robustness: uncaught `TimeoutExpired`, `OSError`, and low-level Polars
    parsing failures would otherwise surface as raw tracebacks or inconsistent
    failure modes
  - reproducibility: the remote smoke check should validate the same installed
    command that users run, not an arbitrary interpreter selected by shell
    environment state
  - portability: explicit spawn-error handling is clearer on systems where
    `datasets` or `unzip` may be missing or differently installed
  - security and workflow integration: bounded subprocess execution is safer
    operationally than waiting indefinitely for external tools
- Files:
  - `bin/run-real-data-tests-remote.sh`
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/layout.py`
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/subprocess_utils.py`
  - `src/gtdb_genomes/taxonomy.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_download.py`
  - `tests/test_layout.py`
  - `tests/test_metadata.py`
  - `tests/test_real_data_scripts.py`
  - `tests/test_release_resolver.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection.py tests/test_selection_real_fixtures.py tests/test_cli.py tests/test_cli_integration.py tests/test_download.py tests/test_layout.py tests/test_metadata.py tests/test_release_resolver.py tests/test_real_data_scripts.py`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest`
- Match to review goals:
  - correctness, reproducibility, robustness, portability, security, and
    workflow integration
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

### Commit `1ca6719` - `docs(readme): warn about legacy UBA accessions`

- Implemented:
  - added a visible README caution block explaining that legacy GTDB
    accessions starting with `UBA` are not supported by NCBI or by this tool
  - documented BioProject `PRJNA417962` as the informational follow-up for
    most skipped `UBA` genomes
  - extended the runtime-contract section to include the
    `download_failures.tsv.final_status` value `unsupported_input`
  - updated the runtime documentation test so the shipped README must continue
    to expose the new caution and contract value
- Files:
  - `README.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the frozen development plan was left untouched, so the README now carries
    the user-facing explanation for the added unsupported-UBA runtime
    behaviour

### Commit `7d0a130` - `docs(testing): add release-variant validation guide`

- Implemented:
  - added a dedicated real-data validation guide covering the local dry-run
    sweep, local real runs, and remote packaged-runtime checks across the
    bundled release families
  - documented the confirmed real-data anchors for legacy releases,
    modern `ar122` releases, and `ar53` releases so the matrix uses real
    bundled taxonomy content rather than guessed taxa
  - added the new validation guide to the README document index
  - extended the entrypoint documentation test so the README must continue to
    expose the real-data validation guide link
- Files:
  - `docs/real-data-validation.md`
  - `README.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_entrypoints.py`
- Match to frozen plan:
  - yes
- Deviations:
  - case outputs still live at `/tmp/gtdb-realtests/.../<case-id>` as planned,
    but captured evidence is written under a sibling `_evidence/` tree so the
    case output directories remain valid empty targets before each run

### Commit `b197142` - `chore(bin): add real-data validation runners`

- Implemented:
  - added a shared bash helper library for real-data validation runs, covering
    evidence capture, exit-code checks, root TSV collection, simple manifest
    assertions, and suite-level result summaries
  - added a local runner for the release-coverage dry-run sweep plus the local
    real-download matrix, using `uv run gtdb-genomes`
  - added a remote runner for packaged-runtime validation, including the
    installed-command smoke checks and the optional large stress case gate via
    `RUN_OPTIONAL_LARGE=1`
  - automated the most important acceptance checks directly in bash:
    expected exit codes, output presence or absence, `unsupported_input`
    legacy failures, duplicate-across-taxa evidence, and the dehydrate method
    outcome for the heavy auto cases
- Files:
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
- Checks run:
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh`
- Match to frozen plan:
  - yes
- Deviations:
  - the runners automate core acceptance checks but still preserve the full
    TSV evidence for manual review, instead of trying to encode every large
    scientific validation judgement into shell-only assertions

### Commit `b390c89` - `fix(bin): stabilise local real-data runner`

- Implemented:
  - changed the local runner to launch the tool through
    `uv run --no-sync gtdb-genomes`, with `UV_CACHE_DIR` defaulting to
    `/tmp/gtdb_uv_cache`, so the runner no longer triggers a networked build
    of the project before the real-data cases start
  - made the local runner resolve the repository root and `cd` into it before
    launching any case, so `uv` and the optional module fallback both run
    against the checked-out project reliably
  - added `LOCAL_LAUNCHER_MODE=module` support so the local matrix can still
    be run from the prepared `.venv` if `uv` itself is not the preferred
    launcher for a given debugging session
  - replaced the global `datasets` and `unzip` preflight with case-aware
    checks: offline dry-runs now require only the local launcher, `A6`
    requires `datasets`, and the real-download `B*` cases require both
    `datasets` and `unzip`
  - removed the default `--debug` flag from `A6` so the runner does not
    capture upstream `datasets` debug output that can include the raw
    `Api-Key` header in evidence logs
- Files:
  - `bin/run-real-data-tests-local.sh`
- Checks run:
  - `bash -n bin/run-real-data-tests-local.sh bin/real-data-test-common.sh`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-offline-a1 bin/run-real-data-tests-local.sh A1`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-offline-a8 bin/run-real-data-tests-local.sh A8`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the original runner design assumed all local cases shared the same tool
    requirements and could safely use plain `uv run`; real debugging showed
    that this was false in three ways: offline dry-runs were blocked
    unnecessarily, `uv` cache initialisation failed in the sandboxed home
    directory, and plain `uv run` retried `hatchling` resolution over the
    network even when the local environment was already prepared

### Commit `c36936a` - `docs(testing): clarify local validation prerequisites`

- Implemented:
  - updated the real-data validation guide to document the actual local
    launch path, including the default `uv run --no-sync` command and the
    optional module fallback for prepared local environments
  - documented the case-family-specific local command requirements so offline
    dry-runs are clearly separated from preview and real-download cases
  - documented that `A6` and all `B*` cases need outbound DNS and network
    access to `api.ncbi.nlm.nih.gov`, and that DNS or connection failures in
    those cases should be treated as external environment problems
  - recorded why the default `A6` runner no longer uses `--debug`
  - added a documentation test so the local validation guide must continue to
    describe the runner environment split accurately
- Files:
  - `docs/real-data-validation.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_entrypoints.py`
  - `bash -n bin/run-real-data-tests-local.sh bin/real-data-test-common.sh`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-offline-a2 bin/run-real-data-tests-local.sh A2`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the original guide implied that `datasets` and `unzip` were uniform local
    prerequisites, but live debugging showed that this overstated the real
    requirements and hid the difference between valid offline dry-runs and
    genuinely networked local cases

### Commit `674e1f0` - `fix(download): use input files for auto previews`

- Implemented:
  - changed the auto-preview command path to use
    `datasets download genome accession --inputfile ... --preview` instead of
    expanding every accession directly into argv
  - kept the preview temp file outside the final output tree by writing it to
    a temporary directory under `/tmp`, while preserving the existing exit `5`
    and no-output-tree behaviour when preview fails
  - added a workflow-level regression test proving that auto preview now uses
    a temporary accession input file, deduplicates repeated accessions in
    deterministic order, and cleans up the temp file after the preview call
  - extended preview-size parsing to accept the JSON output returned by modern
    `datasets --preview`, including `estimated_file_size_mb` and nested
    `included_data_files.*.size_mb` values
  - updated the preview command unit expectations so the input-file preview
    shape is now part of the locked test contract
- Files:
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_download.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_download.py tests/test_edge_contract.py`
  - `.venv/bin/pytest -q tests/test_download.py tests/test_edge_contract.py tests/test_entrypoints.py`
  - `python3 -m compileall src`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-a6-netcheck-20260319-fix2 bin/run-real-data-tests-local.sh A6`
- Match to frozen plan:
  - no, by design
- Deviations:
  - fixing the `431` issue exposed a second live bug immediately afterwards:
    large input-file previews now succeeded, but the tool still exited `5`
    because it only knew how to parse text previews with units like `GB`; the
    real `datasets` response for the large `A6` request was JSON, so the fix
    had to cover both the command shape and the preview parser in the same
    implementation pass

### Commit `3efb568` - `fix(bin): harden local validation helpers`

- Implemented:
  - replaced the `awk` loop variable name `index` in the shared real-data test
    helper with a BSD-awk-safe identifier so the runner can validate TSV
    column lookups on macOS
  - relaxed the duplicate-across-taxa manifest matcher to accept line endings
    with an optional trailing carriage return, which made the `B3` validation
    case robust against the emitted TSV newline format
- Files:
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-local.sh`
- Checks run:
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-b4-netcheck-20260319-fix bin/run-real-data-tests-local.sh B4`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-b3-netcheck-20260319-fix bin/run-real-data-tests-local.sh B3`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-b1-b3-netcheck-20260319 bin/run-real-data-tests-local.sh B1 B3`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the original implementation plan was focused on the `A6` preview path, but
    once that was fixed the live local runner exposed two macOS-specific
    validation issues in the bash helper layer; these were fixed in a follow-on
    commit so the planned `B4`, `B1`, and `B3` validation sequence could
    complete without manual TSV inspection

### Commit `7bb1577` - `fix(bin): align real-data runner expectations`

- Implemented:
  - changed the local and remote direct-success checks so they now treat
    `run_summary.tsv` as the source of truth for a successful direct case,
    requiring a positive `successful_accessions` count and `failed_accessions=0`
    instead of incorrectly demanding that `download_failures.tsv` be header-only
  - corrected the `B2` local real-data case expectation to exit `6` and use the
    legacy-mixed validation path, because the bundled `86 / g__Methanobrevibacter`
    dataset includes one unsupported `UBA*` accession alongside supported
    genomes
  - reran the full local matrix in the fresh `gtdb-genome-netcheck`
    environment and confirmed that the adjusted runner now matches the live
    behaviour for every mandatory local case
- Files:
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
- Checks run:
  - `bash -n bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh bin/real-data-test-common.sh`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-full-netcheck-20260319-rerun bin/run-real-data-tests-local.sh`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the original runner assumptions were stricter than the implemented runtime
    contract in two places: a successful direct run can still carry retry
    history rows, and the live `B2` case is a partial-success mixed legacy run
    rather than a clean success

### Commit `04472d1` - `docs(testing): align real-data guide with live cases`

- Implemented:
  - updated the real-data validation guide so the local acceptance criteria now
    reflect the observed bundled data for `B2`, documenting that the
    `86 / g__Methanobrevibacter` case exits `6` because one legacy `UBA*`
    accession is skipped while the supported genomes still succeed
  - documented that successful direct cases may retain retry-history rows in
    `download_failures.tsv`, so the real pass/fail gate for both local and
    remote direct runs is the shell exit code plus `run_summary.tsv`, not a
    header-only failure TSV
  - kept the remote packaged-runtime guidance aligned with the same rule so the
    remote helper and manual review guidance both interpret successful direct
    runs consistently
- Files:
  - `docs/real-data-validation.md`
- Checks run:
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_TEST_ROOT=/tmp/gtdb-realtests/local-full-netcheck-20260319-rerun bin/run-real-data-tests-local.sh`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the live full local rerun showed that the original guide was still
    encoding earlier assumptions instead of the shipped runtime behaviour, so
    the documentation had to be brought into line with the evidence before the
    remote packaged-runtime pass

### Commit `6062f25` - `chore(data): gzip bundled GTDB taxonomy payloads`

- Implemented:
  - recompressed every bundled GTDB taxonomy payload under
    `data/gtdb_taxonomy/<release>/` from plain `.tsv` to `.tsv.gz` using
    maximum gzip compression
  - updated the bundled release manifest so each release row now points to the
    compressed taxonomy filenames while leaving `releases.tsv` itself as plain
    text for easy inspection and manifest debugging
  - reduced the tracked bundled-data footprint from roughly `410M` to roughly
    `28M` without changing the logical release coverage
- Files:
  - `data/gtdb_taxonomy/releases.tsv`
  - `data/gtdb_taxonomy/80.0/`
  - `data/gtdb_taxonomy/83.0/`
  - `data/gtdb_taxonomy/86.0/`
  - `data/gtdb_taxonomy/89.0/`
  - `data/gtdb_taxonomy/95.0/`
  - `data/gtdb_taxonomy/202.0/`
  - `data/gtdb_taxonomy/207.0/`
  - `data/gtdb_taxonomy/214.0/`
  - `data/gtdb_taxonomy/220.0/`
  - `data/gtdb_taxonomy/226.0/`
- Checks run:
  - `du -sh data/gtdb_taxonomy`
  - `find data/gtdb_taxonomy -maxdepth 2 -type f | sort | sed -n '1,40p'`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the frozen plan assumed bundled taxonomy TSVs, but the tracked payload had
    become large enough that runtime-transparent compression was the more
    practical packaging shape

### Commit `bd76f14` - `feat(taxonomy): load gzipped bundled data`

- Implemented:
  - taught bundled taxonomy validation to read gzip text when the resolved
    taxonomy file ends with `.gz`, while still supporting plain files for
    temporary tests and future fallback use
  - kept manifest and output stability by stripping only the trailing `.gz`
    from `taxonomy_file`, so runtime tables still report logical filenames such
    as `bac120_taxonomy_r95.tsv`
  - added regression coverage for real bundled resolutions returning `.tsv.gz`
    paths and for loading temporary gzipped taxonomy tables while preserving
    logical `taxonomy_file` values and accession normalisation
- Files:
  - `src/gtdb_genomes/release_resolver.py`
  - `src/gtdb_genomes/taxonomy.py`
  - `tests/test_release_resolver.py`
- Checks run:
  - `.venv/bin/python - <<'PY' ... pl.read_csv(Path('data/gtdb_taxonomy/95.0/bac120_taxonomy_r95.tsv.gz')) ... PY`
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the runtime contract deliberately stayed stable at the manifest/output
    level, so only the storage layer changed; the user-visible `taxonomy_file`
    values remain `.tsv` names even though the bundled payload is compressed

### Commit `9886875` - `chore(package): add MIT licence metadata`

- Implemented:
  - added a root MIT `LICENSE`
  - added a root `NOTICE` explaining that the MIT licence covers the project
    code and packaging while the bundled GTDB taxonomy payload remains subject
    to upstream terms and attribution requirements
  - updated `pyproject.toml` to declare MIT and include both licence files in
    package builds
  - updated the Bioconda recipe template to advertise `MIT` and install the
    project `LICENSE` as the recipe licence file
- Files:
  - `LICENSE`
  - `NOTICE`
  - `pyproject.toml`
  - `packaging/bioconda/meta.yaml`
- Checks run:
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin UV_CACHE_DIR=/tmp/gtdb_uv_cache uv build`
  - `.venv/bin/python - <<'PY' ... zipfile.ZipFile(Path('dist/gtdb_genomes-0.1.0-py3-none-any.whl')) ... PY`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the code licence could be made explicit immediately, but the bundled GTDB
    data was intentionally not relicensed; a separate notice was added instead
    of implying that the bundled taxonomy payload itself had become MIT

### Commit `6c4fa47` - `docs(readme): document gzipped bundled data`

- Implemented:
  - updated the README bundled-data section to explain that taxonomy payloads
    now ship as `.tsv.gz`, are decompressed transparently at read time, and
    still rely on a plain-text `releases.tsv` manifest
  - documented the code-vs-data licence split in the README and pointed readers
    at `NOTICE` for the bundled-data warning
  - tightened the runtime docs test so the documentation contract now asserts
    the presence of the gzip layout, the plain-text manifest note, the bundled
    data notice, and the Bioconda MIT metadata
- Files:
  - `README.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q`
  - `PYTHONPATH=src .venv/bin/python -m gtdb_genomes --release 95 --taxon 's__Thermoflexus hugenholtzii' --output /tmp/gtdb-gzip-dry-run --download-method direct --no-prefer-genbank --dry-run`
  - `test ! -e /tmp/gtdb-gzip-dry-run && echo absent`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the documentation now needs to describe the storage-optimised bundled-data
    layout and the explicit licensing split, both of which are implementation
    clarifications beyond the frozen development plan

### Commit `5937a4f` - `refactor(cli): rename api key option to ncbi-api-key`

- Implemented:
  - renamed the public CLI option from `--api-key` to `--ncbi-api-key` with no
    compatibility alias, so older command lines now fail fast instead of being
    silently accepted
  - renamed the normalised CLI field and the internal Python plumbing from
    generic `api_key` names to `ncbi_api_key`, covering the CLI dataclass,
    workflow calls, metadata lookup helpers, preview helpers, direct download
    helpers, and dehydrate helpers
  - kept the external `datasets` subprocess interface unchanged by continuing to
    forward the value as the upstream `datasets --api-key` flag
  - added parser regression coverage proving that `--ncbi-api-key` parses
    correctly and that the removed legacy `--api-key` flag is rejected
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_cli.py`
  - `tests/test_cli_integration.py`
  - `tests/test_download.py`
  - `tests/test_edge_contract.py`
  - `tests/test_metadata.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_download.py tests/test_entrypoints.py tests/test_edge_contract.py`
  - `.venv/bin/pytest -q`
  - `PYTHONPATH=src .venv/bin/python -m gtdb_genomes --help | sed -n '1,80p'`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the internal naming was tightened as well as the public flag so the code
    and docs no longer mix a generic `api_key` label with an NCBI-specific
    command-line interface

### Commit `c7a9d7e` - `docs(readme): rename api key flag references`

- Implemented:
  - updated the README option list, example command, and API-key handling
    section to use `--ncbi-api-key`
  - clarified in the README that `--ncbi-api-key` expects an NCBI API key and
    is used only for the upstream `datasets` command, not for GTDB resolution
    or local taxonomy loading
  - updated the real-data validation guide and local/remote runner scripts so
    the runner-generated commands also use `--ncbi-api-key`
  - renamed the shared runner helper to `real_data_require_ncbi_api_key`
  - extended the README/docs test to lock in the new public flag name and the
    explicit NCBI-only usage wording
- Files:
  - `README.md`
  - `docs/real-data-validation.md`
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
  - `tests/test_entrypoints.py`
- Checks run:
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh`
  - `.venv/bin/pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_download.py tests/test_entrypoints.py tests/test_edge_contract.py`
  - `.venv/bin/pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now also renames the non-existent environment-flag example from
    `--api-key-env` to `--ncbi-api-key-env` so the documentation stays aligned
    with the renamed public flag instead of preserving an obsolete prefix

### Commit `a69a12a` - `docs(readme): expand datasets usage guidance`

- Implemented:
  - added top-of-file README badges for Python `>=3.12`, the latest GitHub
    release for `asuq/gtdb-genome`, and the MIT licence
  - rewrote the opening description so it now states more plainly that GTDB
    taxonomy is bundled locally while NCBI metadata and download operations are
    delegated to the NCBI `datasets` CLI
  - replaced the plain blockquote `> Caution` with GitHub's rendered alert
    syntax `> [!CAUTION]` so the existing legacy `UBA*` warning is highlighted
    correctly in the repository README
  - added a dedicated `NCBI datasets CLI` section that links to
    `https://github.com/ncbi/datasets` and explains the concrete `datasets`
    command families used by the tool for summary lookup, preview, direct
    download, batch dehydrate, and rehydrate
  - tightened the API-key wording so the README now says explicitly that
    `--ncbi-api-key` expects an NCBI API key and that the key is used only for
    the upstream `datasets` command, not for GTDB release resolution or local
    taxonomy loading
  - extended the README/docs assertions so the badge references, upstream
    `datasets` link, NCBI-only API-key wording, and GitHub alert syntax remain
    covered by tests
- Files:
  - `README.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_entrypoints.py`
  - `.venv/bin/pytest -q`
  - `sed -n '1,70p' README.md`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the README now depends on GitHub-oriented badge and alert syntax for better
    rendering in the repository view, which is a presentation-level
    improvement rather than a runtime behaviour change

### Commit `b21dac4` - `feat(ci): add pytest and offline validation workflow`

- Implemented:
  - added the first GitHub Actions workflow at `.github/workflows/ci.yml`
  - configured a standard `pytest` matrix for `ubuntu-latest` and
    `macos-latest` on Python `3.12`
  - added a deterministic Ubuntu-only offline validation job that reuses the
    existing local validation runner in `LOCAL_LAUNCHER_MODE=module`
  - locked the offline validation subset to `A1 A2 A8`, covering the legacy
    `UBA*` warning path, a clean bundled-data dry-run, and the release-alias
    path form without depending on `datasets`, `unzip`, NCBI network access,
    or secrets
  - enabled per-workflow per-ref concurrency cancellation and artifact upload
    for the offline validation evidence tree
- Files:
  - `.github/workflows/ci.yml`
- Checks run:
  - `ruby -e 'require "yaml"; YAML.load_file(".github/workflows/ci.yml"); YAML.load_file(".github/workflows/live-validation.yml"); puts "yaml-ok"'`
  - `.venv/bin/pytest -q`
  - `LOCAL_LAUNCHER_MODE=module LOCAL_TEST_ROOT=/tmp/gtdb-realtests/ci-offline-local bin/run-real-data-tests-local.sh A1 A2 A8`
  - `sed -n '1,20p' /tmp/gtdb-realtests/ci-offline-local/_evidence/case-results.tsv`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the repository previously had no `.github/workflows` directory at all, so
    the CI addition starts from a minimal deterministic baseline rather than
    trying to fold these jobs into a pre-existing workflow layout

### Commit `e992ec8` - `feat(ci): add gated live validation workflow`

- Implemented:
  - added `.github/workflows/live-validation.yml` for one real downloader case
  - gated the live workflow to `workflow_dispatch` and pushes to `main`, so
    the live NCBI-backed check stays out of normal PR CI
  - used `mamba-org/setup-micromamba@v2` to provision an Ubuntu environment
    with `python=3.12`, `uv`, `ncbi-datasets-cli`, and `unzip`
  - reused the existing local validation runner in module mode for the live
    case, choosing `B1` as the smallest real end-to-end genome download path
    that does not require `NCBI_API_KEY`
  - enabled evidence artifact upload for the live validation tree as well
- Files:
  - `.github/workflows/live-validation.yml`
- Checks run:
  - `ruby -e 'require "yaml"; YAML.load_file(".github/workflows/ci.yml"); YAML.load_file(".github/workflows/live-validation.yml"); puts "yaml-ok"'`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome-netcheck/bin:/usr/bin:/bin LOCAL_LAUNCHER_MODE=module LOCAL_TEST_ROOT=/tmp/gtdb-realtests/ci-live-local bin/run-real-data-tests-local.sh B1`
  - `sed -n '1,20p' /tmp/gtdb-realtests/ci-live-local/_evidence/case-results.tsv`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the live workflow intentionally does not use an NCBI API-key secret, so it
    validates a smaller non-secret case rather than one of the metadata-heavy
    `--prefer-genbank` scenarios

### Commit `4c5d427` - `chore(ci): bump actions for node24 support`

- Implemented:
  - updated the GitHub Actions workflow pins for the actions that emitted the
    Node.js 20 deprecation warning in CI
  - bumped `actions/checkout` from `v4` to `v5` in both workflows
  - bumped `actions/setup-python` from `v5` to `v6` in the main CI workflow
  - bumped `astral-sh/setup-uv` from `v6` to `v7` in the main CI workflow
  - bumped `actions/upload-artifact` from `v4` to `v6` in both workflows
- Files:
  - `.github/workflows/ci.yml`
  - `.github/workflows/live-validation.yml`
- Checks run:
  - `ruby -e 'require "yaml"; YAML.load_file(".github/workflows/ci.yml"); YAML.load_file(".github/workflows/live-validation.yml"); puts "yaml-ok"'`
  - `git diff -- .github/workflows/ci.yml .github/workflows/live-validation.yml`
- Match to frozen plan:
  - no, by design
- Deviations:
  - `mamba-org/setup-micromamba@v2` was left unchanged because the warning the
    user reported only covered the four updated actions, and this pass was
    scoped to clearing those concrete deprecation notices first

### Commit `25937f7` - `fix(metadata): use input files for summary lookups`

- Implemented:
  - converted metadata lookup from positional `datasets summary genome accession
    <many accessions>` calls to the scale-safe `--inputfile` form
  - moved workflow metadata planning onto one shared deterministic ordered
    accession list, so the summary input file is written once and reused for
    the actual lookup call
  - reused the same ordered-accession helper for preview input-file writing and
    direct download command construction, reducing duplicate de-duplication
    logic across the hot path
  - added regression coverage for the new summary command shape and for the
    temporary metadata input file lifecycle in dry-run workflow execution
- Files:
  - `src/gtdb_genomes/download.py`
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_metadata.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_metadata.py tests/test_edge_contract.py`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the temporary metadata accession file is created in workflow under `/tmp`
    rather than inside the metadata module, because workflow already owns the
    surrounding dry-run planning lifecycle and cleanup behaviour

### Commit `ff690ab` - `fix(download): deduplicate shared direct accession fetches`

- Implemented:
  - replaced the per-plan direct download loop with a preferred-accession group
    executor, so direct mode downloads one shared preferred accession once even
    when multiple original accessions converge onto it
  - preserved per-original manifest semantics by materialising one execution
    row per original accession after the shared preferred download succeeds
  - preserved per-original fallback semantics by running fallback downloads only
    for grouped originals whose original accession differs from the failed
    preferred accession
  - added regression tests for the real release `80.0` duplicate pair
    `GCF_001881595.2` and `GCA_001881595.3`, covering both the shared-success
    path and the split fallback path
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/pytest -q tests/test_edge_contract.py tests/test_metadata.py`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - direct-mode retry-history rows remain attached to the per-original
    executions that consumed the shared preferred or fallback result, instead
    of being lifted into a new shared-failure schema

### Commit `9c0eee0` - `fix(testing): redact runner evidence secrets`

- Implemented:
  - redacted `--ncbi-api-key` values in the runner-generated `command.sh`
    evidence files
  - changed the runner helpers to capture raw stdout and stderr into temporary
    files outside the evidence tree, redact the NCBI API key before writing the
    final evidence logs, and only then build the combined log
  - added lightweight `python` and `datasets` version capture to
    `_evidence/tool-versions.txt` for both local and remote validation suites
  - added focused bash-helper tests for command redaction, log redaction, and
    version-file capture
- Files:
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh`
  - `.venv/bin/pytest -q tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to frozen plan:
  - no, by design
- Deviations:
  - redaction is intentionally limited to the configured `NCBI_API_KEY` value
    rather than attempting generic header-pattern scrubbing, so the helper
    remains portable across GNU and BSD userlands without depending on extra
    tooling

### Commit `b87afaa` - `refactor(data): streamline taxonomy selection hot paths`

- Implemented:
  - dropped the transient `lineage_tokens` column before selection results
    leave `selection.py`, so downstream workflow stages no longer carry an
    unused list column
  - replaced the Python callback-based GTDB accession normalisation in
    `taxonomy.py` with vectorised Polars string expressions for `RS_` and
    `GB_` prefix stripping
  - added a small regression assertion that selected frames no longer expose
    `lineage_tokens`
- Files:
  - `src/gtdb_genomes/selection.py`
  - `src/gtdb_genomes/taxonomy.py`
  - `tests/test_selection.py`
- Checks run:
  - `.venv/bin/pytest -q`
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `1faa747` - `refactor(workflow): reuse shared ordered accession helper`

- Implemented:
  - removed the local `get_ordered_unique_values()` helper from `workflow.py`
    and reused the shared `get_ordered_unique_accessions()` helper from
    `download.py`
  - kept the unsupported-`UBA*` warning text and ordering semantics unchanged
    while reducing duplicate ordered-deduplication logic in the runtime path
- Why:
  - the ordering rule for accession lists was duplicated in two places after
    the earlier preview and metadata refactors
  - centralising it in one shared helper makes future changes to deterministic
    accession ordering less error-prone and keeps the workflow module smaller
- Files:
  - `src/gtdb_genomes/workflow.py`
- Checks run:
  - `.venv/bin/python -m pytest -q`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `1d36dd2` - `refactor(cli): rename GTDB selection options`

- Implemented:
  - renamed the public CLI surface from `--release`, `--taxon`, and `--output`
    to `--gtdb-release`, `--gtdb-taxon`, and `--outdir`
  - removed `--no-prefer-genbank` and changed `--prefer-genbank` to an opt-in
    flag with default `false`
  - renamed the normalised `CliArgs` fields to `gtdb_release`,
    `gtdb_taxa`, and `outdir`, then updated the workflow to use those names
  - updated the real-data runner helpers and runner suites so they invoke the
    renamed CLI without the removed GenBank-negation flag
  - refreshed parser, integration, contract, and bash-helper tests to reject
    the removed legacy flags and assert the new default preference behaviour
- Why:
  - the old public option names were generic and inconsistent with the tool's
    GTDB-specific interface
  - switching GenBank preference to explicit opt-in makes bundled-data-only
    direct and dry-run use the default path instead of requiring users to
    negate a default-on preference
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/workflow.py`
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
  - `tests/test_cli.py`
  - `tests/test_cli_integration.py`
  - `tests/test_edge_contract.py`
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_edge_contract.py tests/test_real_data_scripts.py`
  - `bash -n bin/real-data-test-common.sh bin/run-real-data-tests-local.sh bin/run-real-data-tests-remote.sh`
  - `python3 -m compileall src`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `198f2d9` - `docs: split usage details from readme`

- Implemented:
  - rewrote the README as a shorter landing page focused on installation,
    renamed CLI examples, workflow overview, and links to supporting documents
  - added `docs/usage-details.md` as the single detailed reference for CLI
    option behaviour, output layout, summary files, NCBI `datasets`, retry
    policy, runtime contract, bundled taxonomy rules, and failure handling
  - moved the output-layout diagram into `docs/usage-details.md` so the README
    no longer carries the full operational specification
  - updated the entrypoint documentation test to assert the README/doc split and
    the renamed flag surface
  - refreshed `docs/pipeline-concept.md` so its still-relevant CLI examples use
    `--gtdb-taxon` and `--outdir`
- Why:
  - the previous README update had turned the README into both a landing page
    and a full reference manual
  - separating the detailed operational material into `usage-details.md` keeps
    the README readable while preserving one authoritative detailed document
- Files:
  - `README.md`
  - `docs/usage-details.md`
  - `docs/pipeline-concept.md`
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q`
- Match to frozen plan:
  - no, by design
- Deviations:
  - the concise README still retains a short workflow summary and the NCBI API
    key alert so the landing page remains self-contained for first-time users

### Commit `3c3712a` - `test(docs): align readme contract assertions`

- Implemented:
  - updated the README contract test to match the current post-reset README
    shape without editing the README itself
  - renamed the test so it no longer claims the README must stay slimmer than
    the current repository state
  - changed the assertions to expect `Output Layout` and `Summary Files` in the
    README while continuing to require `Runtime Contract`, `Retry Policy`, and
    `NCBI datasets CLI` in `docs/usage-details.md`
  - changed the alert assertion to accept the current README `> [!NOTE]`
    block for the legacy `UBA*` warning
- Why:
  - the reset restored commit `6264dfb`, which changed the README content back
    to an older, more detailed form
  - the user explicitly asked to keep the README unchanged in this state, so
    the correct fix was to realign the enforced docs contract rather than edit
    the README again
  - this resolved the only failing test without changing runtime behaviour
- Files:
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q`
  - `shasum -a 256 README.md`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `8911a3b` - `fix(workflow): defer tool preflight until selection`

- Implemented:
  - removed the unconditional external-tool preflight from `cli.main()` and
    moved the requirement check into the workflow after GTDB release loading
    and taxonomy selection
  - extended preflight tool resolution so zero-match runs and
    unsupported-`UBA*`-only runs require no external tools
  - kept `PreflightError` as the public CLI error path so missing supported-run
    tools still return exit code `5` with the normal command-line error output
  - updated CLI and edge-contract tests to assert the new workflow-aware
    preflight boundary, including zero-match and supported-path regressions
- Why:
  - the earlier audit reproduced a real bug where a zero-match query failed
    with missing `datasets` instead of taking the documented exit-`4` path
  - tool checks should depend on whether the selected rows actually require NCBI
    or archive work, not only on the raw CLI flags
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/preflight.py`
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_cli.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_edge_contract.py`
  - `PATH=/usr/bin:/bin .venv/bin/python -m gtdb_genomes --gtdb-release 95 --gtdb-taxon g__DefinitelyNotReal --outdir /tmp/gtdb-zero-match-check-plan`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `be9be16` - `fix(download): tighten preview size parsing`

- Implemented:
  - changed preview size parsing to prefer labelled `Package size:` or
    `Download size:` text instead of taking the largest arbitrary size token
  - changed JSON preview parsing to prefer `estimated_file_size_mb` and fall
    back to the summed `included_data_files[*].size_mb` total only when the
    estimate is absent
  - made ambiguous multi-size plain-text preview output return `None` so
    `select_download_method()` fails explicitly instead of silently switching to
    the wrong mode
  - removed the unused `AccessionDownloadResult` dataclass and the dead
    `download_with_accession_fallback()` helper, which runtime code no longer
    called
  - updated the download tests to cover the `Download size` versus
    `Uncompressed size` regression and the new JSON fallback rule
- Why:
  - the audit reproduced a real `auto`-mode bug where a small package preview
    was forced into `dehydrate` by a larger unrelated `Uncompressed size`
    token
  - the dead direct-fallback helper had become stale after the grouped workflow
    refactor and was no longer part of the live runtime
- Files:
  - `src/gtdb_genomes/download.py`
  - `tests/test_download.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_download.py`
  - `PYTHONPATH=src python3 - <<'PY' ... select_download_method(\"auto\", 5, preview_text=\"Download size: 1.0 GB\\nUncompressed size: 16.0 GB\\n\") ... PY`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `8a1dc0d` - `fix(workflow): correct layout and batch attribution`

- Implemented:
  - introduced explicit `download_batch` tracking on `AccessionExecution` so
    manifests now report the actual preferred, fallback, or dehydrated batch
    unit that was attempted
  - changed local unzip and payload-discovery failures from `stage=preflight`
    to `stage=layout`
  - updated direct grouped execution so shared-preferred successes report the
    shared preferred accession as the batch, while fallback rows report the
    original accession that was actually downloaded
  - changed successful dehydrated executions to record `dehydrated_batch`
    directly on the execution rather than reconstructing it during manifest
    writing
  - refactored taxon selection into a single explode/join pass that preserves
    requested-taxon order and per-taxon row order without rescanning the full
    frame once per taxon
  - refreshed the edge-contract, CLI-integration, and selection tests, and
    updated `docs/usage-details.md` to describe deferred tool requirements and
    the new `layout` failure stage
- Why:
  - the audit found incorrect failure attribution for local archive problems and
    incorrect `download_batch` values for shared-preferred direct downloads
  - the previous selector shape performed one full-frame filter per requested
    taxon and created avoidable intermediate frames on large releases
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `src/gtdb_genomes/selection.py`
  - `docs/usage-details.md`
  - `tests/test_edge_contract.py`
  - `tests/test_selection.py`
  - `tests/test_cli_integration.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py tests/test_selection.py tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q`
  - `python3 -m compileall src`
  - `PATH=/usr/bin:/bin .venv/bin/python -m gtdb_genomes --gtdb-release 95 --gtdb-taxon g__DefinitelyNotReal --outdir /tmp/gtdb-zero-match-check-final`
- Match to frozen plan:
  - no, by design
- Deviations:
  - none

### Commit `30582d6` - `test(fixtures): add GTDB export taxon fixtures`

- Implemented:
  - moved the four GTDB export CSVs from the repo root into
    `tests/fixtures/gtdb_taxon_exports/` so they are test-only fixtures rather
    than ambiguous top-level repo files
  - removed accession `GCF_900143255.1` from
    `g__Frigididesulfovibrio.csv` because its bundled GTDB lineage belongs to
    `g__Frigididesulfovibrio_A` and should not be treated as an exact genus
    membership expectation for `g__Frigididesulfovibrio`
  - kept the fixture filenames unchanged so each file stem remains the source
    of truth for the requested GTDB taxon
- Why:
  - the user supplied these GTDB exports as ground-truth evidence for testing
    taxon membership against the bundled release data
  - placing them under `tests/fixtures/` keeps runtime packaged data separate
    from test evidence and makes the intent unambiguous
- Files:
  - `tests/fixtures/gtdb_taxon_exports/g__Frigididesulfovibrio.csv`
  - `tests/fixtures/gtdb_taxon_exports/o__Altiarchaeales.csv`
  - `tests/fixtures/gtdb_taxon_exports/s__Altiarchaeum hamiconexum.csv`
  - `tests/fixtures/gtdb_taxon_exports/s__Frigididesulfovibrio sp031556355.csv`
- Checks run:
  - none separately; exercised in the subsequent fixture-selection pytest run
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `00cf840` - `test(selection): add GTDB export fixture coverage`

- Implemented:
  - added `tests/test_selection_real_fixtures.py` to drive selection checks
    from the real GTDB export fixtures against bundled release `226.0`
  - normalised fixture lineage spacing before comparison, validated that every
    fixture accession exists in the bundled taxonomy, and asserted that every
    fixture lineage contains the exact taxon token from the filename stem
  - added an accession-set comparison for `select_taxa()` so the selection
    behaviour is checked against the cleaned GTDB export expectations
  - cached the release `226.0` taxonomy load and accession-to-lineage mapping
    once per test session to keep the fixture-driven tests fast and stable
- Why:
  - the existing selection unit tests covered synthetic cases only and did not
    verify exact membership against real GTDB export tables
  - this locks the bundled release `226.0` taxon selection behaviour to a
    small, deterministic, user-supplied set of real GTDB examples
- Files:
  - `tests/test_selection_real_fixtures.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection_real_fixtures.py`
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection.py tests/test_selection_real_fixtures.py`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `854951c` - `fix(selection): lock exact GTDB taxon matching`

- Implemented:
  - extracted the lineage-token expression in `selection.py` so the exact GTDB
    token-matching contract is explicit in one place
  - normalised requested taxa by trimming surrounding whitespace only, without
    changing internal whitespace or folding suffix variants
  - updated the selection docstring to state that matching is by exact GTDB
    lineage token
  - added selection regressions covering suffix-variant exclusion,
    suffix-variant exact matching when explicitly requested, preserved internal
    species whitespace, surrounding-whitespace trimming, incomplete species
    non-matching, and malformed double-space species non-matching
  - extended the real-fixture tests to assert that bundled release `226.0`
    excludes `GCF_900143255.1` from `g__Frigididesulfovibrio` and returns zero
    matches for incomplete species token `s__Altiarchaeum`
- Why:
  - the selector already behaved as an exact GTDB token matcher, but the code
    did not make that contract explicit and lacked direct regressions for
    suffix variants and species-whitespace edge cases
  - the user wanted a hard guarantee that incomplete species tokens do not
    fall back to any broader taxon match
- Files:
  - `src/gtdb_genomes/selection.py`
  - `tests/test_selection.py`
  - `tests/test_selection_real_fixtures.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection.py tests/test_selection_real_fixtures.py tests/test_cli.py tests/test_entrypoints.py`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `a9d9779` - `docs(cli): clarify exact GTDB taxon quoting`

- Implemented:
  - updated the README and detailed usage reference to say that
    `--gtdb-taxon` matches exact GTDB lineage tokens only after trimming
    surrounding whitespace
  - documented that suffix variants such as
    `g__Frigididesulfovibrio_A` are separate taxa and are not retrieved when
    requesting `g__Frigididesulfovibrio`
  - documented that species taxa with spaces must be quoted in the shell and
    gave the explicit example `--gtdb-taxon "s__Altiarchaeum hamiconexum"`
  - documented that unquoted shell-split species input is invalid
  - updated the CLI help text to mention quoting for species taxa
  - added parser and entrypoint regressions to lock in the new help and
    documentation wording
- Why:
  - the previous docs said matching was exact but did not spell out the
    consequences for suffix variants or shell-quoted species names
  - users need a clear warning that the shell will split an unquoted species
    taxon before `gtdb-genomes` can interpret it
- Files:
  - `README.md`
  - `docs/usage-details.md`
  - `src/gtdb_genomes/cli.py`
  - `tests/test_cli.py`
  - `tests/test_entrypoints.py`
- Checks run:
  - `UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv run --group dev pytest tests/test_selection.py tests/test_selection_real_fixtures.py tests/test_cli.py tests/test_entrypoints.py`
- Match to frozen plan:
  - yes
- Deviations:
  - none

### Commit `7d70344` - `docs(validation): add remote packaged quickstart`

- Implemented:
  - expanded `docs/real-data-validation.md` with a new `Remote Server
    Quickstart` section for package-first validation on another server
  - documented the concrete local build and transfer path with `uv build`,
    `scp`, wheel installation through `python -m pip install`, and
    confirmation of `which gtdb-genomes`
  - documented the packaged-data sanity check equivalent to remote
    `C0-manifest`, including the expected exit code `4` for the deliberately
    missing taxon
  - split the remote guide into a minimum smoke-test path using `C6` then `C1`
    and a full packaged-runtime matrix path using
    `bin/run-real-data-tests-remote.sh`
  - documented the remote environment controls `REMOTE_TEST_ROOT`,
    `NCBI_API_KEY`, and `RUN_OPTIONAL_LARGE`
  - added a short expected-results section for `C6`, `C1`, `C4`, and `C5`
    and a short failure-evidence checklist covering `_evidence` outputs
  - updated the evidence tree example to include `_evidence/tool-versions.txt`
- Why:
  - the existing guide documented remote prerequisites and case expectations,
    but it did not give a practical step-by-step path for testing the packaged
    command on a separate clean server
  - the user specifically wanted to verify whether the installed command works
    on another server, so the docs now lead with the wheel-based runtime path
    rather than a source-checkout workflow
  - the quickstart makes the intended remote contract explicit: packaged wheel,
    no `uv` in the remote runtime path, smoke test first, then optional matrix
- Files:
  - `docs/real-data-validation.md`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `1517bb7` - `test(docs): cover remote validation quickstart`

- Implemented:
  - extended `tests/test_entrypoints.py` so the real-data guide must continue
    to mention `uv build`, wheel installation via `python -m pip install`, the
    absence of `uv` in the remote runtime path, `which gtdb-genomes`, the
    remote `C0-manifest` sanity check, `REMOTE_TEST_ROOT`, `case-results.tsv`,
    and `tool-versions.txt`
- Why:
  - the new remote quickstart is user-facing operational guidance, so it
    should be protected by an existing doc regression rather than relying on
    manual review
  - these assertions lock the guide to the intended packaged-runtime workflow
    and evidence contract without changing runtime code
- Files:
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `14f284c` - `fix(packaging): include runtime sources in sdist`

- Implemented:
  - updated the Hatch sdist include list in `pyproject.toml` so source
    distributions explicitly carry `src/gtdb_genomes/**` as well as the
    bundled GTDB taxonomy payload
  - kept the existing console entrypoint
    `gtdb-genomes = "gtdb_genomes.cli:main"` unchanged
  - kept the existing wheel package root `packages = ["src/gtdb_genomes"]`
    unchanged
- Why:
  - a user-reported packaged install failed with
    `ModuleNotFoundError: No module named 'gtdb_genomes.cli'` when running
    `gtdb-genomes --help`
  - inspection of `dist/gtdb_genomes-0.1.0.tar.gz` showed that the current
    sdist omitted `src/gtdb_genomes/cli.py` and the rest of the runtime
    package, and the wheel built from that sdist therefore shipped only
    bundled taxonomy data plus `.dist-info` metadata
  - because the wheel can be built from the sdist, fixing the sdist contents
    is the minimal packaging correction that restores installed runtime
    imports without changing the public CLI contract
- Files:
  - `pyproject.toml`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:/usr/bin:/bin UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv build`
  - `python3 - <<'PY' ... tarfile.open('dist/gtdb_genomes-0.1.0.tar.gz') ... 'gtdb_genomes-0.1.0/src/gtdb_genomes/cli.py' in names ... PY`
  - `python3 - <<'PY' ... zipfile.ZipFile('dist/gtdb_genomes-0.1.0-py3-none-any.whl') ... 'gtdb_genomes/cli.py' in names ... PY`
  - `python3 -m venv /tmp/gtdb_pkg_verify && /tmp/gtdb_pkg_verify/bin/python -m pip install dist/gtdb_genomes-0.1.0-py3-none-any.whl`
  - `/tmp/gtdb_pkg_verify/bin/gtdb-genomes --help`
  - `/tmp/gtdb_pkg_verify/bin/gtdb-genomes --gtdb-release 226 --gtdb-taxon g__DefinitelyNotReal --outdir /tmp/gtdb_pkg_verify_c0_manifest --dry-run`
- Match to requested plan:
  - yes
- Deviations:
  - verification needed unrestricted network access for `uv build` to resolve
    `hatchling` and for `pip install` in the clean temp environment to fetch
    `polars`

### Commit `a009e12` - `test(packaging): lock sdist source inclusion`

- Implemented:
  - added a packaging-contract regression in `tests/test_entrypoints.py`
  - the new test parses `pyproject.toml` and asserts that the wheel package
    root remains `src/gtdb_genomes`
  - the same test asserts that the sdist include list contains both
    `src/gtdb_genomes/**` and `data/gtdb_taxonomy/**`
- Why:
  - the previous test coverage proved source-checkout entrypoints and docs, but
    it did not guard the build configuration that decides whether the packaged
    command contains its runtime Python modules
  - locking this contract at the config level keeps the default test suite
    fast and offline while still protecting against a repeat of the missing
    `gtdb_genomes.cli` artefact bug
- Files:
  - `tests/test_entrypoints.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `03bd6e2` - `chore(workflow): add threaded direct debug tracing`

- Implemented:
  - added direct-download debug instrumentation in `src/gtdb_genomes/workflow.py`
    without changing the normal direct-download execution path
  - logged the resolved direct-download worker count before the thread pool
    starts
  - logged per-group start and completion lines for threaded direct downloads
  - logged the redacted direct `datasets download genome accession` command
    before each preferred or fallback direct download launch
  - logged archive extraction start, finish, and failure points for direct and
    fallback per-accession extraction
- Why:
  - the intermittent remote `C1` segfault only reproduced on the threaded
    direct-download path
  - `remote-smoke-c1` succeeded with `--threads 2`, `c1-serial` succeeded with
    `--threads 1`, and the failing `C1` runs created only the output
    directories before exiting `139`, so the next useful step was to expose the
    last threaded group activity in a `--debug` run rather than changing
    runtime behaviour
- Files:
  - `src/gtdb_genomes/workflow.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `daff30b` - `chore(validation): add investigation case helpers`

- Implemented:
  - added `REAL_DATA_PREPARED_COMMAND` handling to
    `bin/real-data-test-common.sh`
  - added helper logic to detect whether a case command carries
    `--ncbi-api-key`
  - added `real_data_prepare_case_command()` so investigation settings can
    alter case commands before they are recorded or executed
  - added `REAL_DATA_PYTHON_FAULTHANDLER=1` support by prefixing case commands
    with `env PYTHONFAULTHANDLER=1`
  - added `REAL_DATA_DEBUG_SAFE=1` support by appending `--debug` only to
    no-key cases
  - copied `OUTPUT/debug.log` into `_evidence/<case-id>/debug.log` when a real
    run writes one
- Why:
  - the remote `C1` failure needed a reproducible investigation mode with
    richer evidence, but debug mode remained unsafe for API-key cases because
    upstream `datasets` can emit raw headers
  - putting this logic in the shared helper keeps the recorded `command.sh`
    file and the executed command aligned
- Files:
  - `bin/real-data-test-common.sh`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `593909d` - `chore(validation): make C1 threads configurable`

- Implemented:
  - updated `bin/run-real-data-tests-remote.sh` so remote case `C1` now reads
    `REAL_DATA_C1_THREADS`, defaulting to `2`
- Why:
  - the collected evidence showed that `C1` succeeded serially with
    `--threads 1`, so the remote runner needed a narrow override for that one
    investigation target without changing any default CLI behaviour or editing
    the script on the remote machine
- Files:
  - `bin/run-real-data-tests-remote.sh`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `42a3821` - `test(validation): cover investigation runner controls`

- Implemented:
  - extended `tests/test_real_data_scripts.py` to cover investigation-mode
    command preparation and evidence capture
  - added a regression that records the prepared case command and asserts
    `env PYTHONFAULTHANDLER=1` plus `--debug` are written for safe no-key cases
  - added a regression that confirms `REAL_DATA_DEBUG_SAFE=1` does not add
    `--debug` when `--ncbi-api-key` is present
  - added a regression that confirms `real_data_record_output_evidence()`
    copies `debug.log`
  - updated the remote-runner text assertion to keep `REAL_DATA_C1_THREADS`
    present in the script
- Why:
  - the new investigation controls are all shell-level behaviour, so they need
    dedicated helper tests rather than source-text assumptions alone
- Files:
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `b72a6f6` - `test(workflow): cover threaded direct debug logs`

- Implemented:
  - added a workflow contract test in `tests/test_edge_contract.py` for a
    multi-group direct-download run with debug logging enabled
  - the new test asserts that threaded direct execution logs the worker count,
    per-group start markers, redacted command launch lines, archive extraction
    progress, and per-group completion markers
- Why:
  - the threaded instrumentation is useful only if it stays stable and
    predictable enough to diagnose the next intermittent `C1` crash
  - this keeps the logging contract under unit coverage without requiring a
    real segfault reproduction in CI
- Files:
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `7a05e30` - `docs(validation): document C1 investigation mode`

- Implemented:
  - updated `docs/real-data-validation.md` to document
    `REAL_DATA_C1_THREADS`, `REAL_DATA_PYTHON_FAULTHANDLER`, and
    `REAL_DATA_DEBUG_SAFE`
  - added a recommended threaded-then-serial `C1` repro sequence for remote
    investigation mode
  - documented that `_evidence/C1/debug.log` should be compared with stderr and
    `run_summary.tsv` when investigation mode is used
- Why:
  - the remote guide previously documented smoke tests and the main matrix, but
    not the debug-oriented rerun sequence needed for the intermittent `C1`
    segfault
- Files:
  - `docs/real-data-validation.md`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `4431f2c` - `docs(debug): capture C1 segfault investigation`

- Implemented:
  - added `docs/debug-note-c1-threaded-direct-segfault.md`
  - recorded the observed `C1` failure pattern, the serial success comparison,
    the patches added for investigation, and the recommended next repro
    commands
- Why:
  - the repo rules require a debug note during debugging, and this issue now
    has enough concrete evidence to deserve a dedicated note rather than only a
    chat transcript
- Files:
  - `docs/debug-note-c1-threaded-direct-segfault.md`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `eac21a1` - `refactor(cli): make download strategy automatic`

- Implemented:
  - removed the public `--download-method` flag from `src/gtdb_genomes/cli.py`
  - kept `CliArgs.download_method` internally, but fixed it to `auto` so the
    manifest schema and downstream workflow contract did not need churn
  - updated `src/gtdb_genomes/preflight.py` so supported dry-runs always
    require `datasets`, because automatic planning now always has the option to
    preview supported requests
  - replaced the old direct command builder in `src/gtdb_genomes/download.py`
    with a batch-input direct builder that always emits
    `datasets download genome accession --inputfile ... --filename ...`
  - updated CLI and download contract tests to reject the removed flag and pin
    the new internal-auto behaviour
- Why:
  - the public strategy knob no longer matched the desired product contract
  - the later workflow rewrite depends on one stable planning entrypoint and a
    single direct command shape
- Files:
  - `src/gtdb_genomes/cli.py`
  - `src/gtdb_genomes/preflight.py`
  - `src/gtdb_genomes/download.py`
  - `tests/test_cli.py`
  - `tests/test_cli_integration.py`
  - `tests/test_download.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_cli.py`
  - `.venv/bin/python -m pytest -q tests/test_cli_integration.py`
  - `.venv/bin/python -m pytest -q tests/test_download.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `9ce2b60` - `refactor(workflow): batch direct retries with fallback`

- Implemented:
  - removed the threaded per-accession direct-download path from
    `src/gtdb_genomes/workflow.py`
  - added direct batch passes labelled `direct_batch_1` to `direct_batch_4`
    that write one accession file per pass, run one batch direct download,
    extract one archive, keep partial successes, and retry only unresolved
    request accessions
  - preserved original-accession fallback for unresolved preferred-`GCA`
    plans through a second pass family labelled
    `direct_fallback_batch_1` to `direct_fallback_batch_4`
  - kept `paired_to_gca_fallback_original_on_download_failure` alive for
    successful original-accession fallback rows
  - reworked shared batch failures into scoped shared-failure contexts so
    failure manifests can still collapse shared command attempts without
    smearing them across unaffected accessions
  - updated the edge-contract suite to cover one-pass direct success, partial
    success plus retry, preferred-to-original fallback, unresolved failures,
    automatic preview, and the new shared-failure wrapper
- Why:
  - the intermittent remote `C1` segfault sat in the old threaded direct path
  - batch-input direct downloads are simpler to reason about, preserve partial
    successes naturally, and still allow the original-accession recovery path
    the user asked to keep
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `14aebc0` - `chore(validation): align automatic download guidance`

- Implemented:
  - removed `--download-method` from the local and remote real-data runners
  - removed `REAL_DATA_C1_THREADS` from the remote runner and kept the generic
    `REAL_DATA_PYTHON_FAULTHANDLER` and `REAL_DATA_DEBUG_SAFE` helpers
  - updated local runner command requirements so all documented `A*` dry-runs
    now require `datasets`, matching the new automatic-preview contract
  - refreshed `README.md`, `docs/usage-details.md`,
    `docs/real-data-validation.md`, and `docs/development-plan.md` so they
    describe automatic strategy selection, direct batch passes, and retained
    original-accession fallback
  - replaced the old threaded-direct debug note with a closure note that
    explains the threaded path was removed
  - updated doc and shell-helper tests to pin the new runner commands and the
    absence of the public strategy flag
- Why:
  - after the runtime rewrite, the validation scripts and user-facing docs
    were the main remaining source of stale guidance
  - the local dry-run requirements changed materially once preview became part
    of automatic planning for supported requests
- Files:
  - `README.md`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-remote.sh`
  - `docs/debug-note-c1-threaded-direct-segfault.md`
  - `docs/development-plan.md`
  - `docs/real-data-validation.md`
  - `docs/usage-details.md`
  - `tests/test_entrypoints.py`
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_download.py tests/test_edge_contract.py tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `052c315` - `refactor(cli): set fixed thread default`

- Implemented:
  - replaced the old machine-dependent thread default in
    `src/gtdb_genomes/cli.py` with the fixed constant `DEFAULT_THREADS = 8`
  - simplified the `--threads` help text so it now tells users only that the
    option chooses how many CPUs to use for the run and that the default is `8`
  - kept `CliArgs.download_method` fixed to `auto` and left the rest of the
    CLI contract unchanged
  - added a CLI regression test that pins the documented default thread count
    at `8`
- Why:
  - the previous default varied by host and made the user-facing contract
    harder to explain and harder to test
  - the requested documentation tone was simpler than the earlier internal
    concurrency explanation
- Files:
  - `src/gtdb_genomes/cli.py`
  - `tests/test_cli.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_cli.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `d9c2138` - `refactor(workflow): split workflow phases`

- Implemented:
  - split `run_workflow()` in `src/gtdb_genomes/workflow.py` into phase helpers
    for bundled-data selection, early dry-run preflight, supported planning,
    real-run execution, and output materialisation
  - added `WorkflowSelectionPhase` and `WorkflowPlanningPhase` dataclasses so
    state moves between phases with named fields instead of long tuples
  - added an early dry-run `unzip` check before the zero-match and
    unsupported-only dry-run exits, so missing archive support now fails fast
    with the same preflight error path as real runs
  - added simple maintainers' comments at the less obvious retry and
    orchestration boundaries
  - expanded INFO logging for the main workflow milestones, including run
    start, release selection, supported-versus-unsupported counts, metadata
    lookup, automatic planning, direct and dehydrated batch phases,
    output-writing start, and final run summary
  - added edge-contract coverage for the new early `unzip` failure path and
    for the dry-run and real-run INFO milestones
- Why:
  - `run_workflow()` had accumulated several distinct phases and was difficult
    to follow in one block
  - dry-runs previously hid the `unzip` requirement until a later real run,
    which made remote validation and first-time usage more confusing
  - the earlier silent execution left too little reassurance about progress
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_logging.py`
- Match to requested plan:
  - yes
- Deviations:
  - bundled release resolution still happens before the early dry-run `unzip`
    check, because the workflow must know which bundled taxonomy to read before
    it can decide whether there are zero matches

### Commit `882d50c` - `chore(validation): add server wrapper`

- Implemented:
  - added `bin/run-real-data-tests-server.sh` as the preferred on-server
    wrapper for the existing remote runner, with `smoke`, `full`,
    `full-large`, and explicit-case passthrough modes
  - kept the wrapper thin: it does not build, copy, install, or know about any
    server inventory, and it preserves the existing environment-variable based
    controls
  - updated `bin/run-real-data-tests-local.sh` so the documented `A1` to `A9`
    dry-run family now checks for `unzip` as well as `datasets`
  - refreshed `README.md`, `docs/usage-details.md`, and
    `docs/real-data-validation.md` so they document the fixed thread default,
    the earlier dry-run `unzip` expectation, and the new server wrapper
  - added regression coverage for the wrapper presets and passthrough
    behaviour, plus documentation assertions for the new wording
- Why:
  - the runtime changes needed a single-script remote entrypoint so the user
    can run the common server validation path without remembering the case list
  - the docs and helper scripts needed to surface the new `unzip` preflight
    behaviour consistently
- Files:
  - `README.md`
  - `bin/run-real-data-tests-local.sh`
  - `bin/run-real-data-tests-server.sh`
  - `docs/real-data-validation.md`
  - `docs/usage-details.md`
  - `tests/test_entrypoints.py`
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py`
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_download.py tests/test_edge_contract.py tests/test_real_data_scripts.py tests/test_entrypoints.py tests/test_logging.py`
- Match to requested plan:
  - yes
- Deviations:
  - manual remote acceptance was not rerun in this turn

### Commit `aaad58c` - `refactor(workflow): split helper modules`

- Implemented:
  - replaced the large phase-wrapper refactor in `src/gtdb_genomes/workflow.py`
    with a thin orchestration entrypoint that reads top-to-bottom as:
    selection, preflight, planning, dry-run exit, real-run execution, and
    output materialisation
  - removed `WorkflowSelectionPhase` and `WorkflowPlanningPhase` instead of
    replacing them with new orchestration classes
  - split the old monolithic workflow helpers into four owner modules:
    `src/gtdb_genomes/workflow_selection.py`,
    `src/gtdb_genomes/workflow_planning.py`,
    `src/gtdb_genomes/workflow_execution.py`, and
    `src/gtdb_genomes/workflow_outputs.py`
  - kept the concrete domain dataclasses that still carry clear value:
    `AccessionPlan`, `AccessionExecution`, `DownloadExecutionResult`,
    `ResolvedPayloadDirectory`, and `SharedFailureContext`
  - kept the earlier runtime behaviour intact, including the automatic method
    choice, the early dry-run `unzip` check, the batch-direct retry logic, the
    original-accession fallback path, and the added INFO logging
  - added heavier structural comments in each workflow module so a maintainer
    can follow the code by section rather than by one long file
  - retargeted `tests/test_edge_contract.py` so direct imports and monkeypatch
    targets now point at the owner modules instead of treating
    `gtdb_genomes.workflow` as a catch-all helper namespace
- Why:
  - the phase wrapper classes made the workflow harder to follow in an IDE and
    obscured the real execution path for new maintainers
  - moving helper families into owner modules keeps the main workflow readable
    without flattening all responsibilities back into one large script
  - updating the tests to patch the owner modules makes future refactors safer,
    because the patch targets now match the real dependency boundaries
- Files:
  - `src/gtdb_genomes/workflow.py`
  - `src/gtdb_genomes/workflow_selection.py`
  - `src/gtdb_genomes/workflow_planning.py`
  - `src/gtdb_genomes/workflow_execution.py`
  - `src/gtdb_genomes/workflow_outputs.py`
  - `tests/test_edge_contract.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_edge_contract.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_edge_contract.py tests/test_download.py tests/test_logging.py tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to requested plan:
  - yes
- Deviations:
  - none

### Commit `d1b89b3` - `feat(workflow): warn on suppressed assembly targets`

- Implemented:
  - extended `src/gtdb_genomes/metadata.py` so the existing
    `datasets summary genome accession --as-json-lines` path now preserves
    structured assembly status information as well as accession pairing
  - parsed `assemblyInfo.assemblyStatus`, `assemblyInfo.suppressionReason`, and
    `assemblyInfo.pairedAssembly.status` without changing the existing
    accession-preference logic
  - updated `src/gtdb_genomes/workflow_planning.py` to derive suppression notes
    for the actual selected download target, not just the original accession
  - kept the warning logic aligned with the selected target:
    a suppressed original RefSeq assembly does not warn if the selected paired
    GenBank accession is still current
  - changed supported-accession planning to reuse metadata lookups for warning
    purposes even when `--prefer-genbank` is not enabled
  - updated `src/gtdb_genomes/workflow.py` to emit a planning-time warning when
    the selected target is metadata-confirmed suppressed, and a second warning
    after output materialisation if that accession still failed
  - updated `src/gtdb_genomes/workflow_outputs.py` so
    `download_failures.tsv` appends a standard suppression note for failed
    suppressed accessions without changing the TSV schema
- Why:
  - the remote `C5` failure for `GCF_003670205.1` was real, but the workflow
    exposed it as a generic failure instead of explaining that NCBI marked the
    assembly as suppressed
  - the live NCBI finding showed that suppression does not always remove every
    accession-level artefact, so the code needed a metadata-based warning
    rather than a blanket "suppressed means missing" rule
  - reusing the existing metadata summary path keeps runtime scope narrow and
    avoids bolting FTP probes onto normal workflow execution
- Files:
  - `src/gtdb_genomes/metadata.py`
  - `src/gtdb_genomes/workflow.py`
  - `src/gtdb_genomes/workflow_outputs.py`
  - `src/gtdb_genomes/workflow_planning.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_metadata.py tests/test_edge_contract.py tests/test_logging.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_edge_contract.py tests/test_download.py tests/test_logging.py tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to requested change:
  - yes
- Deviations:
  - suppression detection relies on `datasets summary` metadata only; no FTP or
    extra API probe was added to runtime code

### Commit `dbb3069` - `test(workflow): cover suppressed assembly warnings`

- Implemented:
  - added metadata tests that verify summary parsing preserves assembly status,
    suppression reason, and paired-assembly status
  - added planning tests that verify:
    suppressed unchanged targets warn,
    paired-target warnings follow the selected accession,
    and suppressed original accessions do not warn when the selected paired
    accession is still current
  - added integrated workflow tests that verify:
    dry-runs warn during planning,
    failed suppressed accessions warn again at the end,
    and `download_failures.tsv` carries the suppression note
  - updated mixed-UBA tests to reflect the new metadata lookup behaviour for
    supported accessions while keeping the unsupported-only no-metadata path
    intact
- Why:
  - the suppression warning needed both parser-level coverage and end-to-end
    workflow coverage because the note is carried from planning into final
    output writing
  - the mixed-UBA regression mattered because metadata lookup is now reused for
    warning purposes even when the run is not using `--prefer-genbank`
- Files:
  - `tests/test_edge_contract.py`
  - `tests/test_metadata.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_metadata.py tests/test_edge_contract.py tests/test_logging.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_edge_contract.py tests/test_download.py tests/test_logging.py tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to requested change:
  - yes
- Deviations:
  - none

### Commit `<pending>` - `fix(validation): accept suppressed-only partial C5`

- Implemented:
  - updated `bin/real-data-test-common.sh` so `real_data_run_case` now accepts
    an expected-exit regex instead of a single exact integer, while keeping the
    existing exact patterns unchanged for all current callers
  - added a dedicated `C5` post-check in
    `bin/run-real-data-tests-remote.sh` that still requires
    `download_method_used` to be `dehydrate` or `dehydrate_fallback_direct`
  - changed `C5` to accept exit `0` or `6`, but only treat exit `6` as a pass
    when every failed accession in `accession_map.tsv` has a matching
    suppression-note row in `download_failures.tsv`
  - kept `C7` strict and unchanged
  - updated `docs/real-data-validation.md` so the `C5` acceptance rule now
    states the suppressed-only partial-success exception explicitly
- Why:
  - after the workflow gained suppressed-assembly warnings, `C5` began to
    produce the documented partial-success exit `6` when a single suppressed
    accession failed, but the remote runner still treated `C5` as a strict
    exit-`0` case
  - this was a validation contract mismatch rather than a workflow download bug
  - the suppression note already written by the workflow is the narrowest
    signal available to admit only the intended partial-failure case
- Files:
  - `bin/real-data-test-common.sh`
  - `bin/run-real-data-tests-remote.sh`
  - `docs/real-data-validation.md`
  - `tests/test_real_data_scripts.py`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_real_data_scripts.py tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q tests/test_cli.py tests/test_cli_integration.py tests/test_metadata.py tests/test_edge_contract.py tests/test_download.py tests/test_logging.py tests/test_real_data_scripts.py tests/test_entrypoints.py`
- Match to requested change:
  - yes
- Deviations:
  - scope stayed limited to `C5`; `C7` remains strict for now

### Commit `9dc56da` - `fix(licensing): declare mixed archive metadata`

- Implemented:
  - updated `pyproject.toml` so built distribution archives now declare
    `MIT AND CC-BY-SA-4.0` instead of `MIT` alone
  - added `licenses/CC-BY-SA-4.0.txt` so wheel and sdist builds carry the
    bundled GTDB data licence text alongside the existing project `LICENSE`
    and `NOTICE`
  - replaced the previous generic bundled-data warning in `NOTICE` with an
    explicit code-vs-data split, GTDB attribution, licence URLs, and a clear
    statement that the bundled taxonomy payload is not relicensed by this
    project
  - updated `README.md`, `docs/usage-details.md`, and the Bioconda recipe
    template so user-facing packaging metadata no longer implies an MIT-only
    release when GTDB taxonomy data is bundled
  - tightened `tests/test_entrypoints.py` to lock the mixed-licence metadata,
    the new README badges, the explicit notice text, and the presence of the
    bundled CC BY-SA 4.0 licence file
- Why:
  - the published wheel and sdist previously bundled GTDB taxonomy payloads
    while advertising `MIT` as the archive licence expression, which was
    misleading once the distribution archive itself was considered
  - the previous `NOTICE` warned about upstream terms but did not ship the
    actual CC BY-SA 4.0 text or concrete attribution details needed for the
    bundled data path
  - aligning the build metadata, packaged licence files, and release-facing
    docs reduces the risk of publishing an archive that overstates the reach
    of the MIT licence
- Files:
  - `pyproject.toml`
  - `NOTICE`
  - `README.md`
  - `docs/usage-details.md`
  - `packaging/bioconda/meta.yaml`
  - `tests/test_entrypoints.py`
  - `licenses/CC-BY-SA-4.0.txt`
- Checks run:
  - `.venv/bin/python -m pytest -q tests/test_entrypoints.py`
  - `.venv/bin/python -m pytest -q`
  - `PATH=/Users/asuq/miniforge3/envs/gtdb-genome/bin:/usr/bin:/bin UV_CACHE_DIR=/tmp/gtdb_uv_cache /Users/asuq/miniforge3/envs/gtdb-genome/bin/uv build --out-dir /tmp/gtdb_license_build`
  - `python3 -m zipfile -l /tmp/gtdb_license_build/gtdb_genomes-0.1.0-py3-none-any.whl`
  - `python3 - <<'PY' ... inspect wheel METADATA and packaged licence files ... PY`
  - `tar -tzf /tmp/gtdb_license_build/gtdb_genomes-0.1.0.tar.gz`
  - `tar -xOzf /tmp/gtdb_license_build/gtdb_genomes-0.1.0.tar.gz gtdb_genomes-0.1.0/PKG-INFO`
- Match to requested change:
  - yes
- Deviations:
  - the bundled GTDB taxonomy payload remains in the archives; this change
    corrects the archive metadata and packaged notices rather than switching to
    an MIT-only package layout

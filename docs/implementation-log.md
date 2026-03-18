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

# 2026-03-21 C5 CRLF Runtime Parser Fix

Commit:

- `15a66ea` `fix(validation): normalise CRLF in C5 runtime parser`

Context:

- GitHub Actions `CI #35` failed in `Validation C runtime`.
- The failing message was:
  `FAIL: remote dehydrate suppressed partial: no failed accessions found`.
- The core workflow execution code was not the cause. The failure came from the
  packaged-runtime bash validation post-check for `C5`.

Root cause:

- `accession_map.tsv` is written through Python `csv.DictWriter`.
- That writer emits CRLF line endings in the TSV files used by the bash
  validation scripts.
- In `bin/run-real-data-tests-remote.sh`, the `C5` suppression-partial check
  parsed `download_status` from the last TSV column and compared it to the
  literal `failed`.
- On CI, awk saw `failed\r`, so the filter found no failed accessions and
  reported the wrong error.

Changes:

- Added a shared helper in `bin/real-data-test-common.sh` to read TSV columns
  by header name, strip trailing carriage returns, and return unique matched
  values.
- Switched the `C5` failed-accession collection in
  `bin/run-real-data-tests-remote.sh` to that helper.
- Normalised carriage returns in the suppression-note matcher as well.
- Simplified the matcher to use an exact substring check via awk `index(...)`
  instead of a regex, which is more portable across macOS and Linux awk
  implementations.
- Added regression tests in `tests/test_real_data_scripts.py` for:
  - the shared TSV helper on CRLF-terminated last columns
  - the full `C5` post-check on CRLF-terminated `accession_map.tsv` and
    `download_failures.tsv`

Verification:

- `mamba run -n gtdb-genome uv run --group dev pytest -q tests/test_real_data_scripts.py tests/test_entrypoints.py`
- `mamba run -n gtdb-genome uv run --group dev pytest -q`

Pitfalls:

- The original failure message pointed at missing failed accessions, but the
  real issue was TSV line-ending handling in the bash validation layer.
- The inline awk matcher also needed a portability cleanup, because a multi-line
  `if (...)` expression was rejected by the local awk used during regression
  testing.

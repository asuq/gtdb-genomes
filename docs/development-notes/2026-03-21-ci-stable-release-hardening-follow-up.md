## 2026-03-21: CI-stable release hardening follow-up

Primary implementation commit: `90d22ed` (`fix(release): harden bundled taxonomy and CI contracts`)

### Why this follow-up was needed

The release-hardening pass still had three practical gaps:

- the CI packaging regression test could become false after the workspace had
  already been bootstrapped
- partial paired-`GCA_*` metadata could still be treated as usable during
  `--prefer-genbank` planning
- runtime bundled-data validation still paid for a duplicate full-file pass in
  release resolution before taxonomy loading parsed the same files again

There were also contract mismatches around the public TSV schema, local
`--include` validation, and the exact external toolchain pinned in workflows.

### Design choices

- Packaging fixtures are now explicitly manifest-only by default. Positive
  packaging tests build their own synthetic bundled release instead of relying
  on ambient checkout state.
- Partial paired-`GCA_*` fallback is intentionally narrow. The workflow now
  falls back to the original accession only when the second-pass candidate
  lookup is partial, not merely when a candidate status is absent in an
  otherwise successful response.
- Runtime bundled-data validation now happens at taxonomy load time so each
  taxonomy payload is read once per run. Resolver-time validation still checks
  manifest integrity, path wiring, and file presence. Build-time validation
  remains strict and still performs full checksum and row-count verification.
- Local `--include` validation is now a small explicit allow-list:
  `genome`, `gff3`, and `protein`. This gives stable CLI errors for typos
  instead of deferring them to `datasets`.

### Files and behaviour touched

- `tests/test_entrypoints.py`: make the copied build fixture independent of
  any bootstrapped payloads already present in the checkout
- `src/gtdb_genomes/metadata.py` and
  `src/gtdb_genomes/workflow_planning.py`: preserve attempted candidate sets in
  metadata failures and add the partial paired-`GCA_*` fallback path
- `src/gtdb_genomes/release_resolver.py`,
  `src/gtdb_genomes/taxonomy.py`, and
  `src/gtdb_genomes/bundled_data_validation.py`: move content validation into
  taxonomy loading while keeping build-time payload verification strict
- `src/gtdb_genomes/download.py`: reject unsupported `--include` tokens locally
- `.github/workflows/*.yml` and docs: pin tested tool versions and document the
  provenance-expanded runtime contract

### Verification

- `mamba run -n gtdb-genome uv run --group dev pytest -q tests/test_metadata.py tests/test_workflow_planning.py tests/test_release_resolver.py tests/test_download.py`
- `mamba run -n gtdb-genome uv run --group dev pytest -q tests/test_entrypoints.py tests/test_real_data_scripts.py`
- `mamba run -n gtdb-genome uv run --group dev pytest -q`
- `mamba run -n gtdb-genome git diff --check`

All of the above passed after the implementation.

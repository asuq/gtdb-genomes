# C1 Segfault Closure Note

## Original failure

- remote packaged-runtime case `C1` intermittently exited `139`
- the failing runs created the output root plus `.gtdb_genomes_work/` and
  `taxa/`, but wrote no manifests and no stderr text
- the same remote environment completed:
  - `remote-smoke-c1`
  - `C4`
  - `C6`

## Resolution

- the previous direct path used threaded per-accession downloads
- that path has been removed
- direct mode now uses batch-input `datasets download genome accession
  --inputfile ... --filename ...` passes
- partial successes are kept across direct passes
- unresolved preferred `GCA_*` requests may still fall back to the original
  accession, preserving
  `paired_to_gca_fallback_original_on_download_failure`

## Remaining investigation tooling

- `REAL_DATA_PYTHON_FAULTHANDLER=1` still prefixes runner commands with
  `PYTHONFAULTHANDLER=1`
- `REAL_DATA_DEBUG_SAFE=1` still appends `--debug` only to no-key cases
- runner evidence still copies `debug.log` when a run writes one

## Follow-up

If a remote real-data case fails again, rerun the case with
`REAL_DATA_PYTHON_FAULTHANDLER=1` and `REAL_DATA_DEBUG_SAFE=1`, then inspect
`_evidence/<case-id>/debug.log`, `stderr.log`, and `run_summary.tsv`.

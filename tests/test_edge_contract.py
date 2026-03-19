"""Contract-level edge-case tests for the integrated workflow."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import CliArgs, main
from gtdb_genomes.download import (
    CommandFailureRecord,
    PreviewError,
    RetryableCommandResult,
)
from gtdb_genomes.layout import initialise_run_directories
from gtdb_genomes.metadata import SummaryLookupResult
from gtdb_genomes.workflow import (
    AccessionExecution,
    AccessionPlan,
    DownloadExecutionResult,
    build_failure_rows,
    execute_batch_dehydrate_plans,
)


def build_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a minimal taxonomy frame for workflow tests."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1"],
            "lineage": [lineage],
            "ncbi_accession": ["GCF_000001.1"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )


def parse_tsv(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return the header and rows from a TSV output file."""

    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[1:]]
    return header, rows


def test_zero_match_run_writes_header_only_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Zero matches should create the documented output tree and exit 4."""

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame("d__Bacteria;p__Firmicutes;g__Bacillus"),
    )

    output_dir = tmp_path / "zero-match"
    exit_code = main(
        [
            "--release",
            "95",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
        ],
    )

    assert exit_code == 4
    assert (output_dir / "run_summary.tsv").exists()
    assert (output_dir / "accession_map.tsv").read_text().splitlines() == [
        "requested_taxon\ttaxon_slug\tresolved_release\ttaxonomy_file\tlineage\tgtdb_accession\tfinal_accession\taccession_type_original\taccession_type_final\tconversion_status\tdownload_method_used\tdownload_batch\toutput_relpath\tdownload_status",
    ]
    assert (
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv"
    ).exists()


def test_auto_preview_failure_returns_exit_code_five_without_output_tree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Preview failures in auto mode should stop before output creation."""

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(PreviewError("preview failed")),
    )

    output_dir = tmp_path / "preview-failure"
    exit_code = main(
        [
            "--release",
            "95",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "auto",
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_total_runtime_failure_leaves_final_accession_blank(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Total failure should blank `final_accession` and exit 7."""

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return a failed accession execution for the synthetic run."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="download failed",
                            final_status="retry_exhausted",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "runtime-failure"
    exit_code = main(
        [
            "--release",
            "95",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
        ],
    )

    assert exit_code == 7
    accession_map_lines = (output_dir / "accession_map.tsv").read_text().splitlines()
    assert accession_map_lines[1].split("\t")[6] == ""
    assert accession_map_lines[1].split("\t")[9] == "failed_no_usable_accession"
    assert accession_map_lines[1].split("\t")[13] == "failed"
    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_concurrency_used"] == "1"
    assert run_summary["rehydrate_workers_used"] == "0"


def test_failure_manifest_collapses_shared_accession_taxa(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """One shared failed accession should yield one root failure row."""

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia;s__Escherichia coli",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one failed execution for the shared accession."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="download failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCF_000001.1",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-failure"
    exit_code = main(
        [
            "--release",
            "95",
            "--taxon",
            "g__Escherichia",
            "--taxon",
            "s__Escherichia coli",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
        ],
    )

    assert exit_code == 7
    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["requested_taxon"] == "g__Escherichia;s__Escherichia coli"
    assert failure["attempted_accession"] == "GCF_000001.1"


def test_failure_manifest_collapses_shared_metadata_attempts() -> None:
    """Shared metadata retries should be written once per command attempt."""

    enriched_rows = [
        {
            "requested_taxon": "g__Escherichia",
            "taxon_slug": "g__Escherichia",
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
        },
        {
            "requested_taxon": "s__Escherichia coli",
            "taxon_slug": "s__Escherichia_coli",
            "gtdb_accession": "RS_GCF_000002.1",
            "ncbi_accession": "GCF_000002.1",
            "final_accession": "GCF_000002.1",
        },
    ]
    executions = {
        "GCF_000001.1": AccessionExecution(
            original_accession="GCF_000001.1",
            final_accession="GCF_000001.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            payload_directory=None,
            failures=(),
        ),
        "GCF_000002.1": AccessionExecution(
            original_accession="GCF_000002.1",
            final_accession="GCF_000002.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            payload_directory=None,
            failures=(),
        ),
    }
    metadata_failures = (
        CommandFailureRecord(
            stage="metadata_lookup",
            attempt_index=1,
            max_attempts=4,
            error_type="metadata_lookup",
            error_message="temporary failure",
            final_status="retry_scheduled",
        ),
        CommandFailureRecord(
            stage="metadata_lookup",
            attempt_index=2,
            max_attempts=4,
            error_type="metadata_lookup",
            error_message="temporary failure",
            final_status="retry_exhausted",
        ),
    )

    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        metadata_failures,
        (),
        (),
    )

    assert len(failure_rows) == 2
    assert failure_rows[0]["requested_taxon"] == (
        "g__Escherichia;s__Escherichia coli"
    )
    assert failure_rows[0]["attempted_accession"] == (
        "GCF_000001.1;GCF_000002.1"
    )
    assert failure_rows[0]["final_accession"] == (
        "GCF_000001.1;GCF_000002.1"
    )


def test_batch_dehydrate_failure_falls_back_to_direct(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed batch dehydrated download should fall back to direct mode."""

    plans = (
        AccessionPlan(
            original_accession="GCF_000001.1",
            preferred_accession="GCA_000001.1",
            conversion_status="paired_to_gca",
        ),
        AccessionPlan(
            original_accession="GCF_000002.1",
            preferred_accession="GCA_000002.1",
            conversion_status="paired_to_gca",
        ),
    )
    args = CliArgs(
        release="95",
        taxa=("g__Escherichia",),
        output=tmp_path / "output",
        prefer_genbank=True,
        download_method="dehydrate",
        threads=4,
        api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )
    run_directories = initialise_run_directories(tmp_path / "batch-output")

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        lambda *args, **kwargs: RetryableCommandResult(
            succeeded=False,
            stdout="",
            stderr="batch failed",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=4,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="batch failed",
                    final_status="retry_exhausted",
                ),
            ),
        ),
    )

    def fake_execute_direct_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        run_directories,
        logger,
    ) -> DownloadExecutionResult:
        """Return a synthetic direct-download fallback result."""

        return DownloadExecutionResult(
            executions={
                plan.original_accession: AccessionExecution(
                    original_accession=plan.original_accession,
                    final_accession=plan.original_accession,
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    payload_directory=tmp_path,
                    failures=(),
                )
                for plan in plans
            },
            method_used="direct",
            download_concurrency_used=2,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_direct_accession_plans",
        fake_execute_direct_accession_plans,
    )

    result = execute_batch_dehydrate_plans(
        plans,
        args,
        run_directories,
        logging.getLogger("test"),
        (),
    )

    assert result.method_used == "dehydrate_fallback_direct"
    assert result.download_concurrency_used == 2
    assert result.executions["GCF_000001.1"].failures == ()
    assert result.shared_failures[0].attempted_accession == (
        "GCA_000001.1;GCA_000002.1"
    )

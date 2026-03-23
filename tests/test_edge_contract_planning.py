"""Contract-level edge-case tests for planning, logging, and metadata lookup."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.cli import CliArgs
from gtdb_genomes.download import (
    CommandFailureRecord,
    DownloadMethodDecision,
    RetryableCommandResult,
)
from gtdb_genomes.workflow_execution_models import (
    AccessionExecution,
    DownloadExecutionResult,
    ResolvedPayloadDirectory,
    SharedFailureContext,
)
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    SUPPRESSED_ASSEMBLY_NOTE,
    SummaryLookupResult,
)
from gtdb_genomes.workflow_planning import (
    build_failed_suppressed_warning,
    build_planning_suppressed_warning,
    build_suppressed_accession_notes,
    create_staging_directory,
    plan_supported_downloads,
)
from gtdb_genomes.workflow_selection import build_unsupported_uba_warning
from tests.workflow_contract_helpers import (
    build_cli_args,
    build_multi_accession_taxonomy_frame,
    build_taxonomy_frame,
    install_fake_release_resolution,
    install_capture_logger,
    parse_tsv,
)


@pytest.fixture(autouse=True)
def fake_release_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep planning-contract tests independent of generated checkout data."""

    install_fake_release_resolution(monkeypatch)


def test_build_unsupported_uba_warning_mentions_examples_and_bioproject() -> None:
    """The UBA warning builder should produce deterministic user guidance."""

    warning_text = build_unsupported_uba_warning(
        pl.DataFrame(
            {
                "requested_taxon": [
                    "g__Escherichia",
                    "g__Escherichia",
                    "s__Escherichia coli",
                ],
                "ncbi_accession": ["UBA11131", "UBA11131", "UBA22222"],
            },
        ),
    )

    assert "Skipping 2 unsupported legacy GTDB UBA accessions" in warning_text
    assert "g__Escherichia;s__Escherichia coli" in warning_text
    assert "UBA11131, UBA22222" in warning_text
    assert "PRJNA417962" in warning_text


def test_build_suppressed_accession_notes_marks_original_suppressed_target() -> None:
    """Suppressed unchanged targets should be carried into warning notes."""

    mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_003670205.1"],
            "final_accession": ["GCF_003670205.1"],
            "conversion_status": ["unchanged_original"],
        },
    )

    notes = build_suppressed_accession_notes(
        mapped_frame,
        {
            "GCF_003670205.1": AssemblyStatusInfo(
                assembly_status="suppressed",
                suppression_reason="removed by submitter",
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
    )

    assert notes["GCF_003670205.1"].selected_accession == "GCF_003670205.1"
    assert "removed by submitter" in build_planning_suppressed_warning(notes)


def test_build_suppressed_accession_notes_uses_selected_paired_status() -> None:
    """Warnings should follow the selected paired accession, not the original one."""

    mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1"],
            "final_accession": ["GCA_000001.3"],
            "conversion_status": ["paired_to_gca"],
        },
    )

    notes = build_suppressed_accession_notes(
        mapped_frame,
        {
            "GCF_000001.1": AssemblyStatusInfo(
                assembly_status="suppressed",
                suppression_reason="original withdrawn",
                paired_accession="GCA_000001.3",
                paired_assembly_status="current",
            ),
        },
    )

    assert notes == {}

    suppressed_paired_notes = build_suppressed_accession_notes(
        mapped_frame,
        {
            "GCF_000001.1": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession="GCA_000001.3",
                paired_assembly_status="suppressed",
            ),
        },
    )

    assert suppressed_paired_notes["GCF_000001.1"].selected_accession == "GCA_000001.3"
    assert "GCF_000001.1 -> GCA_000001.3" in build_planning_suppressed_warning(
        suppressed_paired_notes,
    )


def test_build_failed_suppressed_warning_mentions_failed_accessions() -> None:
    """The final warning should only mention suppressed accessions that failed."""

    notes = {
        "GCF_003670205.1": build_suppressed_accession_notes(
            pl.DataFrame(
                {
                    "ncbi_accession": ["GCF_003670205.1"],
                    "final_accession": ["GCF_003670205.1"],
                    "conversion_status": ["unchanged_original"],
                },
            ),
            {
                "GCF_003670205.1": AssemblyStatusInfo(
                    assembly_status="suppressed",
                    suppression_reason=None,
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            },
        )["GCF_003670205.1"],
    }

    warning_text = build_failed_suppressed_warning(
        notes,
        ("GCF_003670205.1",),
    )

    assert "1 failed assembly was marked suppressed by NCBI" in warning_text
    assert SUPPRESSED_ASSEMBLY_NOTE in warning_text


def test_auto_method_uses_unique_download_request_count_after_stem_collapse_in_latest_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Latest-mode should size the request after collapsing to datasets tokens."""

    supported_mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1", "GCF_000001.2"],
            "final_accession": ["GCA_000001.1", "GCA_000001.2"],
            "conversion_status": ["paired_to_gca", "paired_to_gca"],
        },
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=True,
        version_latest=True,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

    observed_counts: list[int] = []

    def fake_select_download_method(
        accession_count: int,
    ) -> DownloadMethodDecision:
        """Capture the accession count passed into method selection."""

        observed_counts.append(accession_count)
        return DownloadMethodDecision(
            method_used="direct",
            accession_count=accession_count,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.select_download_method",
        fake_select_download_method,
    )

    plans, decision_method = plan_supported_downloads(supported_mapped_frame, args)

    assert len(plans) == 2
    assert {plan.download_request_accession for plan in plans} == {"GCA_000001"}
    assert observed_counts == [1]
    assert decision_method == "direct"


def test_auto_method_keeps_versioned_requests_by_default_with_prefer_genbank(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Prefer-GenBank should keep versioned request accessions by default."""

    supported_mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1", "GCF_000001.2"],
            "final_accession": ["GCA_000001.1", "GCA_000001.2"],
            "conversion_status": ["paired_to_gca", "paired_to_gca"],
        },
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=True,
        version_latest=False,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

    observed_counts: list[int] = []

    def fake_select_download_method(
        accession_count: int,
    ) -> DownloadMethodDecision:
        """Capture the accession count passed into method selection."""

        observed_counts.append(accession_count)
        return DownloadMethodDecision(
            method_used="direct",
            accession_count=accession_count,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.select_download_method",
        fake_select_download_method,
    )

    plans, decision_method = plan_supported_downloads(supported_mapped_frame, args)

    assert len(plans) == 2
    assert {plan.download_request_accession for plan in plans} == {
        "GCA_000001.1",
        "GCA_000001.2",
    }
    assert observed_counts == [2]
    assert decision_method == "direct"


def test_plan_supported_downloads_switches_to_dehydrate_at_request_threshold(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The planner should use only the request-token count threshold."""

    supported_mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": [
                f"GCF_{accession_index:09d}.1"
                for accession_index in range(1, 1001)
            ],
            "final_accession": [
                f"GCF_{accession_index:09d}.1"
                for accession_index in range(1, 1001)
            ],
            "conversion_status": ["unchanged_original"] * 1000,
        },
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=False,
        version_latest=False,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

    plans, decision_method = plan_supported_downloads(supported_mapped_frame, args)

    assert len(plans) == 1000
    assert decision_method == "dehydrate"


def test_plan_supported_downloads_uses_count_based_selection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Count-based planning should rely only on unique request tokens."""

    supported_mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1", "GCF_000002.1"],
            "final_accession": ["GCF_000001.1", "GCF_000002.1"],
            "conversion_status": ["unchanged_original", "unchanged_original"],
        },
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=False,
        version_latest=False,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

    observed_counts: list[int] = []

    def fake_select_download_method(
        accession_count: int,
    ) -> DownloadMethodDecision:
        """Capture the request count without any preview-specific seam."""

        observed_counts.append(accession_count)
        return DownloadMethodDecision(
            method_used="direct",
            accession_count=accession_count,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.select_download_method",
        fake_select_download_method,
    )

    plans, decision_method = plan_supported_downloads(supported_mapped_frame, args)

    assert len(plans) == 2
    assert observed_counts == [2]
    assert decision_method == "direct"


def test_dry_run_logs_info_milestones(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should emit the new high-level INFO milestones."""

    log_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    output_dir = tmp_path / "dry-run-info"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    log_text = log_stream.getvalue()
    assert "INFO Starting run:" in log_text
    assert "INFO Checking unzip availability for dry-run" in log_text
    assert "INFO Resolved bundled release 95" in log_text
    assert "INFO Selected 1 supported accession(s) and 0 unsupported legacy accession(s)" in (
        log_text
    )
    assert "INFO Automatic planning selected direct for 1 supported accession(s)" in (
        log_text
    )
    assert "INFO Dry-run finished:" in log_text


def test_dry_run_warns_for_suppressed_planned_accession(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should warn when metadata marks the planned target suppressed."""

    log_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(
            summary_map={},
            status_map={
                "GCF_000001.1": AssemblyStatusInfo(
                    assembly_status="suppressed",
                    suppression_reason="removed by submitter",
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            },
            failures=(),
        ),
    )

    output_dir = tmp_path / "dry-run-suppressed-warning"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert "NCBI marks 1 planned assembly as suppressed" in log_stream.getvalue()


def test_real_run_logs_info_milestones(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Real runs should emit the new high-level INFO milestones."""

    log_stream = install_capture_logger(monkeypatch)
    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        lambda *args, **kwargs: RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        lambda archive_path, extraction_root: extraction_root.mkdir(
            parents=True,
            exist_ok=True,
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        lambda extraction_root: (
            ResolvedPayloadDirectory(
                final_accession="GCF_000001.1",
                directory=payload_directory,
            ),
        ),
    )

    output_dir = tmp_path / "real-run-info"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    log_text = log_stream.getvalue()
    assert "INFO Starting run:" in log_text
    assert "INFO Resolved bundled release 95" in log_text
    assert "INFO Automatic planning selected direct for 1 supported accession(s)" in (
        log_text
    )
    assert "INFO direct_batch_1: starting preferred_download for 1 request accession(s)" in (
        log_text
    )
    assert "INFO direct_batch_1: completed with 1 resolved and 0 pending request accession(s)" in (
        log_text
    )
    assert "INFO Writing output manifests to" in log_text
    assert "INFO Run finished: successful_accessions=1 failed_accessions=0 exit_code=0" in (
        log_text
    )


def test_metadata_lookup_uses_accession_input_file_and_cleans_it_up(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Metadata lookup should use a temporary input file outside the output tree."""

    metadata_inputs: list[Path] = []
    metadata_contents: list[str] = []

    def fake_run_summary_lookup_with_retries(
        accessions: tuple[str, ...] | list[str],
        accession_file: Path,
        ncbi_api_key: str | None = None,
        datasets_bin: str = "datasets",
        sleep_func=None,
    ) -> SummaryLookupResult:
        """Capture the metadata input file used by workflow planning."""

        del ncbi_api_key, datasets_bin, sleep_func
        metadata_inputs.append(accession_file)
        metadata_contents.append(accession_file.read_text(encoding="ascii"))
        assert tuple(accessions) == ("GCF_000001.1", "GCF_000002.1")
        assert accession_file.is_file()
        assert accession_file.parent.name.startswith("gtdb_genomes_metadata_")
        return SummaryLookupResult(summary_map={}, failures=())

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_multi_accession_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    output_dir = tmp_path / "metadata-input-file"
    exit_code = main(
        [
            "--gtdb-release",
            "202",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(metadata_inputs) == 1
    assert metadata_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not metadata_inputs[0].exists()


def test_create_staging_directory_uses_tmpdir_when_configured(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Workflow staging directories should respect the configured temp root."""

    temp_root = tmp_path / "custom-temp-root"
    monkeypatch.setenv("TMPDIR", str(temp_root))

    with create_staging_directory("gtdb_genomes_test_") as staging_directory:
        staging_path = Path(staging_directory)
        assert staging_path.parent == temp_root
        assert staging_path.name.startswith("gtdb_genomes_test_")

    assert not staging_path.exists()


def test_total_runtime_failure_leaves_final_accession_blank(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Total failure should blank `final_accession` and exit 7."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> object:
        """Return a failed accession execution for the synthetic run."""

        from gtdb_genomes.download import CommandFailureRecord
        from gtdb_genomes.workflow_execution_models import (
            AccessionExecution,
            DownloadExecutionResult,
        )

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="GCF_000001.1",
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
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "runtime-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["final_accession"] == ""
    assert accession_map["conversion_status"] == "failed_no_usable_accession"
    assert accession_map["download_status"] == "failed"
    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_concurrency_used"] == "1"
    assert run_summary["rehydrate_workers_used"] == "0"


def test_failed_suppressed_accession_repeats_warning_and_failure_note(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Failed suppressed accessions should warn again and annotate failure rows."""

    log_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(
            summary_map={},
            status_map={
                "GCF_000001.1": AssemblyStatusInfo(
                    assembly_status="suppressed",
                    suppression_reason="removed by submitter",
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            },
            failures=(),
        ),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> object:
        """Return one failed execution for the suppressed accession."""

        from gtdb_genomes.download import CommandFailureRecord
        from gtdb_genomes.workflow_execution_models import (
            AccessionExecution,
            DownloadExecutionResult,
        )

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
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
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "suppressed-runtime-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    log_text = log_stream.getvalue()
    assert "NCBI marks 1 planned assembly as suppressed" in log_text
    assert "1 failed assembly was marked suppressed by NCBI" in log_text

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert SUPPRESSED_ASSEMBLY_NOTE in failure["error_message_redacted"]

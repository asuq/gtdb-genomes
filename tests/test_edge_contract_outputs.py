"""Contract-level edge-case tests for output manifests and previews."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.download import (
    CommandFailureRecord,
    PreviewError,
)
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    SummaryLookupResult,
    SUPPRESSED_ASSEMBLY_NOTE,
)
from gtdb_genomes.workflow_execution import (
    AccessionExecution,
    DownloadExecutionResult,
)
from gtdb_genomes.workflow_outputs import build_failure_rows
from tests.workflow_contract_helpers import (
    build_multi_accession_taxonomy_frame,
    build_mixed_uba_taxonomy_frame,
    build_shared_preferred_taxonomy_frame,
    build_taxonomy_frame,
    build_uba_only_taxonomy_frame,
    install_fake_release_resolution,
    install_capture_logger,
    parse_tsv,
)


@pytest.fixture(autouse=True)
def fake_release_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep output-contract tests independent of generated checkout data."""

    install_fake_release_resolution(monkeypatch)


def test_auto_preview_failure_returns_exit_code_five_without_output_tree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Preview failures in auto mode should stop before output creation."""

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
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(PreviewError("preview failed")),
    )

    output_dir = tmp_path / "preview-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_auto_preview_uses_accession_input_file_and_keeps_output_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Auto preview should use a temporary input file outside the output tree."""

    preview_inputs: list[Path] = []
    preview_contents: list[str] = []

    def fake_run_preview_command(
        accession_file: Path,
        include: str,
        ncbi_api_key: str | None = None,
        datasets_bin: str = "datasets",
        debug: bool = False,
        sleep_func=None,
        runner=None,
    ) -> str:
        """Capture the preview input file used by auto mode."""

        del ncbi_api_key, datasets_bin, debug, sleep_func, runner
        preview_inputs.append(accession_file)
        preview_contents.append(accession_file.read_text(encoding="ascii"))
        assert include == "genome"
        assert accession_file.is_file()
        assert accession_file.parent.name.startswith("gtdb_genomes_preview_")
        return "Package size: 1.0 GB\n"

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
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        fake_run_preview_command,
    )

    output_dir = tmp_path / "preview-input-file"
    exit_code = main(
        [
            "--gtdb-release",
            "202",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(preview_inputs) == 1
    assert preview_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not preview_inputs[0].exists()


def test_mixed_uba_real_run_records_failed_unsupported_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA runs should keep successes and audit skipped UBA rows."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the supported accession."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
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

    output_dir = tmp_path / "mixed-uba-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    unsupported_row = next(
        row for row in accession_maps if row["gtdb_accession"] == "UBA11131"
    )
    assert unsupported_row["final_accession"] == ""
    assert unsupported_row["accession_type_original"] == "unknown"
    assert unsupported_row["accession_type_final"] == ""
    assert unsupported_row["conversion_status"] == "failed_no_usable_accession"
    assert unsupported_row["download_batch"] == "UBA11131"
    assert unsupported_row["download_status"] == "failed"

    taxon_header, taxon_rows = parse_tsv(
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv",
    )
    taxon_maps = [dict(zip(taxon_header, row, strict=True)) for row in taxon_rows]
    unsupported_taxon_row = next(
        row for row in taxon_maps if row["gtdb_accession"] == "UBA11131"
    )
    assert unsupported_taxon_row["final_accession"] == ""
    assert unsupported_taxon_row["download_status"] == "failed"
    assert unsupported_taxon_row["duplicate_across_taxa"] == "false"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["gtdb_accession"] == "UBA11131"
    assert failure["attempted_accession"] == "UBA11131"
    assert failure["final_accession"] == ""
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["final_status"] == "unsupported_input"
    assert "PRJNA417962" in failure["error_message_redacted"]


def test_uba_only_real_run_writes_failed_manifests_and_exits_seven(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only real runs should skip downloads but still write audit manifests."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: (_ for _ in ()).throw(
            AssertionError("preflight should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supported download execution should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["gtdb_accession"] == "UBA11131"
    assert accession_map["final_accession"] == ""
    assert accession_map["download_method_used"] == "auto"
    assert accession_map["download_status"] == "failed"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["final_status"] == "unsupported_input"

    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_method_used"] == "auto"
    assert run_summary["download_concurrency_used"] == "0"


def test_mixed_real_run_writes_zero_match_taxon_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed runs should still emit output rows for requested zero-match taxa."""

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
        lambda *args, **kwargs: SummaryLookupResult(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the matched taxon."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
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

    output_dir = tmp_path / "mixed-zero-match-real"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    taxon_summary_header, taxon_summary_rows = parse_tsv(output_dir / "taxon_summary.tsv")
    taxon_summaries = [
        dict(zip(taxon_summary_header, row, strict=True))
        for row in taxon_summary_rows
    ]
    assert [row["requested_taxon"] for row in taxon_summaries] == [
        "g__Escherichia",
        "g__Bacillus",
    ]
    bacillus_summary = next(
        row for row in taxon_summaries if row["requested_taxon"] == "g__Bacillus"
    )
    assert bacillus_summary["matched_rows"] == "0"
    assert bacillus_summary["unique_gtdb_accessions"] == "0"
    assert bacillus_summary["final_accessions"] == "0"
    assert bacillus_summary["successful_accessions"] == "0"
    assert bacillus_summary["failed_accessions"] == "0"

    bacillus_manifest = output_dir / "taxa" / "g__Bacillus" / "taxon_accessions.tsv"
    assert bacillus_manifest.exists()
    manifest_header, manifest_rows = parse_tsv(bacillus_manifest)
    assert manifest_header == [
        "requested_taxon",
        "taxon_slug",
        "lineage",
        "gtdb_accession",
        "final_accession",
        "conversion_status",
        "output_relpath",
        "download_status",
        "duplicate_across_taxa",
    ]
    assert manifest_rows == []


def test_real_run_output_copy_failure_returns_exit_code_eight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Local output-copy failures should return a stable workflow exit code."""

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
        lambda *args, **kwargs: SummaryLookupResult(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_outputs.copy_accession_payload",
        lambda source_directory, destination_directory: (_ for _ in ()).throw(
            PermissionError("disk full"),
        ),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution before output copying fails."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
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

    output_dir = tmp_path / "output-copy-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 8
    assert "Real-run output materialisation failed: disk full" in log_stream.getvalue()
    assert not (output_dir / "run_summary.tsv").exists()


def test_shared_preferred_direct_manifest_uses_preferred_download_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Shared preferred direct success should record the preferred download batch."""

    payload_directory = tmp_path / "shared-preferred-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one shared preferred direct-success result."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCA_001881595.3",
                    conversion_status="paired_to_gca",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession="GCA_001881595.3",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
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

    output_dir = tmp_path / "shared-preferred-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    assert {row["download_batch"] for row in accession_maps} == {"direct_batch_1"}


def test_direct_fallback_manifest_uses_actual_fallback_download_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback rows should record the final per-accession direct batch."""

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one fallback success and one preferred-group failure."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
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
                            error_message="preferred failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595.3",
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

    output_dir = tmp_path / "shared-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = {
        row["gtdb_accession"]: row
        for row in (
            dict(zip(accession_header, values, strict=True))
            for values in accession_rows
        )
    }
    assert accession_maps["RS_GCF_001881595.2"]["download_batch"] == "direct_fallback_batch_1"
    assert accession_maps["GB_GCA_001881595.3"]["download_batch"] == "direct_batch_1"


def test_failure_manifest_collapses_shared_accession_taxa(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """One shared failed accession should yield one root failure row."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia;s__Escherichia coli",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
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
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
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
            download_batch="GCF_000001.1",
            payload_directory=None,
            failures=(),
        ),
        "GCF_000002.1": AccessionExecution(
            original_accession="GCF_000002.1",
            final_accession="GCF_000002.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="GCF_000002.1",
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

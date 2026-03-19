"""Contract-level edge-case tests for the integrated workflow."""

from __future__ import annotations

import io
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
from gtdb_genomes.release_resolver import resolve_release
from gtdb_genomes.taxonomy import load_release_taxonomy
from gtdb_genomes.workflow import (
    AccessionExecution,
    AccessionPlan,
    DownloadExecutionResult,
    build_unsupported_uba_warning,
    build_failure_rows,
    execute_batch_dehydrate_plans,
    execute_direct_accession_plans,
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


def build_mixed_uba_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with one supported and one legacy UBA accession."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1", "UBA11131"],
            "lineage": [lineage, lineage],
            "ncbi_accession": ["GCF_000001.1", "UBA11131"],
            "taxonomy_file": [
                "bac120_taxonomy_r80.tsv",
                "bac120_taxonomy_r80.tsv",
            ],
        },
    )


def build_multi_accession_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with repeated and distinct supported accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "RS_GCF_000001.1_copy",
                "RS_GCF_000002.1",
            ],
            "lineage": [lineage, lineage, lineage],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCF_000001.1",
                "GCF_000002.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
            ],
        },
    )


def build_uba_only_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame containing only unsupported UBA accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["UBA11131"],
            "lineage": [lineage],
            "ncbi_accession": ["UBA11131"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv"],
        },
    )


def install_capture_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> io.StringIO:
    """Patch workflow logging to capture warning text for assertions."""

    stream = io.StringIO()

    def fake_configure_logging(
        debug: bool = False,
        dry_run: bool = False,
        output_root: Path | None = None,
    ) -> tuple[logging.Logger, Path | None]:
        """Return a predictable test logger backed by one string buffer."""

        del dry_run, output_root
        logger = logging.getLogger(f"test-workflow-{id(stream)}")
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
        return logger, None

    def fake_close_logger(logger: logging.Logger) -> None:
        """Flush and detach handlers without closing the shared string buffer."""

        for handler in tuple(logger.handlers):
            handler.flush()
            logger.removeHandler(handler)

    monkeypatch.setattr(
        "gtdb_genomes.workflow.configure_logging",
        fake_configure_logging,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.close_logger",
        fake_close_logger,
    )
    return stream


def parse_tsv(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return the header and rows from a TSV output file."""

    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[1:]]
    return header, rows


def build_cli_args(output_dir: Path) -> CliArgs:
    """Build a minimal CLI argument object for workflow unit tests."""

    return CliArgs(
        release="80",
        taxa=("s__Escherichia coli",),
        output=output_dir,
        prefer_genbank=True,
        download_method="direct",
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )


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


def test_mixed_uba_dry_run_warns_once_and_skips_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA dry-runs should warn once and exit cleanly."""

    warning_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview should not run"),
        ),
    )

    output_dir = tmp_path / "mixed-uba-dry-run"
    exit_code = main(
        [
            "--release",
            "80",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
            "--no-prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text
    assert "GCF_000001.1" not in warning_text


def test_uba_only_dry_run_warns_once_and_skips_ncbi_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only dry-runs should warn and avoid metadata or preview calls."""

    warning_stream = install_capture_logger(monkeypatch)
    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-dry-run"
    exit_code = main(
        [
            "--release",
            "80",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
            "--no-prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text


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


def test_release_80_contains_the_real_shared_preferred_accession_pair() -> None:
    """Release 80 should retain the known GCF/GCA duplicate pair."""

    taxonomy_frame = load_release_taxonomy(resolve_release("80"))
    selected = taxonomy_frame.filter(
        pl.col("ncbi_accession").is_in(
            ["GCF_001881595.2", "GCA_001881595.3"],
        ),
    )

    assert selected.select("ncbi_accession").rows() == [
        ("GCF_001881595.2",),
        ("GCA_001881595.3",),
    ]


def test_direct_mode_downloads_shared_preferred_accession_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Two originals that share one preferred accession should download once."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[str, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful preferred download for the shared group."""

        del command, final_failure_status, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_download_payload(
        accession: str,
        archive_path: Path,
        run_directories,
    ) -> tuple[Path | None, tuple[CommandFailureRecord, ...]]:
        """Return one shared payload directory for the preferred accession."""

        del run_directories
        extraction_calls.append((accession, archive_path))
        return payload_directory, ()

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_download_payload",
        fake_extract_download_payload,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-shared-success")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                preferred_accession="GCA_001881595.3",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                preferred_accession="GCA_001881595.3",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-shared-success"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [("preferred_download", "GCA_001881595.3")]
    assert extraction_calls == [
        (
            "GCA_001881595.3",
            run_directories.downloads_root / "GCA_001881595.3.zip",
        ),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCA_001881595.3"
    assert result.executions["GCF_001881595.2"].download_status == "downloaded"
    assert result.executions["GCA_001881595.3"].final_accession == "GCA_001881595.3"
    assert result.executions["GCA_001881595.3"].download_status == "downloaded"


def test_direct_mode_falls_back_per_original_after_shared_preferred_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A shared preferred failure should trigger fallback only where needed."""

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[str, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return a failed shared preferred download and one fallback success."""

        del command, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        if stage == "preferred_download":
            return RetryableCommandResult(
                succeeded=False,
                stdout="",
                stderr="preferred failed",
                failures=(
                    CommandFailureRecord(
                        stage="preferred_download",
                        attempt_index=4,
                        max_attempts=4,
                        error_type="subprocess",
                        error_message="preferred failed",
                        final_status=final_failure_status,
                        attempted_accession=attempted_accession,
                    ),
                ),
            )
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_download_payload(
        accession: str,
        archive_path: Path,
        run_directories,
    ) -> tuple[Path | None, tuple[CommandFailureRecord, ...]]:
        """Return a payload only for the original-accession fallback download."""

        del run_directories
        extraction_calls.append((accession, archive_path))
        return payload_directory, ()

    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.extract_download_payload",
        fake_extract_download_payload,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-shared-fallback")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                preferred_accession="GCA_001881595.3",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                preferred_accession="GCA_001881595.3",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-shared-fallback"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [
        ("preferred_download", "GCA_001881595.3"),
        ("fallback_download", "GCF_001881595.2"),
    ]
    assert extraction_calls == [
        (
            "GCF_001881595.2",
            run_directories.downloads_root / "GCF_001881595.2.zip",
        ),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCF_001881595.2"
    assert (
        result.executions["GCF_001881595.2"].download_status
        == "downloaded_after_fallback"
    )
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_status == "failed"


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
        assert str(accession_file).startswith("/tmp/gtdb_genomes_preview_")
        return "Package size: 1.0 GB\n"

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_multi_accession_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_preview_command",
        fake_run_preview_command,
    )

    output_dir = tmp_path / "preview-input-file"
    exit_code = main(
        [
            "--release",
            "202",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "auto",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(preview_inputs) == 1
    assert preview_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not preview_inputs[0].exists()


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
        assert str(accession_file).startswith("/tmp/gtdb_genomes_metadata_")
        return SummaryLookupResult(summary_map={}, failures=())

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_multi_accession_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    output_dir = tmp_path / "metadata-input-file"
    exit_code = main(
        [
            "--release",
            "202",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
            "--prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert len(metadata_inputs) == 1
    assert metadata_contents == ["GCF_000001.1\nGCF_000002.1\n"]
    assert not metadata_inputs[0].exists()


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


def test_mixed_uba_real_run_records_failed_unsupported_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA runs should keep successes and audit skipped UBA rows."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )

    def fake_execute_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        decision_method: str,
        run_directories,
        logger,
        secrets: tuple[str, ...],
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
                    payload_directory=payload_directory,
                    failures=(),
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

    output_dir = tmp_path / "mixed-uba-real"
    exit_code = main(
        [
            "--release",
            "80",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
            "--no-prefer-genbank",
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
        "gtdb_genomes.cli.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.execute_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supported download execution should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-real"
    exit_code = main(
        [
            "--release",
            "80",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(output_dir),
            "--download-method",
            "direct",
            "--no-prefer-genbank",
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["gtdb_accession"] == "UBA11131"
    assert accession_map["final_accession"] == ""
    assert accession_map["download_method_used"] == "direct"
    assert accession_map["download_status"] == "failed"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["final_status"] == "unsupported_input"

    run_summary_header, run_summary_rows = parse_tsv(output_dir / "run_summary.tsv")
    run_summary = dict(zip(run_summary_header, run_summary_rows[0], strict=True))
    assert run_summary["download_method_used"] == "direct"
    assert run_summary["download_concurrency_used"] == "0"


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
        ncbi_api_key=None,
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

"""Contract-level edge-case tests for workflow entrypoints and preflight."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.layout import (
    ACCESSION_MAP_COLUMNS,
    DUPLICATED_GENOMES_COLUMNS,
    TAXON_ACCESSION_COLUMNS,
)
from gtdb_genomes.metadata import MetadataLookupError, SummaryLookupResult
from gtdb_genomes.preflight import PreflightError
from tests.workflow_contract_helpers import (
    build_mixed_uba_taxonomy_frame,
    build_taxonomy_frame,
    build_uba_only_taxonomy_frame,
    install_fake_release_resolution,
    install_capture_logger,
    parse_summary_log,
)


@pytest.fixture(autouse=True)
def fake_release_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep workflow entrypoint tests independent of generated checkout data."""

    install_fake_release_resolution(monkeypatch)


def test_zero_match_run_writes_header_only_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Zero matches should create the documented output tree and exit 4."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: (_ for _ in ()).throw(
            AssertionError("preflight should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame("d__Bacteria;p__Firmicutes;g__Bacillus"),
    )

    output_dir = tmp_path / "zero-match"
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

    assert exit_code == 4
    assert (output_dir / "run_summary.log").exists()
    run_summary = parse_summary_log(output_dir / "run_summary.log")
    assert run_summary["exit_code"] == "4"
    assert (output_dir / "accession_map.tsv").read_text().splitlines() == [
        "\t".join(ACCESSION_MAP_COLUMNS),
    ]
    assert (output_dir / "duplicated_genomes.tsv").read_text().splitlines() == [
        "\t".join(DUPLICATED_GENOMES_COLUMNS),
    ]
    assert (
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv"
    ).read_text().splitlines() == ["\t".join(TAXON_ACCESSION_COLUMNS)]


def test_mixed_uba_dry_run_warns_once_and_skips_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA dry-runs should warn once and exit cleanly."""

    warning_stream = install_capture_logger(monkeypatch)
    required_tools_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: required_tools_calls.append(tuple(required_tools)),
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

    output_dir = tmp_path / "mixed-uba-dry-run"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",), ("datasets",)]
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text
    assert "GCF_000001.1" not in warning_text


def test_uba_only_dry_run_warns_once_and_skips_ncbi_calls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only dry-runs should warn and avoid metadata calls."""

    warning_stream = install_capture_logger(monkeypatch)
    required_tools_calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: required_tools_calls.append(tuple(required_tools)),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    output_dir = tmp_path / "uba-only-dry-run"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",)]
    warning_text = warning_stream.getvalue()
    assert warning_text.count("unsupported legacy GTDB UBA accessions") == 1
    assert "PRJNA417962" in warning_text


def test_zero_match_dry_run_missing_unzip_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should fail early when `unzip` is unavailable."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame("d__Bacteria;p__Firmicutes;g__Bacillus"),
    )

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail on the new early dry-run unzip check."""

        assert required_tools == ("unzip",)
        raise PreflightError("Missing required external tools: unzip")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "zero-match-dry-run-preflight"
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

    assert exit_code == 5
    assert not output_dir.exists()


def test_dry_run_unsupported_unzip_version_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dry-runs should fail early when unzip is outside the supported range."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.preflight.shutil.which",
        lambda tool_name: f"/usr/bin/{tool_name}",
    )

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return an unsupported unzip version during entrypoint preflight."""

        del capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="UnZip 7.00 of 20 April 2009\n",
            stderr="",
        )

    monkeypatch.setattr("gtdb_genomes.preflight.subprocess.run", fake_run)

    output_dir = tmp_path / "unsupported-unzip-version"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()


def test_supported_prefer_genbank_dry_run_missing_tools_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Supported prefer-GenBank dry-runs should still enforce datasets."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )

    required_tools_calls: list[tuple[str, ...]] = []

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail when the workflow reaches the supported dry-run datasets check."""

        required_tools_calls.append(required_tools)
        if required_tools == ("unzip",):
            return None
        assert required_tools == ("datasets",)
        raise PreflightError("Missing required external tools: datasets")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "prefer-genbank-dry-run-preflight"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--prefer-genbank",
            "--dry-run",
        ],
    )

    assert exit_code == 5
    assert not output_dir.exists()
    assert required_tools_calls == [("unzip",), ("datasets",)]


def test_supported_real_run_missing_tools_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Supported non-dry runs should still enforce datasets and unzip."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Fail when the workflow reaches the supported real-run path."""

        assert required_tools == ("datasets", "unzip")
        raise PreflightError("Missing required external tools: datasets, unzip")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        raise_preflight_error,
    )

    output_dir = tmp_path / "real-run-preflight"
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

    assert exit_code == 5
    assert not output_dir.exists()


def test_supported_prefer_genbank_total_metadata_failure_falls_back_in_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Total metadata lookup failure should still allow a dry-run to complete."""

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
        lambda *args, **kwargs: (_ for _ in ()).throw(
            MetadataLookupError("metadata lookup failed"),
        ),
    )

    output_dir = tmp_path / "prefer-genbank-metadata-failure"
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
    assert not output_dir.exists()


def test_real_run_initial_output_directory_failure_returns_exit_eight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Early real-run output directory failures should stay user-facing."""

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
    monkeypatch.setattr(
        "gtdb_genomes.workflow.initialise_run_directories",
        lambda output_root: (_ for _ in ()).throw(PermissionError("permission denied")),
    )

    output_dir = tmp_path / "initial-output-failure"
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
    assert "Real-run output materialisation failed: permission denied" in (
        log_stream.getvalue()
    )
    assert not output_dir.exists()


def test_unexpected_execution_failure_returns_exit_nine_and_cleans_workdir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unexpected execution bugs should return exit 9 with cleanup."""

    log_stream = install_capture_logger(monkeypatch)
    cleanup_calls: list[Path] = []
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
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("execution bug")),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.cleanup_working_directories",
        lambda run_directories: cleanup_calls.append(run_directories.working_root) or None,
    )

    output_dir = tmp_path / "unexpected-execution-failure"
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

    assert exit_code == 9
    assert cleanup_calls == [output_dir / ".gtdb_genomes_work"]
    assert "Unexpected internal failure (RuntimeError): execution bug" in (
        log_stream.getvalue()
    )


def test_planning_staging_failure_returns_exit_seven(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Planning-stage local staging failures should not be reported as output failures."""

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
        "gtdb_genomes.workflow_planning.create_staging_directory",
        lambda prefix: (_ for _ in ()).throw(PermissionError("permission denied")),
    )

    output_dir = tmp_path / "planning-staging-failure"
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

    assert exit_code == 7
    assert "Workflow planning failed due to local staging error: permission denied" in (
        log_stream.getvalue()
    )
    assert not output_dir.exists()

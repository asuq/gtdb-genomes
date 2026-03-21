"""Contract-level edge-case tests for workflow entrypoints and preflight."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.layout import (
    ACCESSION_MAP_COLUMNS,
    RUN_SUMMARY_COLUMNS,
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
    assert (output_dir / "run_summary.tsv").exists()
    assert (output_dir / "run_summary.tsv").read_text().splitlines()[0].split(
        "\t",
    ) == list(RUN_SUMMARY_COLUMNS)
    assert (output_dir / "accession_map.tsv").read_text().splitlines() == [
        "\t".join(ACCESSION_MAP_COLUMNS),
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
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: "Package size: 1.0 GB\n",
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
    """UBA-only dry-runs should warn and avoid metadata or preview calls."""

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
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_preview_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("preview should not run"),
        ),
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


def test_supported_prefer_genbank_total_metadata_failure_returns_exit_five(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Total metadata lookup failure should now fail the workflow explicitly."""

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
        ],
    )

    assert exit_code == 5

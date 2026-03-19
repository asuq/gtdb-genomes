"""Contract-level edge-case tests for the integrated workflow."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.download import CommandFailureRecord, PreviewError
from gtdb_genomes.workflow import AccessionExecution


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
    monkeypatch.setattr("gtdb_genomes.workflow.run_summary_lookup", lambda *args, **kwargs: {})
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
    monkeypatch.setattr("gtdb_genomes.workflow.run_summary_lookup", lambda *args, **kwargs: {})

    def fake_execute_accession_plans(*args, **kwargs) -> dict[str, AccessionExecution]:
        """Return a failed accession execution for the synthetic run."""

        return {
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
        }

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

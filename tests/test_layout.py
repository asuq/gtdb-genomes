"""Tests for output layout and manifest writing."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gtdb_genomes.layout import (
    ACCESSION_MAP_COLUMNS,
    DOWNLOAD_FAILURE_COLUMNS,
    LayoutError,
    RUN_SUMMARY_COLUMNS,
    TAXON_ACCESSION_COLUMNS,
    build_unzip_command,
    copy_accession_payload,
    extract_archive,
    get_duplicate_accessions,
    initialise_run_directories,
    write_root_manifests,
    write_zero_match_outputs,
)


def test_initialise_run_directories_creates_working_tree(tmp_path: Path) -> None:
    """Run-directory initialisation should create the documented tree."""

    run_directories = initialise_run_directories(tmp_path / "output")

    assert run_directories.output_root.is_dir()
    assert run_directories.taxa_root.is_dir()
    assert run_directories.working_root.is_dir()
    assert run_directories.downloads_root.is_dir()
    assert run_directories.extracted_root.is_dir()


def test_extract_archive_uses_unzip_runner(tmp_path: Path) -> None:
    """Archive extraction should call unzip with the expected argv layout."""

    commands: list[list[str]] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Record the command and pretend extraction succeeded."""

        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    destination = extract_archive(
        tmp_path / "archive.zip",
        tmp_path / "out",
        runner=runner,
    )

    assert commands == [build_unzip_command(tmp_path / "archive.zip", tmp_path / "out")]
    assert destination == tmp_path / "out"


def test_extract_archive_raises_layout_error_on_failure(tmp_path: Path) -> None:
    """Archive extraction failures should raise a layout error."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake unzip failure."""

        return subprocess.CompletedProcess(command, 1, stdout="", stderr="unzip failed")

    with pytest.raises(LayoutError, match="unzip failed"):
        extract_archive(tmp_path / "archive.zip", tmp_path / "out", runner=runner)


def test_extract_archive_raises_layout_error_on_spawn_failure(tmp_path: Path) -> None:
    """Archive extraction should report missing unzip as a layout error."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a missing-executable error before extraction starts."""

        raise FileNotFoundError("unzip")

    with pytest.raises(LayoutError, match="archive extraction command could not start"):
        extract_archive(tmp_path / "archive.zip", tmp_path / "out", runner=runner)


def test_write_root_manifests_and_zero_match_outputs(tmp_path: Path) -> None:
    """Writers should emit fixed headers even when the rows are empty."""

    run_directories = initialise_run_directories(tmp_path / "output")
    write_root_manifests(
        run_directories,
        [{"run_id": "run-1", "exit_code": 4}],
        [],
        [],
        [],
    )
    write_zero_match_outputs(
        run_directories,
        ("g__Escherichia", "s__Escherichia coli"),
        {
            "g__Escherichia": "g__Escherichia",
            "s__Escherichia coli": "s__Escherichia_coli",
        },
        [{"run_id": "run-1", "exit_code": 4}],
        [],
    )

    run_summary_lines = (
        run_directories.output_root / "run_summary.tsv"
    ).read_text().splitlines()
    accession_map_lines = (
        run_directories.output_root / "accession_map.tsv"
    ).read_text().splitlines()
    failure_lines = (
        run_directories.output_root / "download_failures.tsv"
    ).read_text().splitlines()
    taxon_lines = (
        run_directories.taxa_root / "g__Escherichia" / "taxon_accessions.tsv"
    ).read_text().splitlines()

    assert run_summary_lines[0].split("\t") == list(RUN_SUMMARY_COLUMNS)
    assert accession_map_lines == ["\t".join(ACCESSION_MAP_COLUMNS)]
    assert failure_lines == ["\t".join(DOWNLOAD_FAILURE_COLUMNS)]
    assert taxon_lines == ["\t".join(TAXON_ACCESSION_COLUMNS)]


def test_copy_accession_payload_and_duplicate_detection(tmp_path: Path) -> None:
    """Payload copying and duplicate detection should follow taxon semantics."""

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    (source_directory / "genome.fna").write_text(">seq\nACGT\n")

    destination_directory = tmp_path / "dest"
    copied = copy_accession_payload(source_directory, destination_directory)

    assert copied == destination_directory
    assert (destination_directory / "genome.fna").read_text() == ">seq\nACGT\n"
    assert get_duplicate_accessions(
        [
            {"taxon_slug": "g__Escherichia", "final_accession": "GCA_1"},
            {"taxon_slug": "s__Escherichia_coli", "final_accession": "GCA_1"},
            {"taxon_slug": "g__Bacillus", "final_accession": "GCA_2"},
        ],
    ) == {"GCA_1"}

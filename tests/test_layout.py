"""Tests for output layout and manifest writing."""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path
import stat

import pytest

from gtdb_genomes.layout import (
    ACCESSION_MAP_COLUMNS,
    DOWNLOAD_FAILURE_COLUMNS,
    DUPLICATED_GENOMES_COLUMNS,
    LayoutError,
    RUN_SUMMARY_KEYS,
    TAXON_ACCESSION_COLUMNS,
    build_unzip_command,
    copy_accession_payload,
    extract_archive,
    get_duplicate_accessions,
    initialise_run_directories,
    move_accession_payload,
    write_root_manifests,
    write_zero_match_outputs,
)
from tests.workflow_contract_helpers import parse_summary_log


def write_test_archive(
    archive_path: Path,
    members: dict[str, str],
) -> None:
    """Write one zip fixture with plain-text file members."""

    with zipfile.ZipFile(archive_path, "w") as handle:
        for member_name, member_text in members.items():
            handle.writestr(member_name, member_text)


def write_symlink_archive(archive_path: Path, member_name: str) -> None:
    """Write one zip fixture that contains a symbolic-link member."""

    symlink_info = zipfile.ZipInfo(member_name)
    symlink_info.create_system = 3
    symlink_info.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive_path, "w") as handle:
        handle.writestr(symlink_info, "target")


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
    archive_path = tmp_path / "archive.zip"
    write_test_archive(
        archive_path,
        {"ncbi_dataset/data/GCF_000001.1/genome.fna": ">seq\nACGT\n"},
    )

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
        archive_path,
        tmp_path / "out",
        runner=runner,
    )

    assert commands == [build_unzip_command(archive_path, tmp_path / "out")]
    assert destination == tmp_path / "out"


def test_extract_archive_raises_layout_error_on_failure(tmp_path: Path) -> None:
    """Archive extraction failures should raise a layout error."""

    archive_path = tmp_path / "archive.zip"
    write_test_archive(
        archive_path,
        {"ncbi_dataset/data/GCF_000001.1/genome.fna": ">seq\nACGT\n"},
    )

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
        extract_archive(archive_path, tmp_path / "out", runner=runner)


def test_extract_archive_raises_layout_error_on_spawn_failure(tmp_path: Path) -> None:
    """Archive extraction should report missing unzip as a layout error."""

    archive_path = tmp_path / "archive.zip"
    write_test_archive(
        archive_path,
        {"ncbi_dataset/data/GCF_000001.1/genome.fna": ">seq\nACGT\n"},
    )

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
        extract_archive(archive_path, tmp_path / "out", runner=runner)


@pytest.mark.parametrize(
    ("member_name", "error_fragment"),
    (
        ("/absolute/path.txt", "absolute member path"),
        ("../escape.txt", "parent-traversing member path"),
        ("C:/drive-rooted.txt", "drive-rooted member path"),
        ("", "empty member name"),
    ),
)
def test_extract_archive_rejects_unsafe_member_paths(
    tmp_path: Path,
    member_name: str,
    error_fragment: str,
) -> None:
    """Archive extraction should reject unsafe member paths before unzip runs."""

    archive_path = tmp_path / "archive.zip"
    write_test_archive(archive_path, {member_name: "payload"})

    with pytest.raises(LayoutError, match=error_fragment):
        extract_archive(archive_path, tmp_path / "out")


def test_extract_archive_rejects_symbolic_link_members(tmp_path: Path) -> None:
    """Archive extraction should reject symbolic-link members before unzip runs."""

    archive_path = tmp_path / "archive.zip"
    write_symlink_archive(archive_path, "ncbi_dataset/data/link")

    with pytest.raises(LayoutError, match="symbolic link member"):
        extract_archive(archive_path, tmp_path / "out")


def test_write_root_manifests_and_zero_match_outputs(tmp_path: Path) -> None:
    """Writers should emit fixed headers even when the rows are empty."""

    run_directories = initialise_run_directories(tmp_path / "output")
    write_root_manifests(
        run_directories,
        "Run Identity\nrun_id: run-1\nexit_code: 4\n",
        [],
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
        "Run Identity\nrun_id: run-1\nexit_code: 4\n",
        [],
    )

    run_summary = parse_summary_log(run_directories.output_root / "run_summary.log")
    accession_map_lines = (
        run_directories.output_root / "accession_map.tsv"
    ).read_text().splitlines()
    failure_lines = (
        run_directories.output_root / "download_failures.tsv"
    ).read_text().splitlines()
    duplicate_lines = (
        run_directories.output_root / "duplicated_genomes.tsv"
    ).read_text().splitlines()
    taxon_lines = (
        run_directories.taxa_root / "g__Escherichia" / "taxon_accessions.tsv"
    ).read_text().splitlines()

    assert set(run_summary) <= set(RUN_SUMMARY_KEYS)
    assert run_summary["run_id"] == "run-1"
    assert run_summary["exit_code"] == "4"
    assert accession_map_lines == ["\t".join(ACCESSION_MAP_COLUMNS)]
    assert failure_lines == ["\t".join(DOWNLOAD_FAILURE_COLUMNS)]
    assert duplicate_lines == ["\t".join(DUPLICATED_GENOMES_COLUMNS)]
    assert taxon_lines == ["\t".join(TAXON_ACCESSION_COLUMNS)]


def test_copy_and_move_accession_payload_and_duplicate_detection(
    tmp_path: Path,
) -> None:
    """Payload transfer helpers and duplicate detection should follow taxon semantics."""

    source_directory = tmp_path / "source"
    source_directory.mkdir()
    (source_directory / "genome.fna").write_text(">seq\nACGT\n")

    copied_directory = tmp_path / "copied"
    copied = copy_accession_payload(source_directory, copied_directory)

    assert copied == copied_directory
    assert (copied_directory / "genome.fna").read_text() == ">seq\nACGT\n"
    assert (source_directory / "genome.fna").read_text() == ">seq\nACGT\n"

    moved_source_directory = tmp_path / "move-source"
    moved_source_directory.mkdir()
    (moved_source_directory / "genome.fna").write_text(">seq\nTGCA\n")
    moved_directory = tmp_path / "moved"
    moved_directory.mkdir()
    (moved_directory / "stale.txt").write_text("stale\n")
    moved = move_accession_payload(moved_source_directory, moved_directory)

    assert moved == moved_directory
    assert not moved_source_directory.exists()
    assert not (moved_directory / "stale.txt").exists()
    assert (moved_directory / "genome.fna").read_text() == ">seq\nTGCA\n"
    assert get_duplicate_accessions(
        [
            {"taxon_slug": "g__Escherichia", "final_accession": "GCA_1"},
            {"taxon_slug": "s__Escherichia_coli", "final_accession": "GCA_1"},
            {"taxon_slug": "g__Bacillus", "final_accession": "GCA_2"},
        ],
    ) == {"GCA_1"}

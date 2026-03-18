"""Output layout, working directories, and archive extraction."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess


@dataclass(slots=True)
class LayoutError(Exception):
    """Raised when the output layout cannot be created or populated."""

    message: str

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


@dataclass(slots=True)
class RunDirectories:
    """Filesystem layout for one tool run."""

    output_root: Path
    taxa_root: Path
    working_root: Path
    downloads_root: Path
    extracted_root: Path


def initialise_run_directories(output_root: Path) -> RunDirectories:
    """Create the run output and internal working directories."""

    taxa_root = output_root / "taxa"
    working_root = output_root / ".gtdb_genomes_work"
    downloads_root = working_root / "downloads"
    extracted_root = working_root / "extracted"
    for directory in (
        output_root,
        taxa_root,
        working_root,
        downloads_root,
        extracted_root,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    return RunDirectories(
        output_root=output_root,
        taxa_root=taxa_root,
        working_root=working_root,
        downloads_root=downloads_root,
        extracted_root=extracted_root,
    )


def build_unzip_command(archive_path: Path, destination: Path) -> list[str]:
    """Build the unzip command used for archive extraction."""

    return [
        "unzip",
        "-o",
        "-q",
        str(archive_path),
        "-d",
        str(destination),
    ]


def extract_archive(
    archive_path: Path,
    destination: Path,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Path:
    """Extract one datasets zip archive into the destination directory."""

    destination.mkdir(parents=True, exist_ok=True)
    command = build_unzip_command(archive_path, destination)
    result = runner(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        if not error_message:
            error_message = "archive extraction failed"
        raise LayoutError(error_message)
    return destination


def cleanup_working_directories(run_directories: RunDirectories) -> None:
    """Remove the internal working directory tree for a completed run."""

    if run_directories.working_root.exists():
        shutil.rmtree(run_directories.working_root)

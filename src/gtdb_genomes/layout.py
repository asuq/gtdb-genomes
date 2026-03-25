"""Output layout, working directories, and archive extraction."""

from __future__ import annotations

from collections.abc import Callable
import csv
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import zipfile

from gtdb_genomes.subprocess_utils import (
    DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    build_spawn_error_message,
    build_timeout_error_message,
)


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


RUN_SUMMARY_KEYS = (
    "run_id",
    "accession_decision_sha256",
    "started_at",
    "finished_at",
    "requested_release",
    "resolved_release",
    "download_method_requested",
    "download_method_used",
    "threads_requested",
    "download_concurrency_used",
    "rehydrate_workers_used",
    "include",
    "prefer_genbank",
    "version_latest",
    "package_version",
    "git_revision",
    "datasets_version",
    "unzip_version",
    "release_manifest_sha256",
    "bacterial_taxonomy_sha256",
    "archaeal_taxonomy_sha256",
    "debug_enabled",
    "requested_taxa_count",
    "matched_rows",
    "unique_gtdb_accessions",
    "successful_accessions",
    "failed_accessions",
    "output_dir",
    "exit_code",
)
TAXON_SUMMARY_COLUMNS = (
    "requested_taxon",
    "unique_gtdb_accessions",
    "successful_accessions",
    "failed_accessions",
    "duplicate_copies_written",
    "output_dir",
)
ACCESSION_MAP_COLUMNS = (
    "final_accession",
    "requested_taxa",
    "gtdb_accessions",
    "selected_accessions",
    "download_request_accessions",
    "conversion_status",
    "download_status",
    "output_relpaths",
    "duplicate_across_taxa",
)
DOWNLOAD_FAILURE_COLUMNS = (
    "accession",
    "requested_taxa",
    "gtdb_accessions",
    "suppressed",
    "stage",
    "error_type",
    "reason",
    "status",
)
DUPLICATED_GENOMES_COLUMNS = (
    "final_accession",
    "requested_taxa",
    "taxa_count",
    "output_relpaths",
)
TAXON_ACCESSION_COLUMNS = (
    "final_accession",
    "requested_taxon",
    "lineage",
    "gtdb_accession",
    "ncbi_accession",
    "selected_accession",
    "download_request_accession",
    "conversion_status",
    "output_relpath",
    "download_status",
    "duplicate_across_taxa",
)
WINDOWS_DRIVE_ROOT_PATTERN = re.compile(r"^[A-Za-z]:($|[\\/])")
RESERVED_OUTPUT_ARTEFACTS = (
    ".gtdb_genomes_work",
    "accession_map.tsv",
    "debug.log",
    "download_failures.tsv",
    "duplicated_genomes.tsv",
    "run_summary.log",
    "taxa",
    "taxon_summary.tsv",
)


def find_leftover_run_artefacts(output_root: Path) -> tuple[str, ...]:
    """Return the existing GTDB-genomes artefacts already present in one output root."""

    if not output_root.exists():
        return ()
    return tuple(
        sorted(
            artefact
            for artefact in RESERVED_OUTPUT_ARTEFACTS
            if (output_root / artefact).exists()
        ),
    )


def build_leftover_run_abort_message(
    output_root: Path,
    artefacts: tuple[str, ...],
) -> str:
    """Build one user-facing abort message for leftover run artefacts."""

    artefacts_text = "\n".join(f"  - {artefact}" for artefact in artefacts)
    return (
        "detected leftover gtdb-genomes output from a previous run in:\n"
        f"  {output_root}\n"
        "aborting because these artefacts already exist:\n"
        f"{artefacts_text}"
    )


def validate_output_root_available(output_root: Path) -> None:
    """Reject output roots that already contain GTDB-genomes run artefacts."""

    try:
        if output_root.exists():
            if not output_root.is_dir():
                raise LayoutError(
                    f"Output path must not be an existing file: {output_root}",
                )
            leftover_artefacts = find_leftover_run_artefacts(output_root)
            if leftover_artefacts:
                raise LayoutError(
                    build_leftover_run_abort_message(
                        output_root,
                        leftover_artefacts,
                    ),
                )
    except OSError as error:
        raise LayoutError(
            f"Could not inspect output path {output_root}: {error}",
        ) from error


def initialise_run_directories(output_root: Path) -> RunDirectories:
    """Create the run output and internal working directories."""

    validate_output_root_available(output_root)
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


def normalise_archive_member_name(member_name: str) -> str:
    """Normalise one archive member name for path-safety checks."""

    return member_name.replace("\\", "/")


def validate_archive_member_name(member_name: str) -> None:
    """Reject archive members whose names escape the extraction root."""

    if not member_name.strip():
        raise LayoutError("Archive contains an empty member name")
    normalised_name = normalise_archive_member_name(member_name)
    if normalised_name.startswith("/"):
        raise LayoutError(
            f"Archive contains an absolute member path: {member_name}",
        )
    if WINDOWS_DRIVE_ROOT_PATTERN.match(member_name):
        raise LayoutError(
            f"Archive contains a drive-rooted member path: {member_name}",
        )
    if any(part == ".." for part in PurePosixPath(normalised_name).parts):
        raise LayoutError(
            f"Archive contains a parent-traversing member path: {member_name}",
        )


def validate_archive_member_type(member_info: zipfile.ZipInfo) -> None:
    """Reject symlinks and other non-regular archive member types."""

    if member_info.is_dir():
        return
    mode = (member_info.external_attr >> 16) & 0o777777
    file_type = stat.S_IFMT(mode)
    if mode == 0 or file_type in (0, stat.S_IFREG):
        return
    if file_type == stat.S_IFLNK:
        raise LayoutError(
            f"Archive contains an unsupported symbolic link member: "
            f"{member_info.filename}",
        )
    raise LayoutError(
        f"Archive contains an unsupported non-regular member: "
        f"{member_info.filename}",
    )


def validate_archive_members(archive_path: Path) -> None:
    """Validate all member paths and types before extraction."""

    try:
        with zipfile.ZipFile(archive_path) as handle:
            for member_info in handle.infolist():
                validate_archive_member_name(member_info.filename)
                validate_archive_member_type(member_info)
    except (FileNotFoundError, zipfile.BadZipFile) as error:
        raise LayoutError(
            f"Could not inspect archive members in {archive_path}: {error}",
        ) from error


def extract_archive(
    archive_path: Path,
    destination: Path,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Path:
    """Extract one datasets zip archive into the destination directory."""

    validate_archive_members(archive_path)
    destination.mkdir(parents=True, exist_ok=True)
    command = build_unzip_command(archive_path, destination)
    try:
        result = runner(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as error:
        raise LayoutError(
            build_timeout_error_message(
                "archive_extraction",
                DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
            ),
        ) from error
    except OSError as error:
        raise LayoutError(build_spawn_error_message("archive_extraction", error)) from error
    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        if not error_message:
            error_message = "archive extraction failed"
        raise LayoutError(error_message)
    return destination


def cleanup_working_directories(
    run_directories: RunDirectories,
) -> OSError | None:
    """Remove the internal working directory tree and report cleanup errors."""

    if run_directories.working_root.exists():
        try:
            shutil.rmtree(run_directories.working_root)
        except OSError as error:
            return error
    return None


def get_root_manifest_paths(output_root: Path) -> dict[str, Path]:
    """Return the fixed root manifest paths for one output directory."""

    return {
        "run_summary": output_root / "run_summary.log",
        "taxon_summary": output_root / "taxon_summary.tsv",
        "accession_map": output_root / "accession_map.tsv",
        "download_failures": output_root / "download_failures.tsv",
        "duplicated_genomes": output_root / "duplicated_genomes.tsv",
    }


def get_taxon_directory(run_directories: RunDirectories, taxon_slug: str) -> Path:
    """Return the directory for one taxon slug, creating it if needed."""

    taxon_directory = run_directories.taxa_root / taxon_slug
    taxon_directory.mkdir(parents=True, exist_ok=True)
    return taxon_directory


def get_taxon_accession_path(
    run_directories: RunDirectories,
    taxon_slug: str,
) -> Path:
    """Return the per-taxon accession TSV path."""

    return get_taxon_directory(run_directories, taxon_slug) / "taxon_accessions.tsv"


def write_tsv_rows(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, object]],
) -> None:
    """Write rows to a TSV file, always emitting the header."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(columns),
            delimiter="\t",
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    column: "" if row.get(column) is None else row.get(column)
                    for column in columns
                },
            )


def write_text(path: Path, text: str) -> None:
    """Write one UTF-8 text file, creating parent directories as needed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_root_manifests(
    run_directories: RunDirectories,
    run_summary_text: str,
    taxon_summary_rows: list[dict[str, object]],
    accession_rows: list[dict[str, object]],
    failure_rows: list[dict[str, object]],
    duplicated_rows: list[dict[str, object]],
) -> None:
    """Write the fixed root manifests for one run."""

    manifest_paths = get_root_manifest_paths(run_directories.output_root)
    write_text(manifest_paths["run_summary"], run_summary_text)
    write_tsv_rows(
        manifest_paths["taxon_summary"],
        TAXON_SUMMARY_COLUMNS,
        taxon_summary_rows,
    )
    write_tsv_rows(
        manifest_paths["accession_map"],
        ACCESSION_MAP_COLUMNS,
        accession_rows,
    )
    write_tsv_rows(
        manifest_paths["download_failures"],
        DOWNLOAD_FAILURE_COLUMNS,
        failure_rows,
    )
    write_tsv_rows(
        manifest_paths["duplicated_genomes"],
        DUPLICATED_GENOMES_COLUMNS,
        duplicated_rows,
    )


def write_taxon_accessions(
    run_directories: RunDirectories,
    taxon_slug: str,
    rows: list[dict[str, object]],
) -> None:
    """Write one per-taxon accession TSV file."""

    write_tsv_rows(
        get_taxon_accession_path(run_directories, taxon_slug),
        TAXON_ACCESSION_COLUMNS,
        rows,
    )


def get_accession_output_directory(
    run_directories: RunDirectories,
    taxon_slug: str,
    accession: str,
) -> Path:
    """Return the final output directory for one accession inside one taxon."""

    return get_taxon_directory(run_directories, taxon_slug) / accession


def copy_accession_payload(
    source_directory: Path,
    destination_directory: Path,
) -> Path:
    """Copy one extracted accession payload into its final taxon directory."""

    if destination_directory.exists():
        shutil.rmtree(destination_directory)
    shutil.copytree(source_directory, destination_directory)
    return destination_directory


def move_accession_payload(
    source_directory: Path,
    destination_directory: Path,
) -> Path:
    """Move one extracted accession payload into its final taxon directory."""

    if destination_directory.exists():
        shutil.rmtree(destination_directory)
    destination_directory.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_directory), str(destination_directory))
    return destination_directory


def get_duplicate_accessions(accession_rows: list[dict[str, object]]) -> set[str]:
    """Return final accessions that occur in more than one requested taxon."""

    taxon_sets: dict[str, set[str]] = {}
    for row in accession_rows:
        final_accession = str(row.get("final_accession", "")).strip()
        taxon_slug = str(row.get("taxon_slug", "")).strip()
        if not final_accession or not taxon_slug:
            continue
        taxon_sets.setdefault(final_accession, set()).add(taxon_slug)
    return {
        accession
        for accession, taxon_slugs in taxon_sets.items()
        if len(taxon_slugs) > 1
    }


def write_zero_match_outputs(
    run_directories: RunDirectories,
    requested_taxa: tuple[str, ...],
    taxon_slug_map: dict[str, str],
    run_summary_text: str,
    taxon_summary_rows: list[dict[str, object]],
) -> None:
    """Write the documented zero-match output tree."""

    write_root_manifests(
        run_directories,
        run_summary_text,
        taxon_summary_rows,
        [],
        [],
        [],
    )
    for requested_taxon in requested_taxa:
        taxon_slug = taxon_slug_map[requested_taxon]
        write_taxon_accessions(run_directories, taxon_slug, [])

"""Output layout, working directories, and archive extraction."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import time
import zipfile


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
    "ncbi_options",
    "prefer_genbank",
    "version_latest",
    "package_version",
    "git_revision",
    "datasets_version",
    "release_manifest_sha256",
    "bacterial_taxonomy_sha256",
    "archaeal_taxonomy_sha256",
    "debug_enabled",
    "requested_taxa_count",
    "matched_rows",
    "unique_gtdb_accessions",
    "successful_accessions",
    "failed_accessions",
    "excluded_accessions",
    "output_dir",
    "exit_code",
)
TAXON_SUMMARY_COLUMNS = (
    "requested_taxon",
    "unique_gtdb_accessions",
    "successful_accessions",
    "failed_accessions",
    "excluded_accessions",
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
    "exclusion_reason",
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
    "exclusion_reason",
)
WINDOWS_DRIVE_ROOT_PATTERN = re.compile(r"^[A-Za-z]:($|[\\/])")
ARCHIVE_EXTRACTION_CHUNK_SIZE_BYTES = 1024 * 1024
DEFAULT_ARCHIVE_EXTRACTION_TIMEOUT_SECONDS = 4 * 60 * 60
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


def validate_archive_members(member_infos: list[zipfile.ZipInfo]) -> None:
    """Validate all archive member paths and types before extraction."""

    for member_info in member_infos:
        validate_archive_member_name(member_info.filename)
        validate_archive_member_type(member_info)


def build_archive_member_target(
    destination: Path,
    resolved_destination: Path,
    member_name: str,
) -> Path:
    """Return the validated extraction target for one archive member."""

    normalised_name = normalise_archive_member_name(member_name)
    target = destination.joinpath(*PurePosixPath(normalised_name).parts)
    resolved_target = target.resolve()
    if not resolved_target.is_relative_to(resolved_destination):
        raise LayoutError(
            f"Archive member resolves outside the extraction root: {member_name}",
        )
    return target


def check_archive_extraction_deadline(deadline: float, timeout_seconds: int) -> None:
    """Fail when native archive extraction exceeds its fixed deadline."""

    if time.monotonic() > deadline:
        raise LayoutError(
            f"Archive extraction timed out after {timeout_seconds} seconds",
        )


def extract_archive_member(
    handle: zipfile.ZipFile,
    member_info: zipfile.ZipInfo,
    destination: Path,
    resolved_destination: Path,
    *,
    deadline: float,
    timeout_seconds: int,
) -> None:
    """Extract one previously validated member with bounded-memory copying."""

    check_archive_extraction_deadline(deadline, timeout_seconds)
    target = build_archive_member_target(
        destination,
        resolved_destination,
        member_info.filename,
    )
    if member_info.is_dir():
        target.mkdir(parents=True, exist_ok=True)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    with handle.open(member_info, "r") as source, target.open("wb") as output:
        while chunk := source.read(ARCHIVE_EXTRACTION_CHUNK_SIZE_BYTES):
            check_archive_extraction_deadline(deadline, timeout_seconds)
            output.write(chunk)


def extract_archive(
    archive_path: Path,
    destination: Path,
    *,
    timeout_seconds: int = DEFAULT_ARCHIVE_EXTRACTION_TIMEOUT_SECONDS,
) -> Path:
    """Safely extract one datasets ZIP archive with the Python standard library."""

    deadline = time.monotonic() + timeout_seconds
    try:
        with zipfile.ZipFile(archive_path) as handle:
            member_infos = handle.infolist()
            validate_archive_members(member_infos)
            destination.mkdir(parents=True, exist_ok=True)
            resolved_destination = destination.resolve()
            for member_info in member_infos:
                extract_archive_member(
                    handle,
                    member_info,
                    destination,
                    resolved_destination,
                    deadline=deadline,
                    timeout_seconds=timeout_seconds,
                )
    except LayoutError:
        raise
    except (
        OSError,
        EOFError,
        RuntimeError,
        NotImplementedError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ) as error:
        raise LayoutError(
            f"Could not extract archive {archive_path}: {error}",
        ) from error
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


def remove_directory_if_empty(directory: Path) -> OSError | None:
    """Remove one directory only when it exists and is empty."""

    if not directory.exists() or not directory.is_dir():
        return None
    try:
        if any(directory.iterdir()):
            return None
        directory.rmdir()
    except OSError as error:
        return error
    return None


def prune_empty_run_output_directories(
    run_directories: RunDirectories,
) -> OSError | None:
    """Prune empty output directories after an interrupted run."""

    if run_directories.taxa_root.exists():
        try:
            taxon_directories = sorted(
                path for path in run_directories.taxa_root.iterdir() if path.is_dir()
            )
        except OSError as error:
            return error
        for taxon_directory in taxon_directories:
            prune_error = remove_directory_if_empty(taxon_directory)
            if prune_error is not None:
                return prune_error
    for directory in (
        run_directories.taxa_root,
        run_directories.output_root,
    ):
        prune_error = remove_directory_if_empty(directory)
        if prune_error is not None:
            return prune_error
    return None


def cleanup_interrupted_output_directories(
    run_directories: RunDirectories,
) -> OSError | None:
    """Remove the working tree and prune any empty output directories."""

    working_cleanup_error = cleanup_working_directories(run_directories)
    prune_error = prune_empty_run_output_directories(run_directories)
    if working_cleanup_error is not None:
        return working_cleanup_error
    return prune_error


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

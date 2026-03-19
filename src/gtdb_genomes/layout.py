"""Output layout, working directories, and archive extraction."""

from __future__ import annotations

from collections.abc import Callable
import csv
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


RUN_SUMMARY_COLUMNS = (
    "run_id",
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
    "debug_enabled",
    "requested_taxa_count",
    "matched_rows",
    "unique_gtdb_accessions",
    "final_accessions",
    "successful_accessions",
    "failed_accessions",
    "output_dir",
    "exit_code",
)
TAXON_SUMMARY_COLUMNS = (
    "requested_taxon",
    "taxon_slug",
    "matched_rows",
    "unique_gtdb_accessions",
    "final_accessions",
    "successful_accessions",
    "failed_accessions",
    "duplicate_copies_written",
    "output_dir",
)
ACCESSION_MAP_COLUMNS = (
    "requested_taxon",
    "taxon_slug",
    "resolved_release",
    "taxonomy_file",
    "lineage",
    "gtdb_accession",
    "final_accession",
    "accession_type_original",
    "accession_type_final",
    "conversion_status",
    "download_method_used",
    "download_batch",
    "output_relpath",
    "download_status",
)
DOWNLOAD_FAILURE_COLUMNS = (
    "requested_taxon",
    "taxon_slug",
    "gtdb_accession",
    "final_accession",
    "stage",
    "attempt_index",
    "max_attempts",
    "error_type",
    "error_message_redacted",
    "final_status",
)
TAXON_ACCESSION_COLUMNS = (
    "requested_taxon",
    "taxon_slug",
    "lineage",
    "gtdb_accession",
    "final_accession",
    "conversion_status",
    "output_relpath",
    "download_status",
    "duplicate_across_taxa",
)


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


def get_root_manifest_paths(output_root: Path) -> dict[str, Path]:
    """Return the fixed root TSV paths for one output directory."""

    return {
        "run_summary": output_root / "run_summary.tsv",
        "taxon_summary": output_root / "taxon_summary.tsv",
        "accession_map": output_root / "accession_map.tsv",
        "download_failures": output_root / "download_failures.tsv",
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


def write_root_manifests(
    run_directories: RunDirectories,
    run_summary_rows: list[dict[str, object]],
    taxon_summary_rows: list[dict[str, object]],
    accession_rows: list[dict[str, object]],
    failure_rows: list[dict[str, object]],
) -> None:
    """Write the fixed root TSV manifests for one run."""

    manifest_paths = get_root_manifest_paths(run_directories.output_root)
    write_tsv_rows(
        manifest_paths["run_summary"],
        RUN_SUMMARY_COLUMNS,
        run_summary_rows,
    )
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
    run_summary_rows: list[dict[str, object]],
    taxon_summary_rows: list[dict[str, object]],
) -> None:
    """Write the documented zero-match output tree."""

    write_root_manifests(
        run_directories,
        run_summary_rows,
        taxon_summary_rows,
        [],
        [],
    )
    for requested_taxon in requested_taxa:
        taxon_slug = taxon_slug_map[requested_taxon]
        write_taxon_accessions(run_directories, taxon_slug, [])

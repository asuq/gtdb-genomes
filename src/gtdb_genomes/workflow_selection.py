"""Selection and preflight helpers for the GTDB workflow."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import TYPE_CHECKING

import polars as pl

from gtdb_genomes.download import (
    CommandFailureRecord,
    DEFAULT_REQUESTED_DOWNLOAD_METHOD,
    get_ordered_unique_accessions,
)
from gtdb_genomes.layout import (
    cleanup_working_directories,
    initialise_run_directories,
    write_zero_match_outputs,
)
from gtdb_genomes.logging_utils import close_logger
from gtdb_genomes.preflight import check_required_tools, get_required_tools
from gtdb_genomes.release_resolver import ReleaseResolution, resolve_and_validate_release
from gtdb_genomes.selection import attach_taxon_slugs, build_taxon_slug_map, select_taxa
from gtdb_genomes.taxonomy import load_release_taxonomy
from gtdb_genomes.workflow_execution import AccessionExecution
from gtdb_genomes.workflow_outputs import build_run_summary_row, configure_output_logger

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


UNSUPPORTED_UBA_PREFIX = "UBA"
UNSUPPORTED_UBA_BIOPROJECT = "PRJNA417962"
UNSUPPORTED_UBA_WARNING_EXAMPLES = 5


# Selection and unsupported-accession helpers.


def is_unsupported_uba_accession(accession: str) -> bool:
    """Return whether one legacy GTDB accession starts with `UBA`."""

    return accession.startswith(UNSUPPORTED_UBA_PREFIX)


def count_unique_accessions(frame: pl.DataFrame) -> int:
    """Return the number of unique accession values in one selection frame."""

    if frame.is_empty():
        return 0
    return len(
        get_ordered_unique_accessions(
            frame.get_column("ncbi_accession").to_list(),
        ),
    )


def split_selected_rows_by_accession_support(
    selected_frame: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split selected rows into supported and unsupported accession groups."""

    if selected_frame.is_empty():
        return selected_frame, selected_frame
    unsupported_mask = pl.col("ncbi_accession").str.starts_with(
        UNSUPPORTED_UBA_PREFIX,
    )
    return (
        selected_frame.filter(~unsupported_mask),
        selected_frame.filter(unsupported_mask),
    )


def build_unsupported_uba_warning(unsupported_frame: pl.DataFrame) -> str:
    """Build the documented run-level warning for unsupported `UBA*` accessions."""

    unique_accessions = get_ordered_unique_accessions(
        unsupported_frame.get_column("ncbi_accession").to_list(),
    )
    affected_taxa = get_ordered_unique_accessions(
        unsupported_frame.get_column("requested_taxon").to_list(),
    )
    example_text = ", ".join(unique_accessions[:UNSUPPORTED_UBA_WARNING_EXAMPLES])
    taxa_text = ";".join(affected_taxa)
    return (
        f"Skipping {len(unique_accessions)} unsupported legacy GTDB UBA accessions "
        f"from requested taxa {taxa_text}: {example_text}. These genome accessions "
        "are not supported by NCBI and will not be downloaded. Check BioProject "
        f"{UNSUPPORTED_UBA_BIOPROJECT}, as most UBA genomes are assigned through "
        "that bioproject."
    )


def build_unsupported_uba_error_message(accession: str) -> str:
    """Build the manifest error message for one unsupported `UBA*` accession."""

    return (
        f"Legacy GTDB accession {accession} is not supported by NCBI and was "
        f"skipped. Check BioProject {UNSUPPORTED_UBA_BIOPROJECT}, as most UBA "
        "genomes are assigned through that bioproject."
    )


def build_unsupported_accession_frame(selection_frame: pl.DataFrame) -> pl.DataFrame:
    """Attach fixed unsupported-accession fields to legacy `UBA*` rows."""

    if selection_frame.is_empty():
        return selection_frame.with_columns(
            pl.lit("").alias("final_accession"),
            pl.lit("").alias("accession_type_original"),
            pl.lit("").alias("accession_type_final"),
            pl.lit("").alias("conversion_status"),
        )
    return selection_frame.with_columns(
        pl.lit("").alias("final_accession"),
        pl.lit("unknown").alias("accession_type_original"),
        pl.lit("").alias("accession_type_final"),
        pl.lit("failed_no_usable_accession").alias("conversion_status"),
    )


def build_unsupported_executions(
    unsupported_frame: pl.DataFrame,
) -> dict[str, AccessionExecution]:
    """Build synthetic failed executions for unsupported `UBA*` accessions."""

    executions: dict[str, AccessionExecution] = {}
    if unsupported_frame.is_empty():
        return executions
    for row in unsupported_frame.unique(
        subset=["ncbi_accession"],
        keep="first",
        maintain_order=True,
    ).rows(named=True):
        accession = row["ncbi_accession"]
        executions[accession] = AccessionExecution(
            original_accession=accession,
            final_accession=None,
            conversion_status="failed_no_usable_accession",
            download_status="failed",
            download_batch=accession,
            payload_directory=None,
            failures=(
                CommandFailureRecord(
                    stage="preflight",
                    attempt_index=1,
                    max_attempts=1,
                    error_type="unsupported_accession",
                    error_message=build_unsupported_uba_error_message(accession),
                    final_status="unsupported_input",
                    attempted_accession=accession,
                ),
            ),
        )
    return executions


def prepare_selection_frames(
    args: CliArgs,
    logger: logging.Logger,
) -> tuple[ReleaseResolution, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Load bundled data, resolve the release, and select matching taxonomy rows."""

    resolution = resolve_and_validate_release(args.gtdb_release)
    taxonomy_frame = load_release_taxonomy(resolution)
    selected_frame = attach_taxon_slugs(
        select_taxa(taxonomy_frame, args.gtdb_taxa),
        args.gtdb_taxa,
    )
    supported_selected_frame, unsupported_selected_frame = (
        split_selected_rows_by_accession_support(selected_frame)
    )
    logger.info(
        "Resolved bundled release %s and matched %d taxonomy row(s)",
        resolution.resolved_release,
        selected_frame.height,
    )
    logger.info(
        "Selected %d supported accession(s) and %d unsupported legacy accession(s)",
        count_unique_accessions(supported_selected_frame),
        count_unique_accessions(unsupported_selected_frame),
    )
    return (
        resolution,
        selected_frame,
        supported_selected_frame,
        unsupported_selected_frame,
    )


# Early preflight and zero-match handling.


def run_early_dry_run_unzip_check(
    args: CliArgs,
    logger: logging.Logger,
) -> None:
    """Check `unzip` early so dry-runs surface the real-run requirement sooner."""

    if not args.dry_run:
        return
    logger.info("Checking unzip availability for dry-run")
    check_required_tools(("unzip",))


def run_supported_preflight(
    args: CliArgs,
    supported_selected_frame: pl.DataFrame,
) -> None:
    """Check tools that are required for supported accession planning or runs."""

    if supported_selected_frame.is_empty():
        return
    required_tools = get_required_tools(
        dry_run=args.dry_run,
    )
    if required_tools:
        check_required_tools(required_tools)


def handle_zero_match_exit(
    args: CliArgs,
    logger: logging.Logger,
    resolution: ReleaseResolution,
    selected_frame: pl.DataFrame,
    started_at: str,
) -> tuple[int | None, logging.Logger | None]:
    """Handle the zero-match path and return its exit code when it applies."""

    if not selected_frame.is_empty():
        return None, logger

    if args.dry_run:
        logger.warning("No genomes matched the requested taxa")
        logger.info(
            "Run finished: successful_accessions=0 failed_accessions=0 exit_code=4",
        )
        return 4, logger

    run_directories = initialise_run_directories(args.outdir)
    logger = configure_output_logger(args, logger, run_directories)
    logger.info("Writing output manifests to %s", run_directories.output_root)
    taxon_slug_map = build_taxon_slug_map(args.gtdb_taxa)
    exit_code = 4
    run_summary_rows = [
        build_run_summary_row(
            args,
            resolution,
            DEFAULT_REQUESTED_DOWNLOAD_METHOD,
            0,
            0,
            0,
            [],
            run_directories.output_root,
            exit_code,
            started_at,
            datetime.now(UTC).isoformat(),
        ),
    ]
    taxon_summary_rows = [
        {
            "requested_taxon": requested_taxon,
            "taxon_slug": taxon_slug_map[requested_taxon],
            "matched_rows": 0,
            "unique_gtdb_accessions": 0,
            "final_accessions": 0,
            "successful_accessions": 0,
            "failed_accessions": 0,
            "duplicate_copies_written": 0,
            "output_dir": str(
                run_directories.taxa_root / taxon_slug_map[requested_taxon],
            ),
        }
        for requested_taxon in args.gtdb_taxa
    ]
    write_zero_match_outputs(
        run_directories,
        args.gtdb_taxa,
        taxon_slug_map,
        run_summary_rows,
        taxon_summary_rows,
    )
    logger.warning("No genomes matched the requested taxa")
    logger.info(
        "Run finished: successful_accessions=0 failed_accessions=0 exit_code=4",
    )
    if not args.keep_temp:
        cleanup_error = cleanup_working_directories(run_directories)
        if cleanup_error is not None:
            logger.warning(
                "Could not remove working directory %s: %s",
                run_directories.working_root,
                cleanup_error,
            )
    close_logger(logger)
    return 4, None

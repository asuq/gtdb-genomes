"""End-to-end workflow execution for gtdb-genomes."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, UTC
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
import uuid

import polars as pl

from gtdb_genomes.download import (
    CommandFailureRecord,
    PreviewError,
    build_batch_dehydrate_command,
    build_download_command,
    build_preview_command,
    build_rehydrate_command,
    get_direct_download_concurrency,
    get_ordered_unique_accessions,
    get_rehydrate_workers,
    run_preview_command,
    run_retryable_command,
    select_download_method,
    write_accession_input_file,
)
from gtdb_genomes.layout import (
    LayoutError,
    RunDirectories,
    cleanup_working_directories,
    copy_accession_payload,
    extract_archive,
    get_accession_output_directory,
    get_duplicate_accessions,
    initialise_run_directories,
    write_root_manifests,
    write_taxon_accessions,
    write_zero_match_outputs,
)
from gtdb_genomes.logging_utils import (
    close_logger,
    configure_logging,
    redact_command,
    redact_text,
)
from gtdb_genomes.metadata import (
    MetadataLookupError,
    apply_accession_preferences,
    build_summary_command,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.release_resolver import (
    BundledDataError,
    resolve_and_validate_release,
)
from gtdb_genomes.selection import (
    attach_taxon_slugs,
    build_taxon_slug_map,
    select_taxa,
)
from gtdb_genomes.taxonomy import load_release_taxonomy

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


@dataclass(slots=True)
class AccessionPlan:
    """One unique accession to resolve and download for the run."""

    original_accession: str
    preferred_accession: str
    conversion_status: str


@dataclass(slots=True)
class AccessionExecution:
    """The materialised download outcome for one accession plan."""

    original_accession: str
    final_accession: str | None
    conversion_status: str
    download_status: str
    payload_directory: Path | None
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class DownloadExecutionResult:
    """The realised download execution details for one run."""

    executions: dict[str, AccessionExecution]
    method_used: str
    download_concurrency_used: int
    rehydrate_workers_used: int
    shared_failures: tuple[CommandFailureRecord, ...] = ()


UNSUPPORTED_UBA_PREFIX = "UBA"
UNSUPPORTED_UBA_BIOPROJECT = "PRJNA417962"
UNSUPPORTED_UBA_WARNING_EXAMPLES = 5


def is_unsupported_uba_accession(accession: str) -> bool:
    """Return whether one legacy GTDB accession starts with `UBA`."""

    return accession.startswith(UNSUPPORTED_UBA_PREFIX)


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


def build_accession_plans(mapped_frame: pl.DataFrame) -> tuple[AccessionPlan, ...]:
    """Build one unique download plan per original NCBI accession."""

    if mapped_frame.is_empty():
        return ()
    unique_rows = mapped_frame.unique(
        subset=["ncbi_accession"],
        keep="first",
        maintain_order=True,
    ).rows(named=True)
    return tuple(
        AccessionPlan(
            original_accession=row["ncbi_accession"],
            preferred_accession=row["final_accession"],
            conversion_status=row["conversion_status"],
        )
        for row in unique_rows
    )


def group_plans_by_preferred_accession(
    plans: tuple[AccessionPlan, ...],
) -> tuple[tuple[str, tuple[AccessionPlan, ...]], ...]:
    """Group accession plans by preferred accession in first-seen order."""

    grouped_plans: dict[str, list[AccessionPlan]] = {}
    for plan in plans:
        grouped_plans.setdefault(plan.preferred_accession, []).append(plan)
    return tuple(
        (preferred_accession, tuple(group))
        for preferred_accession, group in grouped_plans.items()
    )


def attach_attempted_accession(
    failures: tuple[CommandFailureRecord, ...],
    attempted_accession: str,
) -> tuple[CommandFailureRecord, ...]:
    """Fill missing attempted-accession values on shared failure records."""

    return tuple(
        replace(
            failure,
            attempted_accession=(
                failure.attempted_accession
                if failure.attempted_accession is not None
                else attempted_accession
            ),
        )
        for failure in failures
    )


def locate_accession_payload_directory(
    extraction_root: Path,
    accession: str,
) -> Path:
    """Locate the extracted datasets payload directory for one accession."""

    direct_candidate = extraction_root / "ncbi_dataset" / "data" / accession
    if direct_candidate.is_dir():
        return direct_candidate
    for candidate in extraction_root.rglob(accession):
        if candidate.is_dir():
            return candidate
    raise LayoutError(
        f"Could not locate extracted payload directory for accession {accession}",
    )


def locate_batch_payload_directories(
    extraction_root: Path,
    accessions: tuple[str, ...],
) -> dict[str, Path]:
    """Locate extracted payload directories for a batch of accessions."""

    payload_directories: dict[str, Path] = {}
    data_root = extraction_root / "ncbi_dataset" / "data"
    missing_accessions: set[str] = set()
    for accession in accessions:
        direct_candidate = data_root / accession
        if direct_candidate.is_dir():
            payload_directories[accession] = direct_candidate
            continue
        missing_accessions.add(accession)

    if not missing_accessions:
        return payload_directories

    for candidate in extraction_root.rglob("*"):
        if not candidate.is_dir():
            continue
        candidate_name = candidate.name
        if candidate_name in missing_accessions:
            payload_directories[candidate_name] = candidate
            missing_accessions.remove(candidate_name)
            if not missing_accessions:
                return payload_directories

    missing_text = ", ".join(sorted(missing_accessions))
    raise LayoutError(
        "Could not locate extracted payload directories for accessions: "
        f"{missing_text}",
    )


def build_layout_failure(
    error: Exception,
    final_status: str = "retry_exhausted",
) -> CommandFailureRecord:
    """Build a synthetic failure record for a local layout error."""

    return CommandFailureRecord(
        stage="preflight",
        attempt_index=1,
        max_attempts=1,
        error_type=type(error).__name__,
        error_message=str(error),
        final_status=final_status,
    )


def extract_download_payload(
    accession: str,
    archive_path: Path,
    run_directories: RunDirectories,
) -> tuple[Path | None, tuple[CommandFailureRecord, ...]]:
    """Extract one downloaded archive and locate its payload directory."""

    extraction_root = run_directories.extracted_root / accession
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return None, (build_layout_failure(error),)

    try:
        payload_directory = locate_accession_payload_directory(
            extraction_root,
            accession,
        )
    except LayoutError as error:
        return None, (build_layout_failure(error),)
    return payload_directory, ()


def build_failed_execution(
    original_accession: str,
    failures: tuple[CommandFailureRecord, ...],
) -> AccessionExecution:
    """Build a failed execution for one original accession."""

    return AccessionExecution(
        original_accession=original_accession,
        final_accession=None,
        conversion_status="failed_no_usable_accession",
        download_status="failed",
        payload_directory=None,
        failures=failures,
    )


def build_successful_execution(
    plan: AccessionPlan,
    final_accession: str,
    download_status: str,
    payload_directory: Path,
    failures: tuple[CommandFailureRecord, ...],
) -> AccessionExecution:
    """Build a successful execution for one accession plan."""

    conversion_status = plan.conversion_status
    if download_status == "downloaded_after_fallback":
        conversion_status = "paired_to_gca_fallback_original_on_download_failure"
    return AccessionExecution(
        original_accession=plan.original_accession,
        final_accession=final_accession,
        conversion_status=conversion_status,
        download_status=download_status,
        payload_directory=payload_directory,
        failures=failures,
    )


def execute_direct_group_fallbacks(
    preferred_accession: str,
    grouped_plans: tuple[AccessionPlan, ...],
    preferred_failures: tuple[CommandFailureRecord, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> dict[str, AccessionExecution]:
    """Run original-accession fallbacks for a failed preferred download group."""

    executions: dict[str, AccessionExecution] = {}
    for plan in grouped_plans:
        if plan.original_accession == preferred_accession:
            executions[plan.original_accession] = build_failed_execution(
                plan.original_accession,
                preferred_failures,
            )
            continue

        archive_path = (
            run_directories.downloads_root / f"{plan.original_accession}.zip"
        )
        logger.debug(
            "Preferred accession %s failed; falling back to original accession %s",
            preferred_accession,
            plan.original_accession,
        )
        fallback_result = run_retryable_command(
            build_download_command(
                [plan.original_accession],
                archive_path,
                args.include,
                ncbi_api_key=args.ncbi_api_key,
                debug=args.debug,
            ),
            stage="fallback_download",
            final_failure_status="fallback_exhausted",
            attempted_accession=plan.original_accession,
        )
        combined_failures = preferred_failures + fallback_result.failures
        if not fallback_result.succeeded:
            executions[plan.original_accession] = build_failed_execution(
                plan.original_accession,
                combined_failures,
            )
            continue

        payload_directory, extraction_failures = extract_download_payload(
            plan.original_accession,
            archive_path,
            run_directories,
        )
        combined_failures += extraction_failures
        if payload_directory is None:
            executions[plan.original_accession] = build_failed_execution(
                plan.original_accession,
                combined_failures,
            )
            continue

        executions[plan.original_accession] = build_successful_execution(
            plan,
            plan.original_accession,
            "downloaded_after_fallback",
            payload_directory,
            combined_failures,
        )
    return executions


def execute_direct_accession_group(
    preferred_accession: str,
    grouped_plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> dict[str, AccessionExecution]:
    """Download one preferred accession once and materialise grouped executions."""

    archive_path = run_directories.downloads_root / f"{preferred_accession}.zip"
    logger.debug("Downloading preferred accession %s", preferred_accession)
    preferred_result = run_retryable_command(
        build_download_command(
            [preferred_accession],
            archive_path,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        ),
        stage="preferred_download",
        attempted_accession=preferred_accession,
    )
    if not preferred_result.succeeded:
        return execute_direct_group_fallbacks(
            preferred_accession,
            grouped_plans,
            preferred_result.failures,
            args,
            run_directories,
            logger,
        )

    payload_directory, extraction_failures = extract_download_payload(
        preferred_accession,
        archive_path,
        run_directories,
    )
    combined_failures = preferred_result.failures + extraction_failures
    if payload_directory is None:
        return {
            plan.original_accession: build_failed_execution(
                plan.original_accession,
                combined_failures,
            )
            for plan in grouped_plans
        }

    return {
        plan.original_accession: build_successful_execution(
            plan,
            preferred_accession,
            "downloaded",
            payload_directory,
            preferred_result.failures,
        )
        for plan in grouped_plans
    }


def execute_direct_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> DownloadExecutionResult:
    """Execute direct accession downloads with bounded concurrency."""

    if not plans:
        return DownloadExecutionResult(
            executions={},
            method_used="direct",
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )
    plan_groups = group_plans_by_preferred_accession(plans)
    max_workers = max(
        1,
        get_direct_download_concurrency(args.threads, len(plan_groups)),
    )
    executions: dict[str, AccessionExecution] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                execute_direct_accession_group,
                preferred_accession,
                grouped_plans,
                args,
                run_directories,
                logger,
            ): preferred_accession
            for preferred_accession, grouped_plans in plan_groups
        }
        for future in future_map:
            executions.update(future.result())
    return DownloadExecutionResult(
        executions=executions,
        method_used="direct",
        download_concurrency_used=max_workers,
        rehydrate_workers_used=0,
        shared_failures=(),
    )


def build_batch_layout_failures(
    failures: tuple[CommandFailureRecord, ...],
    error: Exception,
) -> tuple[CommandFailureRecord, ...]:
    """Append one synthetic local layout failure to a batch failure list."""

    return failures + (build_layout_failure(error),)


def build_batch_archive_path(run_directories: RunDirectories) -> Path:
    """Return the shared archive path for a dehydrated batch download."""

    return run_directories.downloads_root / "dehydrated_batch.zip"


def execute_batch_dehydrate_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> DownloadExecutionResult:
    """Execute one dehydrated batch download with fallback to direct mode."""

    if not plans:
        return DownloadExecutionResult(
            executions={},
            method_used="dehydrate",
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )

    batch_attempted_accessions = ";".join(
        get_ordered_unique_accessions(
            plan.preferred_accession for plan in plans
        ),
    )
    accession_file = write_accession_input_file(
        run_directories.working_root / "dehydrate_accessions.txt",
        (plan.preferred_accession for plan in plans),
    )
    archive_path = build_batch_archive_path(run_directories)
    download_command = build_batch_dehydrate_command(
        accession_file,
        archive_path,
        args.include,
        ncbi_api_key=args.ncbi_api_key,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(download_command, secrets))
    batch_download = run_retryable_command(
        download_command,
        stage="preferred_download",
        attempted_accession=batch_attempted_accessions,
    )
    if not batch_download.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=attach_attempted_accession(
                batch_download.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )

    extraction_root = run_directories.extracted_root / "dehydrated_batch"
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=attach_attempted_accession(
                build_batch_layout_failures(batch_download.failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )

    rehydrate_workers = get_rehydrate_workers(args.threads)
    rehydrate_command = build_rehydrate_command(
        extraction_root,
        rehydrate_workers,
        ncbi_api_key=args.ncbi_api_key,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(rehydrate_command, secrets))
    rehydrate_result = run_retryable_command(
        rehydrate_command,
        stage="rehydrate",
        attempted_accession=batch_attempted_accessions,
    )
    if not rehydrate_result.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=attach_attempted_accession(
                batch_download.failures + rehydrate_result.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )

    shared_failures = attach_attempted_accession(
        batch_download.failures + rehydrate_result.failures,
        batch_attempted_accessions,
    )
    executions: dict[str, AccessionExecution] = {}
    try:
        payload_directories = locate_batch_payload_directories(
            extraction_root,
            tuple(plan.preferred_accession for plan in plans),
        )
        for plan in plans:
            executions[plan.original_accession] = AccessionExecution(
                original_accession=plan.original_accession,
                final_accession=plan.preferred_accession,
                conversion_status=plan.conversion_status,
                download_status="downloaded",
                payload_directory=payload_directories[plan.preferred_accession],
                failures=(),
            )
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=attach_attempted_accession(
                build_batch_layout_failures(shared_failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )

    return DownloadExecutionResult(
        executions=executions,
        method_used="dehydrate",
        download_concurrency_used=1,
        rehydrate_workers_used=rehydrate_workers,
        shared_failures=shared_failures,
    )


def fallback_batch_to_direct(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    batch_failures: tuple[CommandFailureRecord, ...],
    rehydrate_workers_used: int,
) -> DownloadExecutionResult:
    """Fall back from a failed dehydrated batch workflow to direct downloads."""

    logger.warning(
        "Batch dehydrated download failed; falling back to per-accession direct downloads",
    )
    direct_result = execute_direct_accession_plans(
        plans,
        args,
        run_directories,
        logger,
    )
    return DownloadExecutionResult(
        executions=direct_result.executions,
        method_used="dehydrate_fallback_direct",
        download_concurrency_used=direct_result.download_concurrency_used,
        rehydrate_workers_used=rehydrate_workers_used,
        shared_failures=batch_failures,
    )


def execute_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    decision_method: str,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> DownloadExecutionResult:
    """Execute accession plans for the selected download method."""

    if decision_method == "dehydrate":
        return execute_batch_dehydrate_plans(
            plans,
            args,
            run_directories,
            logger,
            secrets,
        )
    return execute_direct_accession_plans(
        plans,
        args,
        run_directories,
        logger,
    )


def build_taxon_summary_rows(
    accession_rows: list[dict[str, Any]],
    duplicate_counts: dict[str, int],
    run_directories: RunDirectories,
) -> list[dict[str, Any]]:
    """Build `taxon_summary.tsv` rows from accession-level output rows."""

    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in accession_rows:
        grouped_rows[row["requested_taxon"]].append(row)

    summary_rows: list[dict[str, Any]] = []
    for requested_taxon, rows in grouped_rows.items():
        taxon_slug = rows[0]["taxon_slug"]
        summary_rows.append(
            {
                "requested_taxon": requested_taxon,
                "taxon_slug": taxon_slug,
                "matched_rows": len(rows),
                "unique_gtdb_accessions": len(
                    {row["gtdb_accession"] for row in rows},
                ),
                "final_accessions": len(
                    {
                        row["final_accession"]
                        for row in rows
                        if row["final_accession"]
                    },
                ),
                "successful_accessions": len(
                    {
                        row["final_accession"]
                        for row in rows
                        if row["download_status"] != "failed"
                        and row["final_accession"]
                    },
                ),
                "failed_accessions": len(
                    {
                        row["gtdb_accession"]
                        for row in rows
                        if row["download_status"] == "failed"
                    },
                ),
                "duplicate_copies_written": duplicate_counts.get(requested_taxon, 0),
                "output_dir": str(run_directories.taxa_root / taxon_slug),
            },
        )
    return summary_rows


def build_run_summary_row(
    args: CliArgs,
    requested_release: str,
    resolved_release: str,
    method_used: str,
    download_concurrency_used: int,
    rehydrate_workers_used: int,
    matched_rows: int,
    accession_rows: list[dict[str, Any]],
    output_root: Path | None,
    exit_code: int,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    """Build the single `run_summary.tsv` row."""

    return {
        "run_id": uuid.uuid4().hex,
        "started_at": started_at,
        "finished_at": finished_at,
        "requested_release": requested_release,
        "resolved_release": resolved_release,
        "download_method_requested": args.download_method,
        "download_method_used": method_used,
        "threads_requested": args.threads,
        "download_concurrency_used": download_concurrency_used,
        "rehydrate_workers_used": rehydrate_workers_used,
        "include": args.include,
        "prefer_genbank": str(args.prefer_genbank).lower(),
        "debug_enabled": str(args.debug).lower(),
        "requested_taxa_count": len(args.gtdb_taxa),
        "matched_rows": matched_rows,
        "unique_gtdb_accessions": len(
            {row["gtdb_accession"] for row in accession_rows},
        ),
        "final_accessions": len(
            {row["final_accession"] for row in accession_rows if row["final_accession"]},
        ),
        "successful_accessions": len(
            {
                row["final_accession"]
                for row in accession_rows
                if row["download_status"] != "failed" and row["final_accession"]
            },
        ),
        "failed_accessions": len(
            {
                row["gtdb_accession"]
                for row in accession_rows
                if row["download_status"] == "failed"
            },
        ),
        "output_dir": "" if output_root is None else str(output_root),
        "exit_code": exit_code,
    }


def join_unique_row_values(
    rows: list[dict[str, Any]],
    field_name: str,
) -> str:
    """Collapse one row field into a deterministic semicolon-joined value."""

    return ";".join(
        sorted(
            {
                str(row.get(field_name, "")).strip()
                for row in rows
                if str(row.get(field_name, "")).strip()
            },
        ),
    )


def build_shared_failure_rows(
    rows: list[dict[str, Any]],
    failures: tuple[CommandFailureRecord, ...],
    secrets: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Build one failure-manifest row per shared command attempt."""

    if not rows or not failures:
        return []
    requested_taxa = join_unique_row_values(rows, "requested_taxon")
    taxon_slugs = join_unique_row_values(rows, "taxon_slug")
    gtdb_accessions = join_unique_row_values(rows, "gtdb_accession")
    ncbi_accessions = join_unique_row_values(rows, "ncbi_accession")
    final_accessions = join_unique_row_values(rows, "final_accession")
    return [
        {
            "requested_taxon": requested_taxa,
            "taxon_slug": taxon_slugs,
            "gtdb_accession": gtdb_accessions,
            "attempted_accession": (
                failure.attempted_accession or ncbi_accessions
            ),
            "final_accession": final_accessions,
            "stage": failure.stage,
            "attempt_index": failure.attempt_index,
            "max_attempts": failure.max_attempts,
            "error_type": failure.error_type,
            "error_message_redacted": redact_text(
                failure.error_message,
                secrets,
            ),
            "final_status": failure.final_status,
        }
        for failure in failures
    ]


def build_failure_rows(
    enriched_rows: list[dict[str, Any]],
    executions: dict[str, AccessionExecution],
    metadata_failures: tuple[CommandFailureRecord, ...],
    shared_failures: tuple[CommandFailureRecord, ...],
    secrets: tuple[str, ...],
    shared_context_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build attempt-centric `download_failures.tsv` rows."""

    context_rows = enriched_rows if shared_context_rows is None else shared_context_rows
    failure_rows: list[dict[str, Any]] = []
    failure_rows.extend(
        build_shared_failure_rows(context_rows, metadata_failures, secrets),
    )
    failure_rows.extend(
        build_shared_failure_rows(context_rows, shared_failures, secrets),
    )

    rows_by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in enriched_rows:
        rows_by_accession[row["ncbi_accession"]].append(row)

    for accession, rows in rows_by_accession.items():
        execution = executions[accession]
        accession_failures = execution.failures
        requested_taxa = ";".join(
            sorted({row["requested_taxon"] for row in rows}),
        )
        taxon_slugs = ";".join(
            sorted({row["taxon_slug"] for row in rows}),
        )
        gtdb_accessions = ";".join(
            sorted({row["gtdb_accession"] for row in rows}),
        )
        final_accession = execution.final_accession or ""
        for failure in accession_failures:
            failure_rows.append(
                {
                    "requested_taxon": requested_taxa,
                    "taxon_slug": taxon_slugs,
                    "gtdb_accession": gtdb_accessions,
                    "attempted_accession": failure.attempted_accession or accession,
                    "final_accession": final_accession,
                    "stage": failure.stage,
                    "attempt_index": failure.attempt_index,
                    "max_attempts": failure.max_attempts,
                    "error_type": failure.error_type,
                    "error_message_redacted": redact_text(
                        failure.error_message,
                        secrets,
                    ),
                    "final_status": failure.final_status,
                },
            )
    return failure_rows


def run_workflow(args: CliArgs) -> int:
    """Run the documented workflow and return the process exit code."""

    logger, _ = configure_logging(debug=args.debug, dry_run=args.dry_run)
    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    started_at = datetime.now(UTC).isoformat()

    try:
        resolution = resolve_and_validate_release(args.gtdb_release)
    except BundledDataError as error:
        logger.error("%s", error)
        close_logger(logger)
        return 3

    taxonomy_frame = load_release_taxonomy(resolution)
    selected_frame = attach_taxon_slugs(
        select_taxa(taxonomy_frame, args.gtdb_taxa),
        args.gtdb_taxa,
    )

    if selected_frame.is_empty():
        if args.dry_run:
            logger.warning("No genomes matched the requested taxa")
            close_logger(logger)
            return 4

        run_directories = initialise_run_directories(args.outdir)
        close_logger(logger)
        logger, _ = configure_logging(
            debug=args.debug,
            dry_run=False,
            output_root=run_directories.output_root,
        )
        taxon_slug_map = build_taxon_slug_map(args.gtdb_taxa)
        exit_code = 4
        run_summary_rows = [
            build_run_summary_row(
                args,
                args.gtdb_release,
                resolution.resolved_release,
                args.download_method,
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
        close_logger(logger)
        if not args.keep_temp:
            cleanup_working_directories(run_directories)
        return exit_code

    supported_selected_frame, unsupported_selected_frame = (
        split_selected_rows_by_accession_support(selected_frame)
    )
    if not unsupported_selected_frame.is_empty():
        logger.warning(build_unsupported_uba_warning(unsupported_selected_frame))

    summary_map: dict[str, set[str]] = {}
    metadata_failures: tuple[CommandFailureRecord, ...] = ()
    supported_accessions = get_ordered_unique_accessions(
        supported_selected_frame.get_column("ncbi_accession").to_list(),
    )
    if not supported_selected_frame.is_empty() and args.prefer_genbank:
        with TemporaryDirectory(
            prefix="gtdb_genomes_metadata_",
            dir="/tmp",
        ) as metadata_directory:
            metadata_accession_file = write_accession_input_file(
                Path(metadata_directory) / "accessions.txt",
                supported_accessions,
            )
            metadata_command = build_summary_command(
                metadata_accession_file,
                ncbi_api_key=args.ncbi_api_key,
            )
            logger.debug("Running %s", redact_command(metadata_command, secrets))
            try:
                summary_lookup = run_summary_lookup_with_retries(
                    supported_accessions,
                    metadata_accession_file,
                    ncbi_api_key=args.ncbi_api_key,
                )
                summary_map = summary_lookup.summary_map
                metadata_failures = summary_lookup.failures
            except MetadataLookupError as error:
                metadata_failures = error.failures
                logger.warning(
                    "Metadata lookup failed; falling back to original accessions: %s",
                    redact_text(str(error), secrets),
                )
                summary_map = {}

    supported_mapped_frame = apply_accession_preferences(
        supported_selected_frame,
        summary_map,
        prefer_genbank=args.prefer_genbank,
    )
    unsupported_mapped_frame = build_unsupported_accession_frame(
        unsupported_selected_frame,
    )
    mapped_frame = pl.concat(
        [
            frame
            for frame in (supported_mapped_frame, unsupported_mapped_frame)
            if not frame.is_empty()
        ],
        how="vertical",
    )
    accession_plans = build_accession_plans(supported_mapped_frame)
    preview_text: str | None = None
    if args.download_method == "auto" and accession_plans:
        preview_accessions = get_ordered_unique_accessions(
            plan.preferred_accession for plan in accession_plans
        )
        with TemporaryDirectory(
            prefix="gtdb_genomes_preview_",
            dir="/tmp",
        ) as preview_directory:
            preview_accession_file = write_accession_input_file(
                Path(preview_directory) / "accessions.txt",
                preview_accessions,
            )
            preview_command = build_preview_command(
                preview_accession_file,
                args.include,
                ncbi_api_key=args.ncbi_api_key,
                debug=args.debug,
            )
            logger.debug("Running %s", redact_command(preview_command, secrets))
            try:
                preview_text = run_preview_command(
                    preview_accession_file,
                    args.include,
                    ncbi_api_key=args.ncbi_api_key,
                    debug=args.debug,
                )
            except PreviewError as error:
                logger.error("%s", redact_text(str(error), secrets))
                close_logger(logger)
                return 5
    if accession_plans:
        try:
            decision = select_download_method(
                args.download_method,
                len(accession_plans),
                preview_text=preview_text,
            )
        except PreviewError as error:
            logger.error("%s", redact_text(str(error), secrets))
            close_logger(logger)
            return 5
        decision_method = decision.method_used
    else:
        decision_method = args.download_method

    if args.dry_run:
        close_logger(logger)
        return 0

    run_directories = initialise_run_directories(args.outdir)
    close_logger(logger)
    logger, _ = configure_logging(
        debug=args.debug,
        dry_run=False,
        output_root=run_directories.output_root,
    )

    if accession_plans:
        execution_result = execute_accession_plans(
            accession_plans,
            args,
            decision_method,
            run_directories,
            logger,
            secrets,
        )
    else:
        execution_result = DownloadExecutionResult(
            executions={},
            method_used=args.download_method,
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )
    executions = {
        **execution_result.executions,
        **build_unsupported_executions(unsupported_selected_frame),
    }

    enriched_rows: list[dict[str, Any]] = []
    supported_enriched_rows: list[dict[str, Any]] = []
    for row in mapped_frame.rows(named=True):
        execution = executions[row["ncbi_accession"]]
        final_accession = execution.final_accession or ""
        unsupported_accession = is_unsupported_uba_accession(row["ncbi_accession"])
        enriched_rows.append(
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "resolved_release": resolution.resolved_release,
                "taxonomy_file": row["taxonomy_file"],
                "lineage": row["lineage"],
                "gtdb_accession": row["gtdb_accession"],
                "ncbi_accession": row["ncbi_accession"],
                "final_accession": final_accession,
                "accession_type_original": row["accession_type_original"],
                "accession_type_final": (
                    row["accession_type_final"]
                    if execution.final_accession is not None
                    else ""
                ),
                "conversion_status": execution.conversion_status,
                "download_method_used": execution_result.method_used,
                "download_batch": (
                    row["ncbi_accession"]
                    if unsupported_accession
                    else "dehydrated_batch"
                    if execution_result.method_used == "dehydrate"
                    else row["ncbi_accession"]
                ),
                "output_relpath": "",
                "download_status": execution.download_status,
                "duplicate_across_taxa": False,
            },
        )
        if not unsupported_accession:
            supported_enriched_rows.append(enriched_rows[-1])

    duplicate_accessions = get_duplicate_accessions(enriched_rows)
    seen_taxon_accessions: set[tuple[str, str]] = set()
    seen_accessions: set[str] = set()
    duplicate_counts: dict[str, int] = defaultdict(int)
    per_taxon_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in enriched_rows:
        if row["download_status"] != "failed" and row["final_accession"]:
            row["duplicate_across_taxa"] = row["final_accession"] in duplicate_accessions
            key = (row["taxon_slug"], row["final_accession"])
            if key not in seen_taxon_accessions:
                destination_directory = get_accession_output_directory(
                    run_directories,
                    row["taxon_slug"],
                    row["final_accession"],
                )
                payload_directory = executions[row["ncbi_accession"]].payload_directory
                if payload_directory is None:
                    raise AssertionError("successful accessions must have payloads")
                copy_accession_payload(payload_directory, destination_directory)
                row["output_relpath"] = str(
                    destination_directory.relative_to(run_directories.output_root),
                )
                seen_taxon_accessions.add(key)
                if row["final_accession"] in seen_accessions:
                    duplicate_counts[row["requested_taxon"]] += 1
                    logger.info(
                        "Copied duplicate genome %s into taxon %s",
                        row["final_accession"],
                        row["taxon_slug"],
                    )
                else:
                    seen_accessions.add(row["final_accession"])
            else:
                row["output_relpath"] = str(
                    get_accession_output_directory(
                        run_directories,
                        row["taxon_slug"],
                        row["final_accession"],
                    ).relative_to(run_directories.output_root),
                )

        per_taxon_rows[row["taxon_slug"]].append(
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "lineage": row["lineage"],
                "gtdb_accession": row["gtdb_accession"],
                "final_accession": row["final_accession"],
                "conversion_status": row["conversion_status"],
                "output_relpath": row["output_relpath"],
                "download_status": row["download_status"],
                "duplicate_across_taxa": str(row["duplicate_across_taxa"]).lower(),
            },
        )

    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        metadata_failures,
        execution_result.shared_failures,
        secrets,
        shared_context_rows=supported_enriched_rows,
    )
    taxon_summary_rows = build_taxon_summary_rows(
        enriched_rows,
        duplicate_counts,
        run_directories,
    )
    successful_count = len(
        {
            row["final_accession"]
            for row in enriched_rows
            if row["download_status"] != "failed" and row["final_accession"]
        },
    )
    failed_count = len(
        {row["gtdb_accession"] for row in enriched_rows if row["download_status"] == "failed"},
    )
    if failed_count == 0:
        exit_code = 0
    elif successful_count > 0:
        exit_code = 6
    else:
        exit_code = 7

    run_summary_rows = [
        build_run_summary_row(
            args,
            args.gtdb_release,
            resolution.resolved_release,
            execution_result.method_used,
            execution_result.download_concurrency_used,
            execution_result.rehydrate_workers_used,
            mapped_frame.height,
            enriched_rows,
            run_directories.output_root,
            exit_code,
            started_at,
            datetime.now(UTC).isoformat(),
        ),
    ]
    write_root_manifests(
        run_directories,
        run_summary_rows,
        taxon_summary_rows,
        [
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "resolved_release": row["resolved_release"],
                "taxonomy_file": row["taxonomy_file"],
                "lineage": row["lineage"],
                "gtdb_accession": row["gtdb_accession"],
                "final_accession": row["final_accession"],
                "accession_type_original": row["accession_type_original"],
                "accession_type_final": row["accession_type_final"],
                "conversion_status": row["conversion_status"],
                "download_method_used": row["download_method_used"],
                "download_batch": row["download_batch"],
                "output_relpath": row["output_relpath"],
                "download_status": row["download_status"],
            }
            for row in enriched_rows
        ],
        failure_rows,
    )
    for taxon_slug, rows in per_taxon_rows.items():
        write_taxon_accessions(run_directories, taxon_slug, rows)

    close_logger(logger)
    if not args.keep_temp:
        cleanup_working_directories(run_directories)
    return exit_code

"""End-to-end workflow execution for gtdb-genomes."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, UTC
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
import uuid

import polars as pl

from gtdb_genomes.download import (
    AccessionDownloadResult,
    CommandFailureRecord,
    PreviewError,
    build_download_command,
    build_preview_command,
    build_rehydrate_command,
    download_with_accession_fallback,
    get_direct_download_concurrency,
    get_rehydrate_workers,
    run_preview_command,
    run_retryable_command,
    select_download_method,
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
    run_summary_lookup,
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

    gtdb_accession: str
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
            gtdb_accession=row["gtdb_accession"],
            original_accession=row["ncbi_accession"],
            preferred_accession=row["final_accession"],
            conversion_status=row["conversion_status"],
        )
        for row in unique_rows
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
    args: CliArgs,
    decision_method: str,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[Path | None, tuple[CommandFailureRecord, ...]]:
    """Extract one downloaded archive and optionally rehydrate it."""

    extraction_root = run_directories.extracted_root / accession
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return None, (build_layout_failure(error),)

    if decision_method == "dehydrate":
        rehydrate_command = build_rehydrate_command(
            extraction_root,
            get_rehydrate_workers(args.threads),
            api_key=args.api_key,
            debug=args.debug,
        )
        logger.debug("Running %s", redact_command(rehydrate_command, secrets))
        rehydrate_result = run_retryable_command(
            rehydrate_command,
            stage="rehydrate",
        )
        if not rehydrate_result.succeeded:
            return None, rehydrate_result.failures

    try:
        payload_directory = locate_accession_payload_directory(
            extraction_root,
            accession,
        )
    except LayoutError as error:
        return None, (build_layout_failure(error),)
    return payload_directory, ()


def execute_accession_plan(
    plan: AccessionPlan,
    args: CliArgs,
    decision_method: str,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> AccessionExecution:
    """Download and extract one accession plan."""

    archive_path = run_directories.downloads_root / f"{plan.original_accession}.zip"
    fallback_accession = None
    if plan.preferred_accession != plan.original_accession:
        fallback_accession = plan.original_accession

    preferred_command = build_download_command(
        [plan.preferred_accession],
        archive_path,
        args.include,
        api_key=args.api_key,
        dehydrated=decision_method == "dehydrate",
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(preferred_command, secrets))
    if fallback_accession is not None:
        fallback_command = build_download_command(
            [fallback_accession],
            archive_path,
            args.include,
            api_key=args.api_key,
            dehydrated=decision_method == "dehydrate",
            debug=args.debug,
        )
        logger.debug(
            "Prepared fallback %s",
            redact_command(fallback_command, secrets),
        )

    download_result: AccessionDownloadResult = download_with_accession_fallback(
        preferred_accession=plan.preferred_accession,
        fallback_accession=fallback_accession,
        archive_path=archive_path,
        include=args.include,
        api_key=args.api_key,
        dehydrated=decision_method == "dehydrate",
        debug=args.debug,
    )
    if not download_result.succeeded or download_result.used_accession is None:
        return AccessionExecution(
            original_accession=plan.original_accession,
            final_accession=None,
            conversion_status="failed_no_usable_accession",
            download_status="failed",
            payload_directory=None,
            failures=download_result.failures,
        )

    payload_directory, extraction_failures = extract_download_payload(
        download_result.used_accession,
        archive_path,
        run_directories,
        args,
        decision_method,
        logger,
        secrets,
    )
    if payload_directory is None:
        return AccessionExecution(
            original_accession=plan.original_accession,
            final_accession=None,
            conversion_status="failed_no_usable_accession",
            download_status="failed",
            payload_directory=None,
            failures=download_result.failures + extraction_failures,
        )

    conversion_status = plan.conversion_status
    download_status = "downloaded"
    if download_result.used_fallback and fallback_accession is not None:
        conversion_status = "paired_to_gca_fallback_original_on_download_failure"
        download_status = "downloaded_after_fallback"
    return AccessionExecution(
        original_accession=plan.original_accession,
        final_accession=download_result.used_accession,
        conversion_status=conversion_status,
        download_status=download_status,
        payload_directory=payload_directory,
        failures=download_result.failures + extraction_failures,
    )


def execute_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    decision_method: str,
    run_directories: RunDirectories,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> dict[str, AccessionExecution]:
    """Execute all accession plans with bounded concurrency."""

    if not plans:
        return {}
    max_workers = max(1, get_direct_download_concurrency(args.threads, len(plans)))
    executions: dict[str, AccessionExecution] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                execute_accession_plan,
                plan,
                args,
                decision_method,
                run_directories,
                logger,
                secrets,
            ): plan.original_accession
            for plan in plans
        }
        for future, original_accession in future_map.items():
            executions[original_accession] = future.result()
    return executions


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
    decision_method: str,
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
        "download_method_used": decision_method,
        "threads_requested": args.threads,
        "download_concurrency_used": min(args.threads, 5),
        "rehydrate_workers_used": get_rehydrate_workers(args.threads),
        "include": args.include,
        "prefer_genbank": str(args.prefer_genbank).lower(),
        "debug_enabled": str(args.debug).lower(),
        "requested_taxa_count": len(args.taxa),
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


def build_failure_rows(
    enriched_rows: list[dict[str, Any]],
    executions: dict[str, AccessionExecution],
    secrets: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Build `download_failures.tsv` rows from accession executions."""

    failure_rows: list[dict[str, Any]] = []
    for row in enriched_rows:
        execution = executions[row["ncbi_accession"]]
        for failure in execution.failures:
            failure_rows.append(
                {
                    "requested_taxon": row["requested_taxon"],
                    "taxon_slug": row["taxon_slug"],
                    "gtdb_accession": row["gtdb_accession"],
                    "final_accession": row["final_accession"],
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
    secrets = tuple(secret for secret in (args.api_key,) if secret)
    started_at = datetime.now(UTC).isoformat()

    try:
        resolution = resolve_and_validate_release(args.release)
    except BundledDataError as error:
        logger.error("%s", error)
        close_logger(logger)
        return 3

    taxonomy_frame = load_release_taxonomy(resolution)
    selected_frame = attach_taxon_slugs(
        select_taxa(taxonomy_frame, args.taxa),
        args.taxa,
    )

    summary_map: dict[str, set[str]] = {}
    if not selected_frame.is_empty() and args.prefer_genbank:
        metadata_command = build_summary_command(
            selected_frame.get_column("ncbi_accession").unique().to_list(),
            api_key=args.api_key,
        )
        logger.debug("Running %s", redact_command(metadata_command, secrets))
        try:
            summary_map = run_summary_lookup(
                selected_frame.get_column("ncbi_accession").unique().to_list(),
                api_key=args.api_key,
            )
        except MetadataLookupError as error:
            logger.warning(
                "Metadata lookup failed; falling back to original accessions: %s",
                redact_text(str(error), secrets),
            )
            summary_map = {}

    mapped_frame = apply_accession_preferences(
        selected_frame,
        summary_map,
        prefer_genbank=args.prefer_genbank,
    )
    if selected_frame.is_empty():
        if args.dry_run:
            logger.warning("No genomes matched the requested taxa")
            close_logger(logger)
            return 4

        run_directories = initialise_run_directories(args.output)
        close_logger(logger)
        logger, _ = configure_logging(
            debug=args.debug,
            dry_run=False,
            output_root=run_directories.output_root,
        )
        taxon_slug_map = build_taxon_slug_map(args.taxa)
        exit_code = 4
        run_summary_rows = [
            build_run_summary_row(
                args,
                args.release,
                resolution.resolved_release,
                args.download_method,
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
            for requested_taxon in args.taxa
        ]
        write_zero_match_outputs(
            run_directories,
            args.taxa,
            taxon_slug_map,
            run_summary_rows,
            taxon_summary_rows,
        )
        logger.warning("No genomes matched the requested taxa")
        close_logger(logger)
        if not args.keep_temp:
            cleanup_working_directories(run_directories)
        return exit_code

    accession_plans = build_accession_plans(mapped_frame)
    preview_text: str | None = None
    if args.download_method == "auto" and accession_plans:
        preview_command = build_preview_command(
            [plan.preferred_accession for plan in accession_plans],
            args.include,
            api_key=args.api_key,
            debug=args.debug,
        )
        logger.debug("Running %s", redact_command(preview_command, secrets))
        try:
            preview_text = run_preview_command(
                [plan.preferred_accession for plan in accession_plans],
                args.include,
                api_key=args.api_key,
                debug=args.debug,
            )
        except PreviewError as error:
            logger.error("%s", redact_text(str(error), secrets))
            close_logger(logger)
            return 5
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

    if args.dry_run:
        close_logger(logger)
        return 0

    run_directories = initialise_run_directories(args.output)
    close_logger(logger)
    logger, _ = configure_logging(
        debug=args.debug,
        dry_run=False,
        output_root=run_directories.output_root,
    )

    executions = execute_accession_plans(
        accession_plans,
        args,
        decision.method_used,
        run_directories,
        logger,
        secrets,
    )

    enriched_rows: list[dict[str, Any]] = []
    for row in mapped_frame.rows(named=True):
        execution = executions[row["ncbi_accession"]]
        final_accession = execution.final_accession or ""
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
                "download_method_used": decision.method_used,
                "download_batch": row["ncbi_accession"],
                "output_relpath": "",
                "download_status": execution.download_status,
                "duplicate_across_taxa": False,
            },
        )

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

    failure_rows = build_failure_rows(enriched_rows, executions, secrets)
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
            args.release,
            resolution.resolved_release,
            decision.method_used,
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

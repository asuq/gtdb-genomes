"""Output-writing helpers for the GTDB workflow."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
import uuid

import polars as pl

from gtdb_genomes.download import CommandFailureRecord
from gtdb_genomes.layout import (
    RunDirectories,
    copy_accession_payload,
    get_accession_output_directory,
    get_duplicate_accessions,
    write_root_manifests,
    write_taxon_accessions,
)
from gtdb_genomes.logging_utils import close_logger, configure_logging, redact_text
from gtdb_genomes.metadata import SUPPRESSED_ASSEMBLY_NOTE
from gtdb_genomes.workflow_execution import (
    AccessionExecution,
    DownloadExecutionResult,
    SharedFailureContext,
)

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs
    from gtdb_genomes.workflow_planning import SuppressedAccessionNote


# Logger and output-root helpers.


def configure_output_logger(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
) -> logging.Logger:
    """Switch from console-only logging to the output-root logger."""

    close_logger(logger)
    logger, _ = configure_logging(
        debug=args.debug,
        dry_run=False,
        output_root=run_directories.output_root,
    )
    return logger


# Manifest row builders.


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
    shared_failures: tuple[SharedFailureContext, ...],
    secrets: tuple[str, ...],
    shared_context_rows: list[dict[str, Any]] | None = None,
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> list[dict[str, Any]]:
    """Build attempt-centric `download_failures.tsv` rows."""

    context_rows = enriched_rows if shared_context_rows is None else shared_context_rows
    failure_rows: list[dict[str, Any]] = []
    suppressed_accessions = (
        {}
        if suppressed_notes is None
        else suppressed_notes
    )
    failure_rows.extend(
        build_shared_failure_rows(context_rows, metadata_failures, secrets),
    )

    rows_by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in enriched_rows:
        rows_by_accession[row["ncbi_accession"]].append(row)

    for shared_failure in shared_failures:
        scoped_rows = [
            row
            for accession in shared_failure.affected_original_accessions
            for row in rows_by_accession.get(accession, ())
        ]
        failure_rows.extend(
            build_shared_failure_rows(
                scoped_rows,
                shared_failure.failures,
                secrets,
            ),
        )

    for accession, rows in rows_by_accession.items():
        execution = executions[accession]
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
        for failure in execution.failures:
            error_message = failure.error_message
            if (
                accession in suppressed_accessions
                and execution.download_status == "failed"
            ):
                error_message = f"{error_message} {SUPPRESSED_ASSEMBLY_NOTE}"
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
                        error_message,
                        secrets,
                    ),
                    "final_status": failure.final_status,
                },
            )
    return failure_rows


# Output materialisation and exit handling.


def build_enriched_output_rows(
    resolved_release: str,
    mapped_frame: pl.DataFrame,
    execution_result: DownloadExecutionResult,
    unsupported_executions: dict[str, AccessionExecution],
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, list[dict[str, Any]]],
    dict[str, int],
]:
    """Build manifest rows and copy payloads into their final taxon directories."""

    executions = {
        **execution_result.executions,
        **unsupported_executions,
    }
    enriched_rows: list[dict[str, Any]] = []
    supported_enriched_rows: list[dict[str, Any]] = []
    for row in mapped_frame.rows(named=True):
        execution = executions[row["ncbi_accession"]]
        final_accession = execution.final_accession or ""
        unsupported_accession = row["ncbi_accession"] in unsupported_executions
        enriched_rows.append(
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "resolved_release": resolved_release,
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
                "download_batch": execution.download_batch,
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

    # Copy once per taxon-accession pair, then reuse the recorded path for
    # duplicate rows that point to the same final payload within that taxon.
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

    return enriched_rows, supported_enriched_rows, per_taxon_rows, duplicate_counts


def resolve_exit_code(
    enriched_rows: list[dict[str, Any]],
) -> tuple[int, int, int]:
    """Return success count, failure count, and workflow exit code."""

    successful_count = len(
        {
            row["final_accession"]
            for row in enriched_rows
            if row["download_status"] != "failed" and row["final_accession"]
        },
    )
    failed_count = len(
        {
            row["gtdb_accession"]
            for row in enriched_rows
            if row["download_status"] == "failed"
        },
    )
    if failed_count == 0:
        return successful_count, failed_count, 0
    if successful_count > 0:
        return successful_count, failed_count, 6
    return successful_count, failed_count, 7


def materialise_real_run_outputs(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
    started_at: str,
    resolved_release: str,
    mapped_frame: pl.DataFrame,
    metadata_failures: tuple[CommandFailureRecord, ...],
    execution_result: DownloadExecutionResult,
    unsupported_executions: dict[str, AccessionExecution],
    secrets: tuple[str, ...],
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> int:
    """Copy payloads, write manifests, and return the final exit code."""

    logger.info("Writing output manifests to %s", run_directories.output_root)
    enriched_rows, supported_enriched_rows, per_taxon_rows, duplicate_counts = (
        build_enriched_output_rows(
            resolved_release,
            mapped_frame,
            execution_result,
            unsupported_executions,
            run_directories,
            logger,
        )
    )
    failure_rows = build_failure_rows(
        enriched_rows,
        {
            **execution_result.executions,
            **unsupported_executions,
        },
        metadata_failures,
        execution_result.shared_failures,
        secrets,
        shared_context_rows=supported_enriched_rows,
        suppressed_notes=suppressed_notes,
    )
    taxon_summary_rows = build_taxon_summary_rows(
        enriched_rows,
        duplicate_counts,
        run_directories,
    )
    successful_count, failed_count, exit_code = resolve_exit_code(enriched_rows)
    run_summary_rows = [
        build_run_summary_row(
            args,
            args.gtdb_release,
            resolved_release,
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
    logger.info(
        "Run finished: successful_accessions=%d failed_accessions=%d exit_code=%d",
        successful_count,
        failed_count,
        exit_code,
    )
    return exit_code

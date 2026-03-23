"""Dehydrated-download execution helpers for the GTDB workflow."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from gtdb_genomes.download import (
    build_batch_dehydrate_command,
    build_rehydrate_command,
    get_ordered_unique_accessions,
    get_rehydrate_workers,
    run_retryable_command,
    write_accession_input_file,
)
from gtdb_genomes.layout import LayoutError, RunDirectories, extract_archive
from gtdb_genomes.logging_utils import redact_command
from gtdb_genomes.subprocess_utils import build_datasets_subprocess_environment
from gtdb_genomes.workflow_execution_direct import execute_direct_accession_plans
from gtdb_genomes.workflow_execution_models import (
    AccessionExecution,
    AccessionPlan,
    DownloadExecutionResult,
)
from gtdb_genomes.workflow_execution_payloads import (
    build_batch_archive_path,
    build_batch_layout_failures,
    build_shared_failure_context,
    locate_batch_payload_directories,
)


if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


def fallback_batch_to_direct(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    batch_failures,
    rehydrate_workers_used: int,
) -> DownloadExecutionResult:
    """Fall back from a failed dehydrated batch workflow to direct downloads."""

    logger.warning(
        "Batch dehydrated download failed; falling back to batch direct downloads",
    )
    logger.info(
        "Starting direct fallback for %d accession plan(s)",
        len(plans),
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
        shared_failures=(batch_failures, *direct_result.shared_failures),
    )


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

    environment = build_datasets_subprocess_environment(args.ncbi_api_key)
    requested_accessions = get_ordered_unique_accessions(
        plan.download_request_accession for plan in plans
    )
    batch_attempted_accessions = ";".join(requested_accessions)
    logger.info(
        "dehydrated_batch: starting preferred_download for %d request accession(s)",
        len(requested_accessions),
    )
    affected_original_accessions = tuple(
        plan.original_accession for plan in plans
    )
    accession_file = write_accession_input_file(
        run_directories.working_root / "dehydrate_accessions.txt",
        requested_accessions,
    )
    archive_path = build_batch_archive_path(run_directories)
    download_command = build_batch_dehydrate_command(
        accession_file,
        archive_path,
        args.include,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(download_command, secrets))
    batch_download = run_retryable_command(
        download_command,
        stage="preferred_download",
        attempted_accession=batch_attempted_accessions,
        environment=environment,
    )
    if not batch_download.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                batch_download.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )
    logger.info("dehydrated_batch: download archive completed")

    extraction_root = run_directories.extracted_root / "dehydrated_batch"
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                build_batch_layout_failures(batch_download.failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=0,
        )

    rehydrate_workers = get_rehydrate_workers(args.threads)
    logger.info(
        "dehydrated_batch: starting rehydrate with %d worker(s)",
        rehydrate_workers,
    )
    rehydrate_command = build_rehydrate_command(
        extraction_root,
        rehydrate_workers,
        debug=args.debug,
    )
    logger.debug("Running %s", redact_command(rehydrate_command, secrets))
    rehydrate_result = run_retryable_command(
        rehydrate_command,
        stage="rehydrate",
        attempted_accession=batch_attempted_accessions,
        environment=environment,
    )
    if not rehydrate_result.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                batch_download.failures + rehydrate_result.failures,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )
    logger.info("dehydrated_batch: rehydrate completed")

    shared_failures = build_shared_failure_context(
        affected_original_accessions,
        batch_download.failures + rehydrate_result.failures,
        batch_attempted_accessions,
    )
    executions: dict[str, AccessionExecution] = {}
    try:
        payload_directories = locate_batch_payload_directories(
            extraction_root,
            tuple(plan.download_request_accession for plan in plans),
        )
        for plan in plans:
            payload = payload_directories[plan.download_request_accession]
            executions[plan.original_accession] = AccessionExecution(
                original_accession=plan.original_accession,
                final_accession=payload.final_accession,
                conversion_status=plan.conversion_status,
                download_status="downloaded",
                download_batch="dehydrated_batch",
                payload_directory=payload.directory,
                failures=(),
                request_accession_used=plan.download_request_accession,
            )
    except LayoutError as error:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_shared_failure_context(
                affected_original_accessions,
                build_batch_layout_failures(shared_failures.failures, error),
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
        )
    logger.info(
        "dehydrated_batch: completed with %d resolved accession(s)",
        len(executions),
    )

    return DownloadExecutionResult(
        executions=executions,
        method_used="dehydrate",
        download_concurrency_used=1,
        rehydrate_workers_used=rehydrate_workers,
        shared_failures=(shared_failures,) if shared_failures.failures else (),
    )

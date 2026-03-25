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
    SharedFailureContext,
)
from gtdb_genomes.workflow_execution_payloads import (
    build_batch_archive_path,
    build_batch_layout_failures,
    build_shared_failure_context,
    build_successful_execution,
    locate_batch_payload_directories,
    locate_partial_batch_payload_directories,
)


if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


def build_dehydrate_fallback_warning(
    fallback_plans: tuple[AccessionPlan, ...],
    resolved_executions: dict[str, AccessionExecution] | None = None,
) -> str:
    """Return the dehydrate-to-direct fallback warning for one batch."""

    fallback_count = len(fallback_plans)
    fallback_noun = "accession" if fallback_count == 1 else "accessions"
    fallback_verb = "is" if fallback_count == 1 else "are"
    resolved_count = 0 if resolved_executions is None else len(resolved_executions)
    resolved_noun = "accession" if resolved_count == 1 else "accessions"
    resolved_verb = "was" if resolved_count == 1 else "were"
    if resolved_count == 0:
        return (
            "Batch dehydrated download failed; "
            f"falling back to batch direct downloads for {fallback_count} "
            f"{fallback_noun}"
        )
    return (
        "Batch dehydrated download partially succeeded; "
        f"{resolved_count} {resolved_noun} {resolved_verb} resolved and "
        f"{fallback_count} unresolved {fallback_noun} {fallback_verb} "
        "falling back to batch direct downloads"
    )


def fallback_batch_to_direct(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    batch_failures: tuple[SharedFailureContext, ...],
    rehydrate_workers_used: int,
    resolved_executions: dict[str, AccessionExecution] | None = None,
) -> DownloadExecutionResult:
    """Fall back from a failed dehydrated batch workflow to direct downloads."""

    logger.warning(
        "%s",
        build_dehydrate_fallback_warning(
            plans,
            resolved_executions,
        ),
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
    executions = {} if resolved_executions is None else dict(resolved_executions)
    executions.update(direct_result.executions)
    return DownloadExecutionResult(
        executions=executions,
        method_used="dehydrate_fallback_direct",
        download_concurrency_used=direct_result.download_concurrency_used,
        rehydrate_workers_used=rehydrate_workers_used,
        shared_failures=batch_failures + direct_result.shared_failures,
    )


def build_optional_shared_failure_context(
    original_accessions: tuple[str, ...],
    failures,
    attempted_accession: str,
) -> tuple[SharedFailureContext, ...]:
    """Return one shared failure context only when there is content to record."""

    if not original_accessions or not failures:
        return ()
    return (
        build_shared_failure_context(
            original_accessions,
            failures,
            attempted_accession,
        ),
    )


def resolve_partial_dehydrate_executions(
    plans: tuple[AccessionPlan, ...],
    extraction_root,
) -> tuple[dict[str, AccessionExecution], tuple[AccessionPlan, ...], dict[str, str]]:
    """Resolve any available dehydrated payloads without failing atomically."""

    resolution = locate_partial_batch_payload_directories(
        extraction_root,
        tuple(plan.download_request_accession for plan in plans),
    )
    executions: dict[str, AccessionExecution] = {}
    unresolved_plans: list[AccessionPlan] = []
    for plan in plans:
        payload = resolution.resolved_payloads.get(plan.download_request_accession)
        if payload is None:
            unresolved_plans.append(plan)
            continue
        executions[plan.original_accession] = build_successful_execution(
            plan,
            payload.final_accession,
            "downloaded",
            "dehydrated_batch",
            plan.download_request_accession,
            payload.directory,
            (),
        )
    return executions, tuple(unresolved_plans), resolution.unresolved_messages


def build_unresolved_layout_failure_context(
    unresolved_plans: tuple[AccessionPlan, ...],
    unresolved_messages: dict[str, str],
    batch_attempted_accessions: str,
) -> tuple[SharedFailureContext, ...]:
    """Return one scoped layout failure context for unresolved dehydrated rows."""

    if not unresolved_plans:
        return ()
    unresolved_text = "; ".join(
        unresolved_messages[plan.download_request_accession]
        for plan in unresolved_plans
        if plan.download_request_accession in unresolved_messages
    )
    if not unresolved_text:
        return ()
    return (
        build_shared_failure_context(
            tuple(plan.original_accession for plan in unresolved_plans),
            build_batch_layout_failures((), LayoutError(unresolved_text)),
            batch_attempted_accessions,
        ),
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
        logger=logger,
        progress_label="dehydrated_batch: preferred_download",
    )
    if not batch_download.succeeded:
        return fallback_batch_to_direct(
            plans,
            args,
            run_directories,
            logger,
            batch_failures=build_optional_shared_failure_context(
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
        resolved_executions, unresolved_plans, unresolved_messages = (
            resolve_partial_dehydrate_executions(plans, extraction_root)
        )
        shared_failures = build_optional_shared_failure_context(
            affected_original_accessions,
            batch_download.failures,
            batch_attempted_accessions,
        ) + build_unresolved_layout_failure_context(
            unresolved_plans,
            unresolved_messages
            if unresolved_messages
            else {
                plan.download_request_accession: str(error)
                for plan in unresolved_plans
            },
            batch_attempted_accessions,
        )
        if not unresolved_plans:
            return DownloadExecutionResult(
                executions=resolved_executions,
                method_used="dehydrate",
                download_concurrency_used=1,
                rehydrate_workers_used=0,
                shared_failures=shared_failures,
            )
        return fallback_batch_to_direct(
            unresolved_plans,
            args,
            run_directories,
            logger,
            batch_failures=shared_failures,
            rehydrate_workers_used=0,
            resolved_executions=resolved_executions,
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
        logger=logger,
        progress_label="dehydrated_batch: rehydrate",
    )
    if not rehydrate_result.succeeded:
        resolved_executions, unresolved_plans, _ = resolve_partial_dehydrate_executions(
            plans,
            extraction_root,
        )
        shared_failures = build_optional_shared_failure_context(
            affected_original_accessions,
            batch_download.failures,
            batch_attempted_accessions,
        ) + build_optional_shared_failure_context(
            tuple(plan.original_accession for plan in unresolved_plans),
            rehydrate_result.failures,
            batch_attempted_accessions,
        )
        if not unresolved_plans:
            return DownloadExecutionResult(
                executions=resolved_executions,
                method_used="dehydrate",
                download_concurrency_used=1,
                rehydrate_workers_used=rehydrate_workers,
                shared_failures=shared_failures,
            )
        return fallback_batch_to_direct(
            unresolved_plans,
            args,
            run_directories,
            logger,
            batch_failures=shared_failures,
            rehydrate_workers_used=rehydrate_workers,
            resolved_executions=resolved_executions,
        )
    logger.info("dehydrated_batch: rehydrate completed")

    shared_failures = build_optional_shared_failure_context(
        affected_original_accessions,
        batch_download.failures + rehydrate_result.failures,
        batch_attempted_accessions,
    )
    resolved_executions, unresolved_plans, unresolved_messages = (
        resolve_partial_dehydrate_executions(plans, extraction_root)
    )
    if unresolved_plans:
        return fallback_batch_to_direct(
            unresolved_plans,
            args,
            run_directories,
            logger,
            batch_failures=shared_failures + build_unresolved_layout_failure_context(
                unresolved_plans,
                unresolved_messages,
                batch_attempted_accessions,
            ),
            rehydrate_workers_used=rehydrate_workers,
            resolved_executions=resolved_executions,
        )
    logger.info(
        "dehydrated_batch: completed with %d resolved accession(s)",
        len(resolved_executions),
    )

    return DownloadExecutionResult(
        executions=resolved_executions,
        method_used="dehydrate",
        download_concurrency_used=1,
        rehydrate_workers_used=rehydrate_workers,
        shared_failures=shared_failures,
    )

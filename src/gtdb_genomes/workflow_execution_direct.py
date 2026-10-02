"""Direct-download execution helpers for the GTDB workflow."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from gtdb_genomes.download import (
    CommandFailureRecord,
    build_direct_batch_download_command,
    run_retryable_command,
    write_accession_input_file,
)
from gtdb_genomes.layout import LayoutError, RunDirectories, extract_archive
from gtdb_genomes.logging_utils import redact_command
from gtdb_genomes.subprocess_utils import (
    build_datasets_subprocess_environment,
    get_stage_display_name,
)
from gtdb_genomes.workflow_execution_batches import (
    RequestPlanGroups,
    build_next_wave_batches,
    group_plans_by_download_request_accession,
)
from gtdb_genomes.workflow_execution_models import (
    AccessionExecution,
    AccessionPlan,
    DirectBatchPhaseResult,
    DownloadExecutionResult,
    SharedFailureContext,
)
from gtdb_genomes.workflow_execution_payloads import (
    build_direct_batch_archive_path,
    build_direct_layout_failure,
    build_layout_failure,
    build_phase_failed_executions,
    build_shared_failure_context,
    build_successful_execution,
    locate_partial_batch_payload_directories,
)


if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


NORMAL_DIRECT_BATCH_MAX_ATTEMPTS = 4
SUPPRESSED_DIRECT_BATCH_MAX_ATTEMPTS = 2
DIRECT_DOWNLOAD_STAGE = "download"
FALLBACK_DOWNLOAD_STAGE = "fallback_download"


def append_shared_failures(
    original_accessions: tuple[str, ...],
    failures: tuple[CommandFailureRecord, ...],
    attempted_accession: str,
    *,
    shared_failures: list[SharedFailureContext],
) -> None:
    """Record one shared batch failure for later audit and failure propagation."""

    shared_failures.append(
        build_shared_failure_context(
            original_accessions,
            failures,
            attempted_accession,
        ),
    )


def propagate_shared_failures_to_failed_plans(
    plans: tuple[AccessionPlan, ...],
    shared_failure_contexts: tuple[SharedFailureContext, ...],
    *,
    failure_history: dict[str, list[CommandFailureRecord]],
) -> None:
    """Copy shared batch failures into failed plans before final execution build."""

    failed_accessions = {
        plan.original_accession
        for plan in plans
    }
    if not failed_accessions:
        return
    for shared_failure_context in shared_failure_contexts:
        for original_accession in shared_failure_context.affected_original_accessions:
            if original_accession in failed_accessions:
                failure_history[original_accession].extend(
                    shared_failure_context.failures,
                )


def get_direct_group_max_attempts(
    plan_groups: RequestPlanGroups,
) -> int:
    """Return the workflow retry budget for one direct request group set."""

    if any(
        not plan.is_suppressed
        for _, grouped_plans in plan_groups
        for plan in grouped_plans
    ):
        return NORMAL_DIRECT_BATCH_MAX_ATTEMPTS
    return SUPPRESSED_DIRECT_BATCH_MAX_ATTEMPTS


def count_batch_request_accessions(
    batches: tuple[RequestPlanGroups, ...],
) -> int:
    """Return the request-accession count covered by one batch tuple."""

    return sum(len(batch_groups) for batch_groups in batches)


def run_direct_batch_phase(
    plan_groups: RequestPlanGroups,
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    *,
    attempt_index: int,
    batch_stage: str,
    batch_prefix: str,
    success_status: str,
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
    last_request_accessions: dict[str, str],
    batch_label_counter: list[int],
) -> DirectBatchPhaseResult:
    """Execute one direct batch once for the current workflow wave."""

    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    environment = build_datasets_subprocess_environment(args.ncbi_api_key)
    batch_stage_display = get_stage_display_name(batch_stage)
    max_attempts = get_direct_group_max_attempts(plan_groups)
    can_retry = attempt_index < max_attempts
    final_status = "retry_scheduled" if can_retry else "retry_exhausted"
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []

    batch_label_counter[0] += 1
    batch_label = f"{batch_prefix}_{batch_label_counter[0]}"
    pending_request_accessions = tuple(
        request_accession for request_accession, _ in plan_groups
    )
    logger.info(
        "%s: starting %s for %d request accession(s)",
        batch_label,
        batch_stage_display,
        len(pending_request_accessions),
    )
    affected_original_accessions = tuple(
        plan.original_accession
        for _, grouped_plans in plan_groups
        for plan in grouped_plans
    )
    for original_accession in affected_original_accessions:
        last_download_batches[original_accession] = batch_label
    for request_accession, grouped_plans in plan_groups:
        for plan in grouped_plans:
            last_request_accessions[plan.original_accession] = request_accession
    accession_file = write_accession_input_file(
        run_directories.working_root / f"{batch_label}.txt",
        pending_request_accessions,
    )
    archive_path = build_direct_batch_archive_path(
        run_directories,
        batch_label,
    )
    download_command = build_direct_batch_download_command(
        accession_file,
        archive_path,
        args.include,
        debug=args.debug,
        download_options=args.ncbi_download_options,
    )
    logger.debug(
        "Running %s",
        redact_command(download_command, secrets),
    )
    batch_attempted_accessions = ";".join(pending_request_accessions)
    batch_result = run_retryable_command(
        download_command,
        stage=batch_stage,
        attempted_accession=batch_attempted_accessions,
        environment=environment,
        logger=logger,
        progress_label=f"{batch_label}: {batch_stage_display}",
    )
    if not batch_result.succeeded:
        logger.warning(
            "%s: %s failed before payload extraction",
            batch_label,
            batch_stage_display,
        )
        append_shared_failures(
            affected_original_accessions,
            batch_result.failures,
            batch_attempted_accessions,
            shared_failures=shared_failures,
        )
        return DirectBatchPhaseResult(
            executions=executions,
            unresolved_groups=plan_groups,
            shared_failures=tuple(shared_failures),
        )

    if batch_result.failures:
        append_shared_failures(
            affected_original_accessions,
            batch_result.failures,
            batch_attempted_accessions,
            shared_failures=shared_failures,
        )

    extraction_root = run_directories.extracted_root / batch_label
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        logger.warning(
            "%s: extraction failed after %s",
            batch_label,
            batch_stage_display,
        )
        append_shared_failures(
            affected_original_accessions,
            (
                build_direct_layout_failure(
                    str(error),
                    batch_attempted_accessions,
                    attempt_index,
                    max_attempts,
                    final_status,
                ),
            ),
            batch_attempted_accessions,
            shared_failures=shared_failures,
        )
        return DirectBatchPhaseResult(
            executions=executions,
            unresolved_groups=plan_groups,
            shared_failures=tuple(shared_failures),
        )

    resolution = locate_partial_batch_payload_directories(
        extraction_root,
        pending_request_accessions,
    )
    unresolved_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []

    for request_accession, grouped_plans in plan_groups:
        payload = resolution.resolved_payloads.get(request_accession)
        if payload is not None:
            for plan in grouped_plans:
                plan_failures = tuple(failure_history[plan.original_accession])
                executions[plan.original_accession] = build_successful_execution(
                    plan,
                    payload.final_accession,
                    success_status,
                    batch_label,
                    request_accession,
                    payload.directory,
                    plan_failures,
                )
            continue

        failure_record = build_direct_layout_failure(
            resolution.unresolved_messages[request_accession],
            request_accession,
            attempt_index,
            max_attempts,
            final_status,
        )
        for plan in grouped_plans:
            failure_history[plan.original_accession].append(failure_record)
        unresolved_groups.append((request_accession, grouped_plans))

    logger.info(
        "%s: completed with %d resolved and %d pending request accession(s)",
        batch_label,
        len(resolution.resolved_payloads),
        len(unresolved_groups),
    )
    return DirectBatchPhaseResult(
        executions=executions,
        unresolved_groups=tuple(unresolved_groups),
        shared_failures=tuple(shared_failures),
    )


def execute_direct_wave_phase(
    initial_groups: RequestPlanGroups,
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    *,
    batch_stage: str,
    batch_prefix: str,
    success_status: str,
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
    last_request_accessions: dict[str, str],
    batch_label_counter: list[int],
) -> DirectBatchPhaseResult:
    """Execute one direct phase using wave-based workflow retries."""

    if not initial_groups:
        return DirectBatchPhaseResult(
            executions={},
            unresolved_groups=(),
            shared_failures=(),
        )

    current_wave_batches: tuple[RequestPlanGroups, ...] = (initial_groups,)
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []
    unresolved_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []
    wave_index = 1
    batch_stage_display = get_stage_display_name(batch_stage)

    while current_wave_batches:
        logger.info(
            "%s wave %d: starting %d batch(es) covering %d request accession(s)",
            batch_stage_display,
            wave_index,
            len(current_wave_batches),
            count_batch_request_accessions(current_wave_batches),
        )
        next_wave_batches: list[RequestPlanGroups] = []
        next_wave_unresolved_count = 0

        for batch_groups in current_wave_batches:
            phase_result = run_direct_batch_phase(
                batch_groups,
                args,
                run_directories,
                logger,
                attempt_index=wave_index,
                batch_stage=batch_stage,
                batch_prefix=batch_prefix,
                success_status=success_status,
                failure_history=failure_history,
                last_download_batches=last_download_batches,
                last_request_accessions=last_request_accessions,
                batch_label_counter=batch_label_counter,
            )
            executions.update(phase_result.executions)
            shared_failures.extend(phase_result.shared_failures)
            if not phase_result.unresolved_groups:
                continue

            next_wave_unresolved_count += len(phase_result.unresolved_groups)
            if wave_index < get_direct_group_max_attempts(phase_result.unresolved_groups):
                next_wave_batches.extend(
                    build_next_wave_batches(phase_result.unresolved_groups),
                )
                continue
            unresolved_groups.extend(phase_result.unresolved_groups)

        logger.info(
            "%s wave %d: completed with %d next-wave batch(es) covering %d unresolved request accession(s)",
            batch_stage_display,
            wave_index,
            len(next_wave_batches),
            next_wave_unresolved_count,
        )
        current_wave_batches = tuple(next_wave_batches)
        wave_index += 1

    return DirectBatchPhaseResult(
        executions=executions,
        unresolved_groups=tuple(unresolved_groups),
        shared_failures=tuple(shared_failures),
    )


def execute_direct_accession_plans(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
) -> DownloadExecutionResult:
    """Execute direct downloads with batch retries and original fallback."""

    if not plans:
        return DownloadExecutionResult(
            executions={},
            method_used="direct",
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )
    plan_groups = group_plans_by_download_request_accession(plans)
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []
    failure_history = defaultdict(list)
    last_download_batches: dict[str, str] = {
        plan.original_accession: plan.original_accession for plan in plans
    }
    last_request_accessions: dict[str, str] = {
        plan.original_accession: plan.download_request_accession for plan in plans
    }
    direct_batch_counter = [0]

    direct_phase = execute_direct_wave_phase(
        plan_groups,
        args,
        run_directories,
        logger,
        batch_stage=DIRECT_DOWNLOAD_STAGE,
        batch_prefix="direct_batch",
        success_status="downloaded",
        failure_history=failure_history,
        last_download_batches=last_download_batches,
        last_request_accessions=last_request_accessions,
        batch_label_counter=direct_batch_counter,
    )
    executions.update(direct_phase.executions)
    shared_failures.extend(direct_phase.shared_failures)

    direct_unresolved_plans: list[AccessionPlan] = []
    fallback_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []

    for _, grouped_plans in direct_phase.unresolved_groups:
        for plan in grouped_plans:
            direct_unresolved_plans.append(plan)
            if plan.conversion_status == "paired_to_gca" and plan.fallback_allowed:
                fallback_groups.append((plan.original_accession, (plan,)))
            elif plan.conversion_status == "paired_to_gca":
                logger.warning(
                    "Skipping original fallback for %s: NCBI eligibility was not confirmed",
                    plan.original_accession,
                )
    failed_after_direct = tuple(
        plan for plan in direct_unresolved_plans
        if plan.conversion_status != "paired_to_gca" or not plan.fallback_allowed
    )
    propagate_shared_failures_to_failed_plans(
        failed_after_direct,
        direct_phase.shared_failures,
        failure_history=failure_history,
    )
    executions.update(
        build_phase_failed_executions(
            failed_after_direct,
            failure_history,
            last_download_batches,
            last_request_accessions,
        ),
    )

    if fallback_groups:
        fallback_batch_counter = [0]
        fallback_phase = execute_direct_wave_phase(
            tuple(fallback_groups),
            args,
            run_directories,
            logger,
            batch_stage=FALLBACK_DOWNLOAD_STAGE,
            batch_prefix="direct_fallback_batch",
            success_status="downloaded_after_fallback",
            failure_history=failure_history,
            last_download_batches=last_download_batches,
            last_request_accessions=last_request_accessions,
            batch_label_counter=fallback_batch_counter,
        )
        executions.update(fallback_phase.executions)
        shared_failures.extend(fallback_phase.shared_failures)
        unresolved_fallback_plans = tuple(
            plan
            for _, grouped_plans in fallback_phase.unresolved_groups
            for plan in grouped_plans
        )
        propagate_shared_failures_to_failed_plans(
            unresolved_fallback_plans,
            direct_phase.shared_failures + fallback_phase.shared_failures,
            failure_history=failure_history,
        )
        executions.update(
            build_phase_failed_executions(
                unresolved_fallback_plans,
                failure_history,
                last_download_batches,
                last_request_accessions,
            ),
        )

    return DownloadExecutionResult(
        executions=executions,
        method_used="direct",
        download_concurrency_used=1,
        rehydrate_workers_used=0,
        shared_failures=tuple(shared_failures),
    )

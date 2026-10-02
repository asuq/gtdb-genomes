"""Thin orchestration entrypoint for the GTDB workflow."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gtdb_genomes.download import DEFAULT_REQUESTED_DOWNLOAD_METHOD, select_download_method
from gtdb_genomes.workflow_eligibility import resolve_eligible_plans
from gtdb_genomes.layout import (
    cleanup_interrupted_output_directories,
    cleanup_working_directories,
    initialise_run_directories,
)
from gtdb_genomes.logging_utils import close_logger, configure_logging, redact_text
from gtdb_genomes.metadata import MetadataLookupError
from gtdb_genomes.release_resolver import BundledDataError
import gtdb_genomes.workflow_execution as workflow_execution
import gtdb_genomes.workflow_outputs as workflow_outputs
import gtdb_genomes.workflow_planning as workflow_planning
import gtdb_genomes.workflow_selection as workflow_selection


if TYPE_CHECKING:
    import logging
    from gtdb_genomes.cli import CliArgs
    from gtdb_genomes.layout import RunDirectories


PLANNING_FAILURE_EXIT_CODE = 7
OUTPUT_MATERIALISATION_FAILURE_EXIT_CODE = 8
UNEXPECTED_INTERNAL_FAILURE_EXIT_CODE = 9
USER_INTERRUPT_EXIT_CODE = 130


def log_run_start(
    logger: logging.Logger,
    args: CliArgs,
) -> None:
    """Log the user-facing start summary for workflow run."""

    logger.info(
        "Starting run: release=%s taxa=%d outdir=%s dry_run=%s",
        args.gtdb_release,
        len(args.gtdb_taxa),
        args.outdir,
        str(args.dry_run).lower(),
    )


def log_output_materialisation_failure(
    logger: logging.Logger,
    error: OSError | shutil.Error,
    secrets: tuple[str, ...],
) -> int:
    """Log one structured output-materialisation failure and return exit code 8."""

    logger.error(
        "Real-run output materialisation failed: %s",
        redact_text(str(error), secrets),
    )
    return OUTPUT_MATERIALISATION_FAILURE_EXIT_CODE


def log_planning_staging_failure(
    logger: logging.Logger,
    error: OSError | shutil.Error,
    secrets: tuple[str, ...],
) -> int:
    """Log one planning-stage local filesystem failure and return exit code 7."""

    logger.error(
        "Workflow planning failed due to local staging error: %s",
        redact_text(str(error), secrets),
    )
    return PLANNING_FAILURE_EXIT_CODE


def log_unexpected_internal_failure(
    logger: logging.Logger,
    error: Exception,
    secrets: tuple[str, ...],
) -> int:
    """Log one unexpected internal failure and return exit code 9."""

    logger.error(
        "Unexpected internal failure (%s): %s",
        type(error).__name__,
        redact_text(str(error), secrets),
    )
    return UNEXPECTED_INTERNAL_FAILURE_EXIT_CODE


def cleanup_run_directories(
    logger: logging.Logger,
    run_directories: RunDirectories,
) -> None:
    """Clean up the working tree and log any cleanup failure."""

    cleanup_error = cleanup_working_directories(run_directories)
    if cleanup_error is not None:
        logger.warning(
            "Could not remove working directory %s: %s",
            run_directories.working_root,
            cleanup_error,
        )


def cleanup_interrupted_run_directories(
    logger: logging.Logger,
    run_directories: RunDirectories,
) -> None:
    """Clean up interrupted real-run directories and log any cleanup failure."""

    cleanup_error = cleanup_interrupted_output_directories(run_directories)
    if cleanup_error is not None:
        logger.warning(
            "Could not finish interrupted-run cleanup under %s: %s",
            run_directories.output_root,
            cleanup_error,
        )


def log_user_interrupt(logger: logging.Logger) -> int:
    """Log one user interrupt and return the conventional exit code."""

    logger.warning("Run interrupted by user")
    return USER_INTERRUPT_EXIT_CODE


def run_workflow(args: CliArgs) -> int:
    """Run the workflow and return the process exit code."""

    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    logger, _ = configure_logging(
        debug=args.debug,
        dry_run=args.dry_run,
        secrets=secrets,
    )
    started_at = datetime.now(UTC).isoformat()
    log_run_start(logger, args)
    run_directories: RunDirectories | None = None

    try:
        resolution, selected_frame, supported_selected_frame, unsupported_selected_frame = (
            workflow_selection.prepare_selection_frames(args, logger)
        )
    except KeyboardInterrupt:
        exit_code = log_user_interrupt(logger)
        close_logger(logger)
        return exit_code
    except BundledDataError as error:
        logger.error("%s", error)
        close_logger(logger)
        return 3
    except (OSError, shutil.Error) as error:
        exit_code = log_planning_staging_failure(logger, error, secrets)
        close_logger(logger)
        return exit_code

    try:
        zero_match_exit, zero_match_logger = workflow_selection.handle_zero_match_exit(
            args,
            logger,
            resolution,
            selected_frame,
            started_at,
        )
    except KeyboardInterrupt:
        exit_code = log_user_interrupt(logger)
        close_logger(logger)
        return exit_code
    except (OSError, shutil.Error) as error:
        exit_code = log_output_materialisation_failure(logger, error, secrets)
        close_logger(logger)
        return exit_code
    if zero_match_exit is not None:
        if zero_match_logger is not None:
            close_logger(zero_match_logger)
        return zero_match_exit

    if not unsupported_selected_frame.is_empty():
        logger.warning(
            workflow_selection.build_unsupported_uba_warning(
                unsupported_selected_frame,
            ),
        )

    try:
        workflow_selection.run_supported_preflight(supported_selected_frame)
        (
            mapped_frame,
            suppressed_notes,
            accession_plans,
            decision_method,
        ) = (
            workflow_planning.prepare_planning_inputs(
                supported_selected_frame,
                unsupported_selected_frame,
                args,
                logger,
            )
        )
        accession_plans, eligibility_executions = resolve_eligible_plans(
            accession_plans, args, logger,
        )
        if args.ncbi_filters:
            decision_method = (
                select_download_method(len({
                    plan.download_request_accession for plan in accession_plans
                })).method_used
                if accession_plans else DEFAULT_REQUESTED_DOWNLOAD_METHOD
            )
            logger.info(
                "After NCBI criteria, selected %s for %d eligible request accession(s)",
                decision_method,
                len({plan.download_request_accession for plan in accession_plans}),
            )
    except KeyboardInterrupt:
        exit_code = log_user_interrupt(logger)
        close_logger(logger)
        return exit_code
    except MetadataLookupError as error:
        logger.error("%s", redact_text(str(error), secrets))
        close_logger(logger)
        return 5
    except (OSError, shutil.Error) as error:
        exit_code = log_planning_staging_failure(logger, error, secrets)
        close_logger(logger)
        return exit_code

    planning_warning = workflow_planning.build_planning_suppressed_warning(
        suppressed_notes,
    )
    if planning_warning is not None:
        logger.warning("%s", planning_warning)
        planning_debug_detail = workflow_planning.build_planning_suppressed_debug_detail(
            suppressed_notes,
        )
        if planning_debug_detail is not None:
            logger.debug("%s", planning_debug_detail)
    explicit_pairing_warning = workflow_planning.build_explicit_pairing_conflict_warning(
        mapped_frame,
    )
    if explicit_pairing_warning is not None:
        logger.warning("%s", explicit_pairing_warning)

    # Dry-runs stop after planning and report the planned workload
    if args.dry_run:
        logger.info(
            "Dry-run finished: planned_supported_accessions=%d unsupported_legacy_accessions=%d",
            len(accession_plans),
            workflow_selection.count_unique_accessions(unsupported_selected_frame),
        )
        close_logger(logger)
        return 5 if any(
            execution.download_status == "failed"
            for execution in eligibility_executions.values()
        ) else 0

    # Real runs execute downloads and materialise outputs
    run_directories: RunDirectories
    try:
        run_directories = initialise_run_directories(args.outdir)
        logger = workflow_outputs.configure_output_logger(args, logger, run_directories)
    except KeyboardInterrupt:
        exit_code = log_user_interrupt(logger)
        if run_directories is not None and not args.keep_temp:
            cleanup_interrupted_run_directories(logger, run_directories)
        close_logger(logger)
        return exit_code
    except (OSError, shutil.Error) as error:
        exit_code = log_output_materialisation_failure(logger, error, secrets)
        close_logger(logger)
        return exit_code
    try:
        if accession_plans:
            execution_result = workflow_execution.execute_accession_plans(
                accession_plans,
                args,
                decision_method,
                run_directories,
                logger,
                secrets,
            )
        else:
            execution_result = workflow_execution.DownloadExecutionResult(
                executions={},
                method_used=DEFAULT_REQUESTED_DOWNLOAD_METHOD,
                download_concurrency_used=0,
                rehydrate_workers_used=0,
                shared_failures=(),
            )
        execution_result.executions.update(eligibility_executions)
        unsupported_executions = workflow_selection.build_unsupported_executions(
            unsupported_selected_frame,
        )
        try:
            exit_code = workflow_outputs.materialise_real_run_outputs(
                args,
                logger,
                run_directories,
                started_at,
                resolution,
                mapped_frame,
                execution_result,
                unsupported_executions,
                secrets,
                suppressed_notes=suppressed_notes,
            )
        except (OSError, shutil.Error) as error:
            exit_code = log_output_materialisation_failure(logger, error, secrets)
        else:
            failed_original_accessions = tuple(
                original_accession
                for original_accession, execution in execution_result.executions.items()
                if execution.download_status == "failed"
            )
            failed_suppressed_warning = workflow_planning.build_failed_suppressed_warning(
                suppressed_notes,
                failed_original_accessions,
            )
            if failed_suppressed_warning is not None:
                logger.warning("%s", failed_suppressed_warning)
                failed_suppressed_debug_detail = (
                    workflow_planning.build_failed_suppressed_debug_detail(
                        suppressed_notes,
                        failed_original_accessions,
                    )
                )
                if failed_suppressed_debug_detail is not None:
                    logger.debug("%s", failed_suppressed_debug_detail)
    except KeyboardInterrupt:
        exit_code = log_user_interrupt(logger)
        if run_directories is not None and not args.keep_temp:
            cleanup_interrupted_run_directories(logger, run_directories)
        close_logger(logger)
        return exit_code
    except Exception as error:
        exit_code = log_unexpected_internal_failure(logger, error, secrets)
        if not args.keep_temp:
            cleanup_run_directories(logger, run_directories)
        close_logger(logger)
        return exit_code
    if not args.keep_temp:
        cleanup_run_directories(logger, run_directories)
    close_logger(logger)
    return exit_code

"""Thin orchestration entrypoint for the GTDB workflow."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from gtdb_genomes.download import DEFAULT_REQUESTED_DOWNLOAD_METHOD, PreviewError
from gtdb_genomes.layout import cleanup_working_directories, initialise_run_directories
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


OUTPUT_MATERIALISATION_FAILURE_EXIT_CODE = 8


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


def run_workflow(args: CliArgs) -> int:
    """Run the workflow and return the process exit code."""

    logger, _ = configure_logging(debug=args.debug, dry_run=args.dry_run)
    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    started_at = datetime.now(UTC).isoformat()
    log_run_start(logger, args)

    try:
        # Selection and early preflight
        resolution, selected_frame, supported_selected_frame, unsupported_selected_frame = (
            workflow_selection.prepare_selection_frames(args, logger)
        )
        workflow_selection.run_early_dry_run_unzip_check(args, logger)
        zero_match_exit, zero_match_logger = workflow_selection.handle_zero_match_exit(
            args,
            logger,
            resolution,
            selected_frame,
            started_at,
        )
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

        # Supported-accession planning
        workflow_selection.run_supported_preflight(args, supported_selected_frame)
        (
            mapped_frame,
            metadata_shared_failures,
            suppressed_notes,
            accession_plans,
            decision_method,
        ) = (
            workflow_planning.prepare_planning_inputs(
                supported_selected_frame,
                unsupported_selected_frame,
                args,
                logger,
                secrets,
            )
        )
        planning_warning = workflow_planning.build_planning_suppressed_warning(
            suppressed_notes,
        )
        if planning_warning is not None:
            logger.warning("%s", planning_warning)
    except BundledDataError as error:
        logger.error("%s", error)
        close_logger(logger)
        return 3
    except MetadataLookupError as error:
        logger.error("%s", redact_text(str(error), secrets))
        close_logger(logger)
        return 5
    except PreviewError as error:
        logger.error("%s", redact_text(str(error), secrets))
        close_logger(logger)
        return 5

    # Dry-runs stop after planning and report the planned workload
    if args.dry_run:
        logger.info(
            "Dry-run finished: planned_supported_accessions=%d unsupported_legacy_accessions=%d",
            len(accession_plans),
            workflow_selection.count_unique_accessions(unsupported_selected_frame),
        )
        close_logger(logger)
        return 0

    # Real runs execute downloads and materialise outputs
    run_directories = initialise_run_directories(args.outdir)
    logger = workflow_outputs.configure_output_logger(args, logger, run_directories)
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
            metadata_shared_failures,
            execution_result,
            unsupported_executions,
            secrets,
            suppressed_notes=suppressed_notes,
        )
    except (OSError, shutil.Error) as error:
        logger.error(
            "Real-run output materialisation failed: %s",
            redact_text(str(error), secrets),
        )
        exit_code = OUTPUT_MATERIALISATION_FAILURE_EXIT_CODE
    else:
        failed_suppressed_warning = workflow_planning.build_failed_suppressed_warning(
            suppressed_notes,
            tuple(
                original_accession
                for original_accession, execution in execution_result.executions.items()
                if execution.download_status == "failed"
            ),
        )
        if failed_suppressed_warning is not None:
            logger.warning("%s", failed_suppressed_warning)
    if not args.keep_temp:
        cleanup_error = cleanup_working_directories(run_directories)
        if cleanup_error is not None:
            logger.warning(
                "Could not remove working directory %s: %s",
                run_directories.working_root,
                cleanup_error,
            )
    close_logger(logger)
    return exit_code

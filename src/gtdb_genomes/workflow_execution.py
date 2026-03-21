"""Public workflow execution API with split internal implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gtdb_genomes.workflow_execution_dehydrate import (
    execute_batch_dehydrate_plans,
    fallback_batch_to_direct,
)
from gtdb_genomes.workflow_execution_direct import (
    execute_direct_accession_plans,
    group_plans_by_download_request_accession,
)
from gtdb_genomes.workflow_execution_models import (
    AccessionExecution,
    AccessionPlan,
    DirectBatchPhaseResult,
    DownloadExecutionResult,
    PartialBatchPayloadResolution,
    ResolvedPayloadDirectory,
    SharedFailureContext,
)
from gtdb_genomes.workflow_execution_payloads import (
    attach_attempted_accession,
    build_batch_archive_path,
    build_batch_layout_failures,
    build_direct_batch_archive_path,
    build_direct_layout_failure,
    build_failed_execution,
    build_layout_failure,
    build_phase_failed_executions,
    build_resolved_payload_directory,
    build_shared_failure_context,
    build_successful_execution,
    collect_payload_directories,
    collect_root_payload_directories,
    extract_download_payload,
    has_accession_named_parent,
    locate_accession_payload_directory,
    locate_batch_payload_directories,
    locate_partial_batch_payload_directories,
)


if TYPE_CHECKING:
    import logging

    from gtdb_genomes.cli import CliArgs
    from gtdb_genomes.layout import RunDirectories


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

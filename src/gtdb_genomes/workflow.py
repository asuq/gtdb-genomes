"""End-to-end workflow execution for gtdb-genomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, UTC
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
import uuid

import polars as pl

from gtdb_genomes.download import (
    CommandFailureRecord,
    PreviewError,
    build_batch_dehydrate_command,
    build_direct_batch_download_command,
    build_preview_command,
    build_rehydrate_command,
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
    build_download_request_accession,
    build_summary_command,
    get_assembly_accession_stem,
    parse_assembly_accession,
    parse_assembly_accession_stem,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.preflight import check_required_tools, get_required_tools
from gtdb_genomes.release_resolver import (
    BundledDataError,
    ReleaseResolution,
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
    selected_accession: str
    download_request_accession: str
    conversion_status: str


@dataclass(slots=True)
class AccessionExecution:
    """The materialised download outcome for one accession plan."""

    original_accession: str
    final_accession: str | None
    conversion_status: str
    download_status: str
    download_batch: str
    payload_directory: Path | None
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class DownloadExecutionResult:
    """The realised download execution details for one run."""

    executions: dict[str, AccessionExecution]
    method_used: str
    download_concurrency_used: int
    rehydrate_workers_used: int
    shared_failures: tuple["SharedFailureContext", ...] = ()


@dataclass(frozen=True, slots=True)
class ResolvedPayloadDirectory:
    """The extracted payload directory and its realised accession."""

    final_accession: str
    directory: Path


@dataclass(slots=True)
class SharedFailureContext:
    """Shared failure history scoped to one affected accession subset."""

    affected_original_accessions: tuple[str, ...]
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class PartialBatchPayloadResolution:
    """Resolved and unresolved payloads for one extracted batch archive."""

    resolved_payloads: dict[str, ResolvedPayloadDirectory]
    unresolved_messages: dict[str, str]


@dataclass(slots=True)
class DirectBatchPhaseResult:
    """Accumulated results from one direct batch phase."""

    executions: dict[str, AccessionExecution]
    unresolved_groups: tuple[tuple[str, tuple[AccessionPlan, ...]], ...]
    shared_failures: tuple[SharedFailureContext, ...]


@dataclass(slots=True)
class WorkflowSelectionPhase:
    """Selected rows and support split for one workflow run."""

    resolution: ReleaseResolution
    selected_frame: pl.DataFrame
    supported_selected_frame: pl.DataFrame
    unsupported_selected_frame: pl.DataFrame


@dataclass(slots=True)
class WorkflowPlanningPhase:
    """Planned download inputs after preference resolution and preview."""

    mapped_frame: pl.DataFrame
    metadata_failures: tuple[CommandFailureRecord, ...]
    accession_plans: tuple[AccessionPlan, ...]
    decision_method: str


UNSUPPORTED_UBA_PREFIX = "UBA"
UNSUPPORTED_UBA_BIOPROJECT = "PRJNA417962"
UNSUPPORTED_UBA_WARNING_EXAMPLES = 5
MAX_DIRECT_BATCH_PASSES = 4


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


def build_accession_plans(
    mapped_frame: pl.DataFrame,
    *,
    prefer_genbank: bool,
    version_fixed: bool,
) -> tuple[AccessionPlan, ...]:
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
            selected_accession=row["final_accession"],
            download_request_accession=build_download_request_accession(
                row["final_accession"],
                prefer_genbank=prefer_genbank,
                version_fixed=version_fixed,
            ),
            conversion_status=row["conversion_status"],
        )
        for row in unique_rows
    )


def group_plans_by_download_request_accession(
    plans: tuple[AccessionPlan, ...],
) -> tuple[tuple[str, tuple[AccessionPlan, ...]], ...]:
    """Group accession plans by request accession in first-seen order."""

    grouped_plans: dict[str, list[AccessionPlan]] = {}
    for plan in plans:
        grouped_plans.setdefault(plan.download_request_accession, []).append(plan)
    return tuple(
        (download_request_accession, tuple(group))
        for download_request_accession, group in grouped_plans.items()
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


def build_resolved_payload_directory(
    candidate: Path,
) -> ResolvedPayloadDirectory | None:
    """Return one resolved payload directory when the path name is an accession."""

    if not candidate.is_dir():
        return None
    parsed_accession = parse_assembly_accession(candidate.name)
    if parsed_accession is None:
        return None
    return ResolvedPayloadDirectory(
        final_accession=parsed_accession.accession,
        directory=candidate,
    )


def collect_root_payload_directories(
    root: Path,
) -> tuple[ResolvedPayloadDirectory, ...]:
    """Collect accession-named directories directly under one root."""

    return tuple(
        resolved_payload
        for candidate in sorted(root.iterdir(), key=lambda path: path.name)
        if (resolved_payload := build_resolved_payload_directory(candidate)) is not None
    )


def has_accession_named_parent(candidate: Path, root: Path) -> bool:
    """Return whether a candidate is nested below another accession directory."""

    for parent in candidate.parents:
        if parent == root:
            return False
        if parse_assembly_accession(parent.name) is not None:
            return True
    return False


def collect_payload_directories(
    extraction_root: Path,
) -> tuple[ResolvedPayloadDirectory, ...]:
    """Collect realised payload directories from one extracted archive."""

    data_root = extraction_root / "ncbi_dataset" / "data"
    if data_root.is_dir():
        payload_directories = collect_root_payload_directories(data_root)
        if payload_directories:
            return payload_directories

    payload_directories = tuple(
        resolved_payload
        for candidate in sorted(
            extraction_root.rglob("*"),
            key=lambda path: str(path.relative_to(extraction_root)),
        )
        if (resolved_payload := build_resolved_payload_directory(candidate)) is not None
        and not has_accession_named_parent(candidate, extraction_root)
    )
    if payload_directories:
        return payload_directories
    raise LayoutError("Could not locate extracted payload directories")


def locate_accession_payload_directory(
    extraction_root: Path,
    requested_accession: str,
) -> ResolvedPayloadDirectory:
    """Locate the extracted payload directory for one requested accession."""

    payload_directories = locate_batch_payload_directories(
        extraction_root,
        (requested_accession,),
    )
    return payload_directories[requested_accession]


def locate_batch_payload_directories(
    extraction_root: Path,
    requested_accessions: tuple[str, ...],
) -> dict[str, ResolvedPayloadDirectory]:
    """Locate extracted payload directories for one request batch."""

    resolution = locate_partial_batch_payload_directories(
        extraction_root,
        requested_accessions,
    )
    if resolution.unresolved_messages:
        unresolved_text = "; ".join(
            resolution.unresolved_messages[requested_accession]
            for requested_accession in requested_accessions
            if requested_accession in resolution.unresolved_messages
        )
        raise LayoutError(unresolved_text)
    return resolution.resolved_payloads


def locate_partial_batch_payload_directories(
    extraction_root: Path,
    requested_accessions: tuple[str, ...],
) -> PartialBatchPayloadResolution:
    """Locate payloads for one request batch without failing atomically."""

    try:
        payload_records = collect_payload_directories(extraction_root)
    except LayoutError:
        payload_records = ()
    payloads_by_accession = {
        payload.final_accession: payload for payload in payload_records
    }
    payloads_by_stem: dict[str, list[ResolvedPayloadDirectory]] = defaultdict(list)
    for payload in payload_records:
        payloads_by_stem[get_assembly_accession_stem(payload.final_accession)].append(
            payload,
        )

    located_payloads: dict[str, ResolvedPayloadDirectory] = {}
    unresolved_messages: dict[str, str] = {}
    for requested_accession in requested_accessions:
        exact_match = payloads_by_accession.get(requested_accession)
        if exact_match is not None:
            located_payloads[requested_accession] = exact_match
            continue

        request_stem = parse_assembly_accession_stem(requested_accession)
        if request_stem is None:
            unresolved_messages[requested_accession] = (
                "Could not locate extracted payload directory for requested "
                f"accession {requested_accession}"
            )
            continue

        stem_matches = tuple(payloads_by_stem.get(request_stem.accession, ()))
        if len(stem_matches) == 1:
            located_payloads[requested_accession] = stem_matches[0]
            continue
        if len(stem_matches) > 1:
            unresolved_messages[requested_accession] = (
                "Resolved multiple extracted payload directories for requested "
                f"accession {requested_accession}: "
                f"{', '.join(payload.final_accession for payload in stem_matches)}"
            )
            continue
        unresolved_messages[requested_accession] = (
            "Could not locate extracted payload directory for requested "
            f"accession {requested_accession}"
        )
    return PartialBatchPayloadResolution(
        resolved_payloads=located_payloads,
        unresolved_messages=unresolved_messages,
    )


def build_layout_failure(
    error: Exception,
    final_status: str = "retry_exhausted",
) -> CommandFailureRecord:
    """Build a synthetic failure record for a local layout error."""

    return CommandFailureRecord(
        stage="layout",
        attempt_index=1,
        max_attempts=1,
        error_type=type(error).__name__,
        error_message=str(error),
        final_status=final_status,
    )


def build_direct_layout_failure(
    error_message: str,
    attempted_accession: str,
    attempt_index: int,
    max_attempts: int,
    final_status: str,
) -> CommandFailureRecord:
    """Build one direct-batch layout failure for a single accession token."""

    return CommandFailureRecord(
        stage="layout",
        attempt_index=attempt_index,
        max_attempts=max_attempts,
        error_type="LayoutError",
        error_message=error_message,
        final_status=final_status,
        attempted_accession=attempted_accession,
    )


def build_shared_failure_context(
    original_accessions: tuple[str, ...],
    failures: tuple[CommandFailureRecord, ...],
    attempted_accession: str,
) -> SharedFailureContext:
    """Scope shared failures to the affected original accessions."""

    return SharedFailureContext(
        affected_original_accessions=get_ordered_unique_accessions(
            original_accessions,
        ),
        failures=attach_attempted_accession(failures, attempted_accession),
    )


def extract_download_payload(
    requested_accession: str,
    archive_path: Path,
    run_directories: RunDirectories,
    *,
    extraction_key: str | None = None,
) -> tuple[ResolvedPayloadDirectory | None, tuple[CommandFailureRecord, ...]]:
    """Extract one downloaded archive and locate its payload directory."""

    extraction_root = run_directories.extracted_root / (
        requested_accession if extraction_key is None else extraction_key
    )
    try:
        extract_archive(archive_path, extraction_root)
    except LayoutError as error:
        return None, (build_layout_failure(error),)

    try:
        payload_directory = locate_accession_payload_directory(
            extraction_root,
            requested_accession,
        )
    except LayoutError as error:
        return None, (build_layout_failure(error),)
    return payload_directory, ()


def build_failed_execution(
    original_accession: str,
    failures: tuple[CommandFailureRecord, ...],
    download_batch: str,
) -> AccessionExecution:
    """Build a failed execution for one original accession."""

    return AccessionExecution(
        original_accession=original_accession,
        final_accession=None,
        conversion_status="failed_no_usable_accession",
        download_status="failed",
        download_batch=download_batch,
        payload_directory=None,
        failures=failures,
    )


def build_successful_execution(
    plan: AccessionPlan,
    final_accession: str,
    download_status: str,
    download_batch: str,
    payload_directory: Path,
    failures: tuple[CommandFailureRecord, ...],
) -> AccessionExecution:
    """Build a successful execution for one accession plan."""

    conversion_status = plan.conversion_status
    if (
        download_status == "downloaded_after_fallback"
        and plan.conversion_status == "paired_to_gca"
    ):
        conversion_status = "paired_to_gca_fallback_original_on_download_failure"
    return AccessionExecution(
        original_accession=plan.original_accession,
        final_accession=final_accession,
        conversion_status=conversion_status,
        download_status=download_status,
        download_batch=download_batch,
        payload_directory=payload_directory,
        failures=failures,
    )


def build_direct_batch_archive_path(
    run_directories: RunDirectories,
    batch_label: str,
) -> Path:
    """Return the archive path for one direct batch pass."""

    return run_directories.downloads_root / f"{batch_label}.zip"


def build_phase_failed_executions(
    plans: tuple[AccessionPlan, ...],
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
) -> dict[str, AccessionExecution]:
    """Build failed executions for one set of unresolved direct plans."""

    return {
        plan.original_accession: build_failed_execution(
            plan.original_accession,
            tuple(failure_history[plan.original_accession]),
            last_download_batches[plan.original_accession],
        )
        for plan in plans
    }


def execute_direct_batch_phase(
    plan_groups: tuple[tuple[str, tuple[AccessionPlan, ...]], ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    *,
    batch_stage: str,
    batch_prefix: str,
    success_status: str,
    failure_history: dict[str, list[CommandFailureRecord]],
    last_download_batches: dict[str, str],
) -> DirectBatchPhaseResult:
    """Execute one batch-based direct phase with shrinking retry inputs."""

    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    pending_groups = plan_groups
    executions: dict[str, AccessionExecution] = {}
    shared_failures: list[SharedFailureContext] = []

    for attempt_index in range(1, MAX_DIRECT_BATCH_PASSES + 1):
        if not pending_groups:
            break
        batch_label = f"{batch_prefix}_{attempt_index}"
        pending_request_accessions = tuple(
            request_accession for request_accession, _ in pending_groups
        )
        logger.info(
            "%s: starting %s for %d request accession(s)",
            batch_label,
            batch_stage,
            len(pending_request_accessions),
        )
        affected_original_accessions = tuple(
            plan.original_accession
            for _, grouped_plans in pending_groups
            for plan in grouped_plans
        )
        for original_accession in affected_original_accessions:
            last_download_batches[original_accession] = batch_label
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
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
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
        )
        if not batch_result.succeeded:
            logger.warning(
                "%s: %s failed before payload extraction",
                batch_label,
                batch_stage,
            )
            shared_failures.append(
                build_shared_failure_context(
                    affected_original_accessions,
                    batch_result.failures,
                    batch_attempted_accessions,
                ),
            )
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=pending_groups,
                shared_failures=tuple(shared_failures),
            )

        extraction_root = run_directories.extracted_root / batch_label
        try:
            extract_archive(archive_path, extraction_root)
        except LayoutError as error:
            logger.warning(
                "%s: extraction failed after %s",
                batch_label,
                batch_stage,
            )
            shared_failures.append(
                build_shared_failure_context(
                    affected_original_accessions,
                    (build_layout_failure(error),),
                    batch_attempted_accessions,
                ),
            )
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=pending_groups,
                shared_failures=tuple(shared_failures),
            )

        resolution = locate_partial_batch_payload_directories(
            extraction_root,
            pending_request_accessions,
        )
        made_progress = bool(resolution.resolved_payloads)
        # Keep retrying only while a pass resolves something; repeated
        # no-progress passes would only re-run the same failing batch.
        can_retry = attempt_index < MAX_DIRECT_BATCH_PASSES and made_progress
        unresolved_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []
        final_status = "retry_scheduled" if can_retry else "retry_exhausted"

        for request_accession, grouped_plans in pending_groups:
            payload = resolution.resolved_payloads.get(request_accession)
            if payload is not None:
                for plan in grouped_plans:
                    plan_failures = tuple(failure_history[plan.original_accession])
                    executions[plan.original_accession] = build_successful_execution(
                        plan,
                        payload.final_accession,
                        success_status,
                        batch_label,
                        payload.directory,
                        plan_failures,
                    )
                continue

            failure_record = build_direct_layout_failure(
                resolution.unresolved_messages[request_accession],
                request_accession,
                attempt_index,
                MAX_DIRECT_BATCH_PASSES,
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

        if not unresolved_groups:
            return DirectBatchPhaseResult(
                executions=executions,
                unresolved_groups=(),
                shared_failures=tuple(shared_failures),
            )
        if can_retry:
            pending_groups = tuple(unresolved_groups)
            continue
        return DirectBatchPhaseResult(
            executions=executions,
            unresolved_groups=tuple(unresolved_groups),
            shared_failures=tuple(shared_failures),
        )

    return DirectBatchPhaseResult(
        executions=executions,
        unresolved_groups=pending_groups,
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
    failure_history: dict[str, list[CommandFailureRecord]] = {
        plan.original_accession: [] for plan in plans
    }
    last_download_batches: dict[str, str] = {
        plan.original_accession: plan.original_accession for plan in plans
    }

    preferred_phase = execute_direct_batch_phase(
        plan_groups,
        args,
        run_directories,
        logger,
        batch_stage="preferred_download",
        batch_prefix="direct_batch",
        success_status="downloaded",
        failure_history=failure_history,
        last_download_batches=last_download_batches,
    )
    executions.update(preferred_phase.executions)
    shared_failures.extend(preferred_phase.shared_failures)

    preferred_unresolved_plans: list[AccessionPlan] = []
    fallback_groups: list[tuple[str, tuple[AccessionPlan, ...]]] = []
    # Only rows that switched to a preferred request accession can later retry
    # against their original accession in the fallback phase.
    for _, grouped_plans in preferred_phase.unresolved_groups:
        for plan in grouped_plans:
            preferred_unresolved_plans.append(plan)
            if plan.conversion_status == "paired_to_gca":
                fallback_groups.append((plan.original_accession, (plan,)))
    failed_after_preferred = tuple(
        plan
        for plan in preferred_unresolved_plans
        if plan.conversion_status != "paired_to_gca"
    )
    executions.update(
        build_phase_failed_executions(
            failed_after_preferred,
            failure_history,
            last_download_batches,
        ),
    )

    if fallback_groups:
        fallback_phase = execute_direct_batch_phase(
            tuple(fallback_groups),
            args,
            run_directories,
            logger,
            batch_stage="fallback_download",
            batch_prefix="direct_fallback_batch",
            success_status="downloaded_after_fallback",
            failure_history=failure_history,
            last_download_batches=last_download_batches,
        )
        executions.update(fallback_phase.executions)
        shared_failures.extend(fallback_phase.shared_failures)
        unresolved_fallback_plans = tuple(
            plan
            for _, grouped_plans in fallback_phase.unresolved_groups
            for plan in grouped_plans
        )
        executions.update(
            build_phase_failed_executions(
                unresolved_fallback_plans,
                failure_history,
                last_download_batches,
            ),
        )

    return DownloadExecutionResult(
        executions=executions,
        method_used="direct",
        download_concurrency_used=1,
        rehydrate_workers_used=0,
        shared_failures=tuple(shared_failures),
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
            plan.download_request_accession for plan in plans
        ),
    )
    logger.info(
        "dehydrated_batch: starting preferred_download for %d request accession(s)",
        len(plans),
    )
    affected_original_accessions = tuple(
        plan.original_accession for plan in plans
    )
    accession_file = write_accession_input_file(
        run_directories.working_root / "dehydrate_accessions.txt",
        (plan.download_request_accession for plan in plans),
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


def fallback_batch_to_direct(
    plans: tuple[AccessionPlan, ...],
    args: CliArgs,
    run_directories: RunDirectories,
    logger: logging.Logger,
    batch_failures: SharedFailureContext,
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
    shared_failures: tuple[SharedFailureContext, ...],
    secrets: tuple[str, ...],
    shared_context_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build attempt-centric `download_failures.tsv` rows."""

    context_rows = enriched_rows if shared_context_rows is None else shared_context_rows
    failure_rows: list[dict[str, Any]] = []
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


def get_staging_directory_root() -> Path | None:
    """Return the configured temporary root for workflow staging files."""

    temp_root = os.environ.get("TMPDIR")
    if not temp_root:
        return None
    path = Path(temp_root)
    if path.exists() and not path.is_dir():
        return None
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_staging_directory(prefix: str) -> TemporaryDirectory[str]:
    """Create one temporary workflow staging directory."""

    temp_root = get_staging_directory_root()
    if temp_root is None:
        return TemporaryDirectory(prefix=prefix)
    return TemporaryDirectory(prefix=prefix, dir=temp_root)


def resolve_supported_accession_preferences(
    supported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[pl.DataFrame, tuple[CommandFailureRecord, ...]]:
    """Resolve preferred accessions for supported selected rows."""

    summary_map: dict[str, set[str]] = {}
    metadata_failures: tuple[CommandFailureRecord, ...] = ()
    supported_accessions = get_ordered_unique_accessions(
        supported_selected_frame.get_column("ncbi_accession").to_list(),
    )
    if not supported_selected_frame.is_empty() and args.prefer_genbank:
        logger.info(
            "Running metadata lookup for %d supported accession(s)",
            len(supported_accessions),
        )
        with create_staging_directory("gtdb_genomes_metadata_") as metadata_directory:
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
                logger.info(
                    "Metadata lookup finished with %d preferred mapping(s)",
                    len(summary_map),
                )
            except MetadataLookupError as error:
                metadata_failures = error.failures
                logger.warning(
                    "Metadata lookup failed; falling back to original accessions: %s",
                    redact_text(str(error), secrets),
                )
                summary_map = {}
    return (
        apply_accession_preferences(
            supported_selected_frame,
            summary_map,
            prefer_genbank=args.prefer_genbank,
        ),
        metadata_failures,
    )


def plan_supported_downloads(
    supported_mapped_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[tuple[AccessionPlan, ...], str]:
    """Build supported-accession plans and resolve the effective method."""

    accession_plans = build_accession_plans(
        supported_mapped_frame,
        prefer_genbank=args.prefer_genbank,
        version_fixed=args.version_fixed,
    )
    if not accession_plans:
        return (), args.download_method

    preview_accessions = get_ordered_unique_accessions(
        plan.download_request_accession for plan in accession_plans
    )
    preview_text: str | None = None
    with create_staging_directory("gtdb_genomes_preview_") as preview_directory:
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
        preview_text = run_preview_command(
            preview_accession_file,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        )

    decision = select_download_method(
        args.download_method,
        len(preview_accessions),
        preview_text=preview_text,
    )
    return accession_plans, decision.method_used


def count_unique_accessions(frame: pl.DataFrame) -> int:
    """Return the number of unique accession values in one selection frame."""

    if frame.is_empty():
        return 0
    return len(
        get_ordered_unique_accessions(
            frame.get_column("ncbi_accession").to_list(),
        ),
    )


def log_run_start(
    logger: logging.Logger,
    args: CliArgs,
) -> None:
    """Log the user-facing start summary for one workflow run."""

    logger.info(
        "Starting run: release=%s taxa=%d outdir=%s dry_run=%s",
        args.gtdb_release,
        len(args.gtdb_taxa),
        args.outdir,
        str(args.dry_run).lower(),
    )


def log_run_finish(
    logger: logging.Logger,
    *,
    successful_count: int,
    failed_count: int,
    exit_code: int,
) -> None:
    """Log the final high-level outcome for one workflow run."""

    logger.info(
        "Run finished: successful_accessions=%d failed_accessions=%d exit_code=%d",
        successful_count,
        failed_count,
        exit_code,
    )


def prepare_selection_phase(
    args: CliArgs,
    logger: logging.Logger,
) -> WorkflowSelectionPhase:
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
    return WorkflowSelectionPhase(
        resolution=resolution,
        selected_frame=selected_frame,
        supported_selected_frame=supported_selected_frame,
        unsupported_selected_frame=unsupported_selected_frame,
    )


def run_early_dry_run_unzip_check(
    args: CliArgs,
    logger: logging.Logger,
) -> None:
    """Check `unzip` early so dry-runs surface the real-run requirement sooner."""

    if not args.dry_run:
        return
    logger.info("Checking unzip availability for dry-run")
    check_required_tools(("unzip",))


def handle_zero_match_exit(
    args: CliArgs,
    logger: logging.Logger,
    selection_phase: WorkflowSelectionPhase,
    started_at: str,
) -> int | None:
    """Handle the zero-match path and return its exit code when it applies."""

    if not selection_phase.selected_frame.is_empty():
        return None

    if args.dry_run:
        logger.warning("No genomes matched the requested taxa")
        log_run_finish(logger, successful_count=0, failed_count=0, exit_code=4)
        return 4

    run_directories = initialise_run_directories(args.outdir)
    logger = reconfigure_output_logger(args, logger, run_directories)
    logger.info("Writing output manifests to %s", run_directories.output_root)
    taxon_slug_map = build_taxon_slug_map(args.gtdb_taxa)
    exit_code = 4
    run_summary_rows = [
        build_run_summary_row(
            args,
            args.gtdb_release,
            selection_phase.resolution.resolved_release,
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
    log_run_finish(logger, successful_count=0, failed_count=0, exit_code=exit_code)
    close_logger(logger)
    if not args.keep_temp:
        cleanup_working_directories(run_directories)
    return exit_code


def reconfigure_output_logger(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
) -> logging.Logger:
    """Switch from console-only logging to the output-root logger when needed."""

    close_logger(logger)
    logger, _ = configure_logging(
        debug=args.debug,
        dry_run=False,
        output_root=run_directories.output_root,
    )
    return logger


def run_supported_preflight(
    args: CliArgs,
    selection_phase: WorkflowSelectionPhase,
) -> None:
    """Check tools that are required for supported accession planning or runs."""

    if selection_phase.supported_selected_frame.is_empty():
        return
    required_tools = get_required_tools(
        dry_run=args.dry_run,
    )
    if required_tools:
        check_required_tools(required_tools)


def prepare_planning_phase(
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
    selection_phase: WorkflowSelectionPhase,
) -> WorkflowPlanningPhase:
    """Resolve accession preferences and plan the supported download strategy."""

    supported_mapped_frame, metadata_failures = resolve_supported_accession_preferences(
        selection_phase.supported_selected_frame,
        args,
        logger,
        secrets,
    )
    unsupported_mapped_frame = build_unsupported_accession_frame(
        selection_phase.unsupported_selected_frame,
    )
    mapped_frame = pl.concat(
        [
            frame
            for frame in (supported_mapped_frame, unsupported_mapped_frame)
            if not frame.is_empty()
        ],
        how="vertical",
    )
    accession_plans, decision_method = plan_supported_downloads(
        supported_mapped_frame,
        args,
        logger,
        secrets,
    )
    logger.info(
        "Automatic planning selected %s for %d supported accession(s)",
        decision_method,
        len(accession_plans),
    )
    return WorkflowPlanningPhase(
        mapped_frame=mapped_frame,
        metadata_failures=metadata_failures,
        accession_plans=accession_plans,
        decision_method=decision_method,
    )


def build_execution_result(
    args: CliArgs,
    logger: logging.Logger,
    run_directories: RunDirectories,
    secrets: tuple[str, ...],
    planning_phase: WorkflowPlanningPhase,
) -> DownloadExecutionResult:
    """Execute the planned supported downloads for one real run."""

    if not planning_phase.accession_plans:
        return DownloadExecutionResult(
            executions={},
            method_used=args.download_method,
            download_concurrency_used=0,
            rehydrate_workers_used=0,
            shared_failures=(),
        )
    return execute_accession_plans(
        planning_phase.accession_plans,
        args,
        planning_phase.decision_method,
        run_directories,
        logger,
        secrets,
    )


def build_enriched_output_rows(
    selection_phase: WorkflowSelectionPhase,
    planning_phase: WorkflowPlanningPhase,
    execution_result: DownloadExecutionResult,
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
        **build_unsupported_executions(selection_phase.unsupported_selected_frame),
    }
    enriched_rows: list[dict[str, Any]] = []
    supported_enriched_rows: list[dict[str, Any]] = []
    for row in planning_phase.mapped_frame.rows(named=True):
        execution = executions[row["ncbi_accession"]]
        final_accession = execution.final_accession or ""
        unsupported_accession = is_unsupported_uba_accession(row["ncbi_accession"])
        enriched_rows.append(
            {
                "requested_taxon": row["requested_taxon"],
                "taxon_slug": row["taxon_slug"],
                "resolved_release": selection_phase.resolution.resolved_release,
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
    selection_phase: WorkflowSelectionPhase,
    planning_phase: WorkflowPlanningPhase,
    execution_result: DownloadExecutionResult,
    secrets: tuple[str, ...],
) -> int:
    """Copy payloads, write manifests, and return the final exit code."""

    logger.info("Writing output manifests to %s", run_directories.output_root)
    enriched_rows, supported_enriched_rows, per_taxon_rows, duplicate_counts = (
        build_enriched_output_rows(
            selection_phase,
            planning_phase,
            execution_result,
            run_directories,
            logger,
        )
    )
    failure_rows = build_failure_rows(
        enriched_rows,
        {
            **execution_result.executions,
            **build_unsupported_executions(selection_phase.unsupported_selected_frame),
        },
        planning_phase.metadata_failures,
        execution_result.shared_failures,
        secrets,
        shared_context_rows=supported_enriched_rows,
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
            selection_phase.resolution.resolved_release,
            execution_result.method_used,
            execution_result.download_concurrency_used,
            execution_result.rehydrate_workers_used,
            planning_phase.mapped_frame.height,
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
    log_run_finish(
        logger,
        successful_count=successful_count,
        failed_count=failed_count,
        exit_code=exit_code,
    )
    return exit_code


def run_workflow(args: CliArgs) -> int:
    """Run the documented workflow and return the process exit code."""

    logger, _ = configure_logging(debug=args.debug, dry_run=args.dry_run)
    secrets = tuple(secret for secret in (args.ncbi_api_key,) if secret)
    started_at = datetime.now(UTC).isoformat()
    log_run_start(logger, args)

    try:
        # Phase 1: bundled release resolution and GTDB taxon selection.
        selection_phase = prepare_selection_phase(args, logger)
        run_early_dry_run_unzip_check(args, logger)
        zero_match_exit = handle_zero_match_exit(
            args,
            logger,
            selection_phase,
            started_at,
        )
        if zero_match_exit is not None:
            close_logger(logger)
            return zero_match_exit

        if not selection_phase.unsupported_selected_frame.is_empty():
            logger.warning(
                build_unsupported_uba_warning(
                    selection_phase.unsupported_selected_frame,
                ),
            )

        # Phase 2: supported-accession preflight and automatic planning.
        run_supported_preflight(args, selection_phase)
        planning_phase = prepare_planning_phase(
            args,
            logger,
            secrets,
            selection_phase,
        )
    except BundledDataError as error:
        logger.error("%s", error)
        close_logger(logger)
        return 3
    except PreviewError as error:
        logger.error("%s", redact_text(str(error), secrets))
        close_logger(logger)
        return 5

    # Phase 3: dry-runs stop after planning and report the planned workload.
    if args.dry_run:
        logger.info(
            "Dry-run finished: planned_supported_accessions=%d unsupported_legacy_accessions=%d",
            len(planning_phase.accession_plans),
            count_unique_accessions(selection_phase.unsupported_selected_frame),
        )
        close_logger(logger)
        return 0

    # Phase 4: real runs materialise payloads and write the output manifests.
    run_directories = initialise_run_directories(args.outdir)
    logger = reconfigure_output_logger(args, logger, run_directories)
    execution_result = build_execution_result(
        args,
        logger,
        run_directories,
        secrets,
        planning_phase,
    )
    exit_code = materialise_real_run_outputs(
        args,
        logger,
        run_directories,
        started_at,
        selection_phase,
        planning_phase,
        execution_result,
        secrets,
    )
    close_logger(logger)
    if not args.keep_temp:
        cleanup_working_directories(run_directories)
    return exit_code

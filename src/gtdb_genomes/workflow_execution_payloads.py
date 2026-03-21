"""Payload discovery and execution helper functions for workflow execution."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from gtdb_genomes.download import CommandFailureRecord, get_ordered_unique_accessions
from gtdb_genomes.layout import LayoutError, RunDirectories, extract_archive
from gtdb_genomes.metadata import (
    get_assembly_accession_stem,
    parse_assembly_accession,
    parse_assembly_accession_stem,
)

from gtdb_genomes.workflow_execution_models import (
    AccessionExecution,
    AccessionPlan,
    PartialBatchPayloadResolution,
    ResolvedPayloadDirectory,
    SharedFailureContext,
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


def build_batch_layout_failures(
    failures: tuple[CommandFailureRecord, ...],
    error: Exception,
) -> tuple[CommandFailureRecord, ...]:
    """Append one synthetic local layout failure to a batch failure list."""

    return failures + (build_layout_failure(error),)


def build_batch_archive_path(run_directories: RunDirectories) -> Path:
    """Return the shared archive path for a dehydrated batch download."""

    return run_directories.downloads_root / "dehydrated_batch.zip"

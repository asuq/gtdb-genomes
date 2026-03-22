"""Planning helpers for the GTDB workflow."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from gtdb_genomes.download import (
    DEFAULT_REQUESTED_DOWNLOAD_METHOD,
    PreviewCommandResult,
    PreviewError,
    build_preview_command,
    get_ordered_unique_accessions,
    run_preview_command,
    select_download_method,
    write_accession_input_file,
)
from gtdb_genomes.logging_utils import redact_command, redact_text
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    find_incomplete_genbank_metadata_accessions,
    MetadataLookupError,
    SUPPRESSED_ASSEMBLY_NOTE,
    apply_accession_preferences,
    build_download_request_accession,
    is_suppressed_status,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.workflow_execution import (
    AccessionPlan,
    SharedFailureContext,
    build_shared_failure_context,
)
from gtdb_genomes.workflow_selection import build_unsupported_accession_frame

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


# Temporary planning workspace helpers.


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


# Metadata preference resolution.


@dataclass(frozen=True, slots=True)
class SuppressedAccessionNote:
    """One metadata-confirmed suppressed download target."""

    original_accession: str
    selected_accession: str
    suppression_reason: str | None


def build_accession_plans(
    mapped_frame: pl.DataFrame,
    *,
    prefer_genbank: bool,
    version_latest: bool,
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
            download_request_accession=build_download_request_accession(
                row["final_accession"],
                prefer_genbank=prefer_genbank,
                version_latest=version_latest,
            ),
            conversion_status=row["conversion_status"],
        )
        for row in unique_rows
    )


def build_original_accession_scope(
    accessions: tuple[str, ...],
) -> tuple[str, ...]:
    """Return a deterministic original-accession scope for shared failures."""

    return get_ordered_unique_accessions(accessions)


def build_candidate_accession_scope(
    summary_map: dict[str, set[str]],
    candidate_accessions: tuple[str, ...],
) -> tuple[str, ...]:
    """Return originals affected by the paired-GenBank candidate lookup."""

    candidate_accession_set = set(candidate_accessions)
    return get_ordered_unique_accessions(
        original_accession
        for original_accession, discovered_accessions in summary_map.items()
        if any(
            candidate_accession in candidate_accession_set
            for candidate_accession in discovered_accessions
        )
    )


def build_suppressed_accession_notes(
    mapped_frame: pl.DataFrame,
    status_map: dict[str, AssemblyStatusInfo],
) -> dict[str, SuppressedAccessionNote]:
    """Return suppression notes for the accessions the workflow will download."""

    suppressed_notes: dict[str, SuppressedAccessionNote] = {}
    if mapped_frame.is_empty():
        return suppressed_notes

    unique_rows = mapped_frame.unique(
        subset=["ncbi_accession"],
        keep="first",
        maintain_order=True,
    ).rows(named=True)
    for row in unique_rows:
        original_accession = row["ncbi_accession"]
        selected_accession = row["final_accession"]
        selected_status_info = status_map.get(selected_accession)
        if (
            selected_status_info is not None
            and is_suppressed_status(selected_status_info.assembly_status)
        ):
            suppressed_notes[original_accession] = SuppressedAccessionNote(
                original_accession=original_accession,
                selected_accession=selected_accession,
                suppression_reason=selected_status_info.suppression_reason,
            )
            continue

        status_info = status_map.get(original_accession)
        if status_info is None:
            continue

        if selected_accession == original_accession:
            if not is_suppressed_status(status_info.assembly_status):
                continue
            suppressed_notes[original_accession] = SuppressedAccessionNote(
                original_accession=original_accession,
                selected_accession=selected_accession,
                suppression_reason=status_info.suppression_reason,
            )
            continue

        if (
            row["conversion_status"] == "paired_to_gca"
            and selected_accession == status_info.paired_accession
            and is_suppressed_status(status_info.paired_assembly_status)
        ):
            suppressed_notes[original_accession] = SuppressedAccessionNote(
                original_accession=original_accession,
                selected_accession=selected_accession,
                suppression_reason=None,
            )
    return suppressed_notes


def format_suppressed_accession_examples(
    suppressed_notes: dict[str, SuppressedAccessionNote],
) -> str:
    """Format deterministic accession examples for warning messages."""

    examples: list[str] = []
    for note in suppressed_notes.values():
        accession_text = note.selected_accession
        if note.selected_accession != note.original_accession:
            accession_text = (
                f"{note.original_accession} -> {note.selected_accession}"
            )
        if note.suppression_reason:
            accession_text = (
                f"{accession_text} (reason: {note.suppression_reason})"
            )
        examples.append(accession_text)
    return ", ".join(examples)


def build_planning_suppressed_warning(
    suppressed_notes: dict[str, SuppressedAccessionNote],
) -> str | None:
    """Build the planning-time warning for suppressed download targets."""

    if not suppressed_notes:
        return None
    count = len(suppressed_notes)
    noun = "assembly" if count == 1 else "assemblies"
    return (
        f"NCBI marks {count} planned {noun} as suppressed; "
        f"{SUPPRESSED_ASSEMBLY_NOTE} "
        f"Affected accessions: {format_suppressed_accession_examples(suppressed_notes)}"
    )


def build_failed_suppressed_warning(
    suppressed_notes: dict[str, SuppressedAccessionNote],
    failed_original_accessions: tuple[str, ...],
) -> str | None:
    """Build the final warning for failed suppressed download targets."""

    failed_notes = {
        original_accession: suppressed_notes[original_accession]
        for original_accession in failed_original_accessions
        if original_accession in suppressed_notes
    }
    if not failed_notes:
        return None
    count = len(failed_notes)
    noun = "assembly" if count == 1 else "assemblies"
    verb = "was" if count == 1 else "were"
    return (
        f"{count} failed {noun} {verb} marked suppressed by NCBI; "
        f"{SUPPRESSED_ASSEMBLY_NOTE} "
        f"Affected accessions: {format_suppressed_accession_examples(failed_notes)}"
    )


def resolve_supported_accession_preferences(
    supported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[
    pl.DataFrame,
    tuple[SharedFailureContext, ...],
    dict[str, SuppressedAccessionNote],
]:
    """Resolve preferred accessions for supported selected rows."""

    if supported_selected_frame.is_empty():
        return (
            apply_accession_preferences(
                supported_selected_frame,
                {},
                status_map={},
                prefer_genbank=args.prefer_genbank,
            ),
            (),
            {},
        )
    if not args.prefer_genbank:
        logger.info("Skipping metadata lookup because --prefer-genbank is disabled")
        return (
            apply_accession_preferences(
                supported_selected_frame,
                {},
                status_map={},
                prefer_genbank=False,
            ),
            (),
            {},
        )

    summary_map: dict[str, set[str]] = {}
    status_map: dict[str, AssemblyStatusInfo] = {}
    incomplete_genbank_accessions: set[str] = set()
    metadata_shared_failures: list[SharedFailureContext] = []
    supported_accessions = get_ordered_unique_accessions(
        supported_selected_frame.get_column("ncbi_accession").to_list(),
    )
    logger.info(
        "Running metadata lookup for %d supported accession(s)",
        len(supported_accessions),
    )
    with create_staging_directory("gtdb_genomes_metadata_") as metadata_directory:
        metadata_accession_file = write_accession_input_file(
            Path(metadata_directory) / "accessions.txt",
            supported_accessions,
        )
        summary_lookup = run_summary_lookup_with_retries(
            supported_accessions,
            metadata_accession_file,
            ncbi_api_key=args.ncbi_api_key,
        )
        summary_map = summary_lookup.summary_map
        status_map = summary_lookup.status_map
        if summary_lookup.failures:
            metadata_shared_failures.append(
                build_shared_failure_context(
                    build_original_accession_scope(supported_accessions),
                    summary_lookup.failures,
                    ";".join(supported_accessions),
                ),
            )
        incomplete_genbank_accessions.update(
            accession
            for accession in summary_lookup.incomplete_accessions
            if accession.startswith("GCF_")
        )
        logger.info(
            "Metadata lookup finished with %d preferred mapping(s)",
            len(summary_map),
        )
        candidate_accessions = get_ordered_unique_accessions(
            accession
            for discovered_accessions in summary_map.values()
            for accession in discovered_accessions
            if accession.startswith("GCA_") and accession not in status_map
        )
        if candidate_accessions:
            candidate_original_accessions = build_candidate_accession_scope(
                summary_map,
                candidate_accessions,
            )
            logger.info(
                "Running candidate metadata lookup for %d paired GenBank "
                "accession(s)",
                len(candidate_accessions),
            )
            candidate_accession_file = write_accession_input_file(
                Path(metadata_directory) / "paired-gca-accessions.txt",
                candidate_accessions,
            )
            try:
                candidate_lookup = run_summary_lookup_with_retries(
                    candidate_accessions,
                    candidate_accession_file,
                    ncbi_api_key=args.ncbi_api_key,
                )
            except MetadataLookupError as error:
                logger.warning(
                    "Candidate metadata lookup failed for %d paired GenBank "
                    "accession(s); falling back to original accessions",
                    len(candidate_accessions),
                )
                metadata_shared_failures.append(
                    build_shared_failure_context(
                        candidate_original_accessions,
                        error.failures,
                        ";".join(candidate_accessions),
                    ),
                )
            else:
                if candidate_lookup.failures:
                    metadata_shared_failures.append(
                        build_shared_failure_context(
                            candidate_original_accessions,
                            candidate_lookup.failures,
                            ";".join(candidate_accessions),
                        ),
                    )
                status_map = {
                    **status_map,
                    **candidate_lookup.status_map,
                }
            incomplete_genbank_accessions.update(
                find_incomplete_genbank_metadata_accessions(
                    summary_map,
                    status_map,
                )
            )
    mapped_frame = apply_accession_preferences(
        supported_selected_frame,
        summary_map,
        status_map=status_map,
        incomplete_genbank_accessions=incomplete_genbank_accessions,
        prefer_genbank=args.prefer_genbank,
    )
    return (
        mapped_frame,
        tuple(metadata_shared_failures),
        build_suppressed_accession_notes(mapped_frame, status_map),
    )


# Automatic method planning.


def normalise_preview_command_result(
    preview_result: PreviewCommandResult | str,
) -> PreviewCommandResult:
    """Return a structured preview result for real runs and test doubles."""

    if isinstance(preview_result, PreviewCommandResult):
        return preview_result
    return PreviewCommandResult(
        preview_text=preview_result,
        failures=(),
    )


def plan_supported_downloads(
    supported_mapped_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[tuple[AccessionPlan, ...], str, tuple[SharedFailureContext, ...]]:
    """Build supported-accession plans and resolve the effective method."""

    accession_plans = build_accession_plans(
        supported_mapped_frame,
        prefer_genbank=args.prefer_genbank,
        version_latest=args.version_latest,
    )
    if not accession_plans:
        return (), DEFAULT_REQUESTED_DOWNLOAD_METHOD, ()

    preview_accessions = get_ordered_unique_accessions(
        plan.download_request_accession for plan in accession_plans
    )
    preview_original_accessions = build_original_accession_scope(
        tuple(plan.original_accession for plan in accession_plans),
    )
    with create_staging_directory("gtdb_genomes_preview_") as preview_directory:
        preview_accession_file = write_accession_input_file(
            Path(preview_directory) / "accessions.txt",
            preview_accessions,
        )
        decision = select_download_method(
            DEFAULT_REQUESTED_DOWNLOAD_METHOD,
            len(preview_accessions),
        )
        preview_command = build_preview_command(
            preview_accession_file,
            args.include,
            ncbi_api_key=args.ncbi_api_key,
            debug=args.debug,
        )
        logger.debug("Running %s", redact_command(preview_command, secrets))
        preview_shared_failures: tuple[SharedFailureContext, ...] = ()
        try:
            preview_result = normalise_preview_command_result(
                run_preview_command(
                    preview_accession_file,
                    args.include,
                    ncbi_api_key=args.ncbi_api_key,
                    debug=args.debug,
                ),
            )
        except PreviewError as error:
            if error.failures:
                preview_shared_failures = (
                    build_shared_failure_context(
                        preview_original_accessions,
                        error.failures,
                        ";".join(preview_accessions),
                    ),
                )
            logger.warning(
                "datasets preview failed during automatic planning; continuing "
                "with %s because auto mode now uses only the request-token count: %s",
                decision.method_used,
                redact_text(str(error), secrets),
            )
        else:
            if preview_result.failures:
                preview_shared_failures = (
                    build_shared_failure_context(
                        preview_original_accessions,
                        preview_result.failures,
                        ";".join(preview_accessions),
                    ),
                )
    return accession_plans, decision.method_used, preview_shared_failures


def prepare_planning_inputs(
    supported_selected_frame: pl.DataFrame,
    unsupported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
    secrets: tuple[str, ...],
) -> tuple[
    pl.DataFrame,
    tuple[SharedFailureContext, ...],
    dict[str, SuppressedAccessionNote],
    tuple[AccessionPlan, ...],
    str,
]:
    """Resolve accession preferences and plan the supported download strategy."""

    (
        supported_mapped_frame,
        metadata_shared_failures,
        suppressed_notes,
    ) = resolve_supported_accession_preferences(
        supported_selected_frame,
        args,
        logger,
        secrets,
    )
    unsupported_mapped_frame = build_unsupported_accession_frame(
        unsupported_selected_frame,
    )
    mapped_frame = pl.concat(
        [
            frame
            for frame in (supported_mapped_frame, unsupported_mapped_frame)
            if not frame.is_empty()
        ],
        how="vertical",
    )
    accession_plans, decision_method, preview_shared_failures = plan_supported_downloads(
        supported_mapped_frame,
        args,
        logger,
        secrets,
    )
    planning_shared_failures = (
        metadata_shared_failures + preview_shared_failures
    )
    logger.info(
        "Automatic planning selected %s for %d supported accession(s)",
        decision_method,
        len(accession_plans),
    )
    return (
        mapped_frame,
        planning_shared_failures,
        suppressed_notes,
        accession_plans,
        decision_method,
    )

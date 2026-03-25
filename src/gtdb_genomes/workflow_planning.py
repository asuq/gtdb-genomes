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
    get_ordered_unique_accessions,
    select_download_method,
    write_accession_input_file,
)
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    build_augmented_discovered_accessions,
    find_matching_genbank_accessions,
    get_explicit_paired_genbank_candidate,
    MetadataLookupError,
    apply_accession_preferences,
    build_download_request_accession,
    is_suppressed_status,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.workflow_execution import AccessionPlan
from gtdb_genomes.workflow_selection import build_unsupported_accession_frame

if TYPE_CHECKING:
    from gtdb_genomes.cli import CliArgs


# Temporary planning workspace helpers.
SUPPRESSED_WARNING_EXAMPLES = 5
SUPPRESSED_WARNING_SUMMARY = "payloads may no longer be downloadable."


def get_staging_directory_root() -> Path | None:
    """Return the configured temporary root for workflow staging files."""

    for environment_variable in ("TMPDIR", "TMP", "TEMP"):
        temp_root = os.environ.get(environment_variable)
        if not temp_root:
            continue
        path = Path(temp_root)
        if path.exists() and not path.is_dir():
            continue
        path.mkdir(parents=True, exist_ok=True)
        return path
    return None


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
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> tuple[AccessionPlan, ...]:
    """Build one unique download plan per original NCBI accession."""

    if mapped_frame.is_empty():
        return ()
    suppressed_accessions = (
        frozenset()
        if suppressed_notes is None
        else frozenset(suppressed_notes)
    )
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
            is_suppressed=row["ncbi_accession"] in suppressed_accessions,
        )
        for row in unique_rows
    )


def build_candidate_accession_scope(
    summary_map: dict[str, set[str]],
    status_map: dict[str, AssemblyStatusInfo],
    candidate_accessions: tuple[str, ...],
    *,
    version_latest: bool,
) -> tuple[str, ...]:
    """Return originals affected by the paired-GenBank candidate lookup."""

    candidate_accession_set = set(candidate_accessions)
    return get_ordered_unique_accessions(
        original_accession
        for original_accession, discovered_accessions in summary_map.items()
        if any(
            candidate_accession in candidate_accession_set
            for candidate_accession in build_augmented_discovered_accessions(
                discovered_accessions,
                get_explicit_paired_genbank_candidate(
                    original_accession,
                    status_map,
                    version_latest=version_latest,
                ),
            )
        )
    )


def build_candidate_metadata_accessions(
    summary_map: dict[str, set[str]],
    status_map: dict[str, AssemblyStatusInfo],
    *,
    version_latest: bool,
) -> tuple[str, ...]:
    """Return the GenBank candidates that still need metadata lookup."""

    return get_ordered_unique_accessions(
        accession
        for requested_accession, discovered_accessions in summary_map.items()
        for accession in find_matching_genbank_accessions(
            requested_accession,
            build_augmented_discovered_accessions(
                discovered_accessions,
                get_explicit_paired_genbank_candidate(
                    requested_accession,
                    status_map,
                    version_latest=version_latest,
                ),
            ),
            version_latest=version_latest,
        )
        if accession not in status_map
    )


def build_explicit_pairing_conflict_warning(
    mapped_frame: pl.DataFrame,
) -> str | None:
    """Build the run-level warning for conflicting explicit paired metadata."""

    if mapped_frame.is_empty():
        return None
    conflicting_accessions = get_ordered_unique_accessions(
        row["ncbi_accession"]
        for row in mapped_frame.unique(
            subset=["ncbi_accession"],
            keep="first",
            maintain_order=True,
        ).rows(named=True)
        if row["conversion_status"] == "paired_gca_conflict_fallback_original"
    )
    if not conflicting_accessions:
        return None
    count = len(conflicting_accessions)
    noun = "accession" if count == 1 else "accessions"
    verb = "was" if count == 1 else "were"
    examples = ", ".join(conflicting_accessions)
    return (
        f"{count} requested {noun} {verb} kept on the original RefSeq target "
        "because explicit paired GenBank metadata conflicted with the expected "
        f"assembly family. Affected accessions: {examples}"
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
    if len(examples) <= SUPPRESSED_WARNING_EXAMPLES:
        return ", ".join(examples)
    remaining = len(examples) - SUPPRESSED_WARNING_EXAMPLES
    visible_examples = examples[:SUPPRESSED_WARNING_EXAMPLES]
    return f"{', '.join(visible_examples)}, and {remaining} more"


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
        f"{SUPPRESSED_WARNING_SUMMARY}"
    )


def build_planning_suppressed_debug_detail(
    suppressed_notes: dict[str, SuppressedAccessionNote],
) -> str | None:
    """Build the planning-time debug detail for suppressed download targets."""

    if not suppressed_notes:
        return None
    return (
        "Suppressed planned accessions: "
        f"{format_suppressed_accession_examples(suppressed_notes)}"
    )


def select_failed_suppressed_notes(
    suppressed_notes: dict[str, SuppressedAccessionNote],
    failed_original_accessions: tuple[str, ...],
) -> dict[str, SuppressedAccessionNote]:
    """Return suppressed-note rows scoped to failed original accessions."""

    return {
        original_accession: suppressed_notes[original_accession]
        for original_accession in failed_original_accessions
        if original_accession in suppressed_notes
    }


def build_failed_suppressed_warning(
    suppressed_notes: dict[str, SuppressedAccessionNote],
    failed_original_accessions: tuple[str, ...],
) -> str | None:
    """Build the final warning for failed suppressed download targets."""

    failed_notes = select_failed_suppressed_notes(
        suppressed_notes,
        failed_original_accessions,
    )
    if not failed_notes:
        return None
    count = len(failed_notes)
    noun = "assembly" if count == 1 else "assemblies"
    verb = "was" if count == 1 else "were"
    return (
        f"{count} failed {noun} {verb} marked suppressed by NCBI; "
        f"{SUPPRESSED_WARNING_SUMMARY}"
    )


def build_failed_suppressed_debug_detail(
    suppressed_notes: dict[str, SuppressedAccessionNote],
    failed_original_accessions: tuple[str, ...],
) -> str | None:
    """Build the final debug detail for failed suppressed download targets."""

    failed_notes = select_failed_suppressed_notes(
        suppressed_notes,
        failed_original_accessions,
    )
    if not failed_notes:
        return None
    return (
        "Suppressed failed accessions: "
        f"{format_suppressed_accession_examples(failed_notes)}"
    )


def resolve_supported_accession_preferences(
    supported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
) -> tuple[pl.DataFrame, dict[str, SuppressedAccessionNote]]:
    """Resolve preferred accessions for supported selected rows."""

    if supported_selected_frame.is_empty():
        return (
            apply_accession_preferences(
                supported_selected_frame,
                {},
                status_map={},
                prefer_genbank=args.prefer_genbank,
                version_latest=args.version_latest,
            ),
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
                version_latest=args.version_latest,
            ),
            {},
        )

    summary_map: dict[str, set[str]] = {}
    status_map: dict[str, AssemblyStatusInfo] = {}
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
        try:
            summary_lookup = run_summary_lookup_with_retries(
                supported_accessions,
                metadata_accession_file,
                ncbi_api_key=args.ncbi_api_key,
            )
        except MetadataLookupError as error:
            logger.warning(
                "Primary metadata lookup failed for %d supported accession(s); "
                "keeping original accessions",
                len(supported_accessions),
            )
        else:
            summary_map = summary_lookup.summary_map
            status_map = summary_lookup.status_map
            logger.info(
                "Metadata lookup finished with %d preferred mapping(s)",
                len(summary_map),
            )
            candidate_accessions = build_candidate_metadata_accessions(
                summary_map,
                status_map,
                version_latest=args.version_latest,
            )
            if candidate_accessions:
                candidate_original_accessions = build_candidate_accession_scope(
                    summary_map,
                    status_map,
                    candidate_accessions,
                    version_latest=args.version_latest,
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
                else:
                    status_map = {
                        **status_map,
                        **candidate_lookup.status_map,
                    }
    mapped_frame = apply_accession_preferences(
        supported_selected_frame,
        summary_map,
        status_map=status_map,
        prefer_genbank=args.prefer_genbank,
        version_latest=args.version_latest,
    )
    return mapped_frame, build_suppressed_accession_notes(mapped_frame, status_map)

# Automatic method planning.


def plan_supported_downloads(
    supported_mapped_frame: pl.DataFrame,
    args: CliArgs,
    suppressed_notes: dict[str, SuppressedAccessionNote] | None = None,
) -> tuple[tuple[AccessionPlan, ...], str]:
    """Build supported-accession plans and resolve the effective method."""

    accession_plans = build_accession_plans(
        supported_mapped_frame,
        prefer_genbank=args.prefer_genbank,
        version_latest=args.version_latest,
        suppressed_notes=suppressed_notes,
    )
    if not accession_plans:
        return (), DEFAULT_REQUESTED_DOWNLOAD_METHOD

    decision = select_download_method(
        len(
            get_ordered_unique_accessions(
                plan.download_request_accession for plan in accession_plans
            ),
        ),
    )
    return accession_plans, decision.method_used


def prepare_planning_inputs(
    supported_selected_frame: pl.DataFrame,
    unsupported_selected_frame: pl.DataFrame,
    args: CliArgs,
    logger: logging.Logger,
) -> tuple[
    pl.DataFrame,
    dict[str, SuppressedAccessionNote],
    tuple[AccessionPlan, ...],
    str,
]:
    """Resolve accession preferences and plan the supported download strategy."""

    supported_mapped_frame, suppressed_notes = resolve_supported_accession_preferences(
        supported_selected_frame,
        args,
        logger,
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
    accession_plans, decision_method = plan_supported_downloads(
        supported_mapped_frame,
        args,
        suppressed_notes,
    )
    logger.info(
        "Automatic planning selected %s for %d supported accession(s)",
        decision_method,
        len(accession_plans),
    )
    return (
        mapped_frame,
        suppressed_notes,
        accession_plans,
        decision_method,
    )

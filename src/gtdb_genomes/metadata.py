"""NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from gtdb_genomes.assembly_accessions import (
    AssemblyAccession,
    AssemblyAccessionStem,
    get_assembly_accession_stem,
    parse_assembly_accession,
    parse_assembly_accession_stem,
    select_matching_genbank_candidates,
)
from gtdb_genomes.download import (
    CommandFailureRecord,
    RETRY_DELAYS_SECONDS,
)
from gtdb_genomes.metadata_summary_parsing import (
    AssemblyStatusInfo,
    MetadataLookupError,
    ParsedSummaryOutput,
    SummaryLookupResult,
    parse_summary_json_lines,
    parse_summary_output,
    parse_summary_status_map,
)
from gtdb_genomes.subprocess_utils import (
    DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    build_datasets_subprocess_environment,
    build_spawn_error_message,
    build_subprocess_error_message,
    build_timeout_error_message,
)


@dataclass(frozen=True, slots=True)
class ExplicitPairedGenbankCandidate:
    """Structured explicit paired-GenBank metadata for one RefSeq request."""

    accession: str
    assembly_status: str


SUPPRESSED_ASSEMBLY_NOTE = (
    "NCBI metadata marked this assembly as suppressed; "
    "the genome payload may no longer be downloadable."
)
UNKNOWN_ASSEMBLY_STATUS_INFO = AssemblyStatusInfo(
    assembly_status=None,
    suppression_reason=None,
    paired_accession=None,
    paired_assembly_status=None,
)


def build_summary_command(
    accession_file: Path,
    datasets_bin: str = "datasets",
) -> list[str]:
    """Build the datasets summary command for assembly accessions."""

    return [
        datasets_bin,
        "summary",
        "genome",
        "accession",
        "--inputfile",
        str(accession_file),
        "--as-json-lines",
    ]


def build_download_request_accession(
    selected_accession: str,
    *,
    prefer_genbank: bool,
    version_latest: bool,
) -> str:
    """Return the accession token that should be passed to `datasets`."""

    if not prefer_genbank or not version_latest:
        return selected_accession
    return get_assembly_accession_stem(selected_accession)


def run_summary_lookup_with_retries(
    accessions: Iterable[str],
    accession_file: Path,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    sleep_func: Callable[[float], None] = time.sleep,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> SummaryLookupResult:
    """Look up accession metadata with the fixed retry budget."""

    ordered_accessions = tuple(dict.fromkeys(accessions))
    if not ordered_accessions:
        return SummaryLookupResult(summary_map={}, failures=())
    attempted_accessions = ";".join(ordered_accessions)
    command_runner = subprocess.run if runner is None else runner
    command = build_summary_command(
        accession_file,
        datasets_bin=datasets_bin,
    )
    environment = build_datasets_subprocess_environment(ncbi_api_key)
    max_attempts = len(RETRY_DELAYS_SECONDS) + 1
    failures: list[CommandFailureRecord] = []
    for attempt_index in range(1, max_attempts + 1):
        retry_allowed = attempt_index < max_attempts
        try:
            result = command_runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                env=environment,
                timeout=DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            error_type = "metadata_lookup_timeout"
            error_message = build_timeout_error_message(
                "metadata_lookup",
                DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
                timeout_error=error,
            )
        except OSError as error:
            error_type = "metadata_lookup_spawn_error"
            error_message = build_spawn_error_message("metadata_lookup", error)
            retry_allowed = False
        else:
            if result.returncode == 0:
                try:
                    parsed_summary = parse_summary_output(
                        result.stdout,
                        ordered_accessions,
                    )
                    return SummaryLookupResult(
                        summary_map=parsed_summary.summary_map,
                        status_map=parsed_summary.status_map,
                        incomplete_accessions=parsed_summary.incomplete_accessions,
                        failures=tuple(failures),
                    )
                except MetadataLookupError as error:
                    error_type = "metadata_lookup"
                    error_message = str(error)
            else:
                error_type = "metadata_lookup"
                error_message = build_subprocess_error_message(
                    "metadata_lookup",
                    result,
                )

        if retry_allowed:
            failures.append(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=attempt_index,
                    max_attempts=max_attempts,
                    error_type=error_type,
                    error_message=error_message,
                    final_status="retry_scheduled",
                    attempted_accession=attempted_accessions,
                ),
            )
            sleep_func(RETRY_DELAYS_SECONDS[attempt_index - 1])
            continue
        failures.append(
            CommandFailureRecord(
                stage="metadata_lookup",
                attempt_index=attempt_index,
                max_attempts=max_attempts,
                error_type=error_type,
                error_message=error_message,
                final_status="retry_exhausted",
                attempted_accession=attempted_accessions,
            ),
        )
        raise MetadataLookupError(error_message, failures=tuple(failures))
    raise RuntimeError(
        "Internal error: metadata retry loop terminated unexpectedly",
    )


def is_suppressed_status(status: str | None) -> bool:
    """Return whether one assembly status value means suppressed."""

    return isinstance(status, str) and status.strip().lower() == "suppressed"


def find_matching_genbank_accessions(
    requested_accession: str,
    discovered_accessions: set[str],
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    *,
    version_latest: bool = False,
) -> tuple[str, ...]:
    """Return matching GenBank accessions for one RefSeq assembly accession."""

    accession_status_map = {} if status_map is None else status_map
    requested_parts = parse_assembly_accession(requested_accession)
    if requested_parts is None:
        return ()
    matching_accessions = select_matching_genbank_candidates(
        requested_accession,
        [
            parsed_accession
            for accession in discovered_accessions
            if (parsed_accession := parse_assembly_accession(accession)) is not None
            and parsed_accession.prefix == "GCA"
            and parsed_accession.numeric_identifier == requested_parts.numeric_identifier
        ],
        version_latest=version_latest,
    )
    matching_accessions.sort(
        key=lambda accession: (
            is_suppressed_status(
                accession_status_map.get(
                    accession.accession,
                    UNKNOWN_ASSEMBLY_STATUS_INFO,
                ).assembly_status,
            ),
            -accession.version,
            accession.accession,
        ),
    )
    return tuple(accession.accession for accession in matching_accessions)


def get_explicit_paired_genbank_candidate(
    requested_accession: str,
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    *,
    version_latest: bool,
) -> ExplicitPairedGenbankCandidate | None:
    """Return one usable explicit paired-GenBank candidate when available."""

    if status_map is None:
        return None
    requested_status_info = status_map.get(requested_accession)
    if requested_status_info is None:
        return None
    paired_accession = requested_status_info.paired_accession
    paired_assembly_status = requested_status_info.paired_assembly_status
    if paired_accession is None and paired_assembly_status is None:
        return None
    if paired_accession is None or paired_assembly_status is None:
        return None
    requested_parts = parse_assembly_accession(requested_accession)
    paired_parts = parse_assembly_accession(paired_accession)
    if requested_parts is None or paired_parts is None:
        return None
    if paired_parts.prefix != "GCA":
        return None
    if paired_parts.numeric_identifier != requested_parts.numeric_identifier:
        return None
    if not version_latest and paired_parts.version != requested_parts.version:
        return None
    return ExplicitPairedGenbankCandidate(
        accession=paired_accession,
        assembly_status=paired_assembly_status,
    )


def classify_explicit_pairing_issue(
    requested_accession: str,
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    *,
    version_latest: bool,
) -> str | None:
    """Return the fallback status for unusable explicit pairing metadata."""

    if status_map is None:
        return None
    requested_status_info = status_map.get(requested_accession)
    if requested_status_info is None:
        return None
    paired_accession = requested_status_info.paired_accession
    paired_assembly_status = requested_status_info.paired_assembly_status
    if paired_accession is None and paired_assembly_status is None:
        return None
    if paired_accession is None or paired_assembly_status is None:
        return "paired_gca_metadata_incomplete_fallback_original"
    requested_parts = parse_assembly_accession(requested_accession)
    paired_parts = parse_assembly_accession(paired_accession)
    if requested_parts is None or paired_parts is None:
        return "paired_gca_conflict_fallback_original"
    if paired_parts.prefix != "GCA":
        return "paired_gca_conflict_fallback_original"
    if paired_parts.numeric_identifier != requested_parts.numeric_identifier:
        return "paired_gca_conflict_fallback_original"
    if not version_latest and paired_parts.version != requested_parts.version:
        return "paired_gca_metadata_incomplete_fallback_original"
    return None


def build_augmented_discovered_accessions(
    discovered_accessions: set[str],
    explicit_candidate: ExplicitPairedGenbankCandidate | None = None,
) -> set[str]:
    """Return discovered accessions plus one explicit paired candidate when known."""

    augmented_accessions = set(discovered_accessions)
    if explicit_candidate is not None:
        augmented_accessions.add(explicit_candidate.accession)
    return augmented_accessions


def get_candidate_status_info(
    accession: str,
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    *,
    explicit_candidate: ExplicitPairedGenbankCandidate | None = None,
) -> AssemblyStatusInfo | None:
    """Return status metadata for one candidate accession."""

    candidate_status_info = None if status_map is None else status_map.get(accession)
    if candidate_status_info is not None:
        return candidate_status_info
    if explicit_candidate is None or accession != explicit_candidate.accession:
        return None
    return AssemblyStatusInfo(
        assembly_status=explicit_candidate.assembly_status,
        suppression_reason=None,
        paired_accession=None,
        paired_assembly_status=None,
    )


def select_preferred_heuristic_genbank_candidate(
    requested_accession: str,
    discovered_accessions: set[str],
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    *,
    version_latest: bool,
    explicit_candidate: ExplicitPairedGenbankCandidate | None = None,
) -> tuple[str | None, AssemblyStatusInfo | None, bool]:
    """Return the best heuristic candidate and whether metadata remain blocking."""

    matching_genbank = find_matching_genbank_accessions(
        requested_accession,
        discovered_accessions,
        status_map=status_map,
        version_latest=version_latest,
    )
    for accession in matching_genbank:
        candidate_status_info = get_candidate_status_info(
            accession,
            status_map,
            explicit_candidate=explicit_candidate,
        )
        if candidate_status_info is None:
            return None, None, True
        return accession, candidate_status_info, False
    return None, None, False


def choose_preferred_accession(
    requested_accession: str,
    discovered_accessions: set[str] | None,
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    prefer_genbank: bool = True,
    version_latest: bool = False,
) -> tuple[str, str]:
    """Choose the final accession and conversion status for one request."""

    if not prefer_genbank:
        return requested_accession, "unchanged_original"
    if requested_accession.startswith("GCA_"):
        return requested_accession, "unchanged_original"
    if discovered_accessions is None:
        return requested_accession, "metadata_lookup_failed_fallback_original"
    explicit_pairing_issue = classify_explicit_pairing_issue(
        requested_accession,
        status_map,
        version_latest=version_latest,
    )
    if explicit_pairing_issue is not None:
        return requested_accession, explicit_pairing_issue
    explicit_candidate = get_explicit_paired_genbank_candidate(
        requested_accession,
        status_map,
        version_latest=version_latest,
    )
    augmented_discovered_accessions = build_augmented_discovered_accessions(
        discovered_accessions,
        explicit_candidate,
    )
    if explicit_candidate is not None and not version_latest:
        if is_suppressed_status(explicit_candidate.assembly_status):
            return requested_accession, "paired_gca_suppressed_fallback_original"
        return explicit_candidate.accession, "paired_to_gca"

    if explicit_candidate is not None and version_latest:
        preferred_accession, preferred_status_info, metadata_incomplete = (
            select_preferred_heuristic_genbank_candidate(
                requested_accession,
                augmented_discovered_accessions,
                status_map=status_map,
                version_latest=True,
                explicit_candidate=explicit_candidate,
            )
        )
        if metadata_incomplete:
            return requested_accession, "paired_gca_metadata_incomplete_fallback_original"
        if preferred_accession is None:
            if is_suppressed_status(explicit_candidate.assembly_status):
                return requested_accession, "paired_gca_suppressed_fallback_original"
            return explicit_candidate.accession, "paired_to_gca"
        if preferred_status_info is None:
            return requested_accession, "paired_gca_metadata_incomplete_fallback_original"
        if is_suppressed_status(preferred_status_info.assembly_status):
            return requested_accession, "paired_gca_suppressed_fallback_original"
        return preferred_accession, "paired_to_gca"

    preferred_accession, preferred_status_info, metadata_incomplete = (
        select_preferred_heuristic_genbank_candidate(
            requested_accession,
            augmented_discovered_accessions,
            status_map=status_map,
            version_latest=version_latest,
        )
    )
    if metadata_incomplete:
        return requested_accession, "paired_gca_metadata_incomplete_fallback_original"
    if preferred_accession is not None and preferred_status_info is not None:
        if is_suppressed_status(preferred_status_info.assembly_status):
            return requested_accession, "paired_gca_suppressed_fallback_original"
        return preferred_accession, "paired_to_gca"
    return requested_accession, "unchanged_original"


def get_accession_type(accession: str) -> str:
    """Return the accession prefix class for one assembly accession."""

    if accession.startswith("GCA_"):
        return "GCA"
    if accession.startswith("GCF_"):
        return "GCF"
    return "unknown"


def build_accession_preference_table(
    accessions: Iterable[str],
    summary_map: dict[str, set[str]],
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    prefer_genbank: bool = True,
    version_latest: bool = False,
) -> pl.DataFrame:
    """Build a Polars table describing the chosen accession for each request."""

    rows: list[dict[str, str]] = []
    for requested_accession in dict.fromkeys(accessions):
        final_accession, conversion_status = choose_preferred_accession(
            requested_accession,
            summary_map.get(requested_accession),
            status_map=status_map,
            prefer_genbank=prefer_genbank,
            version_latest=version_latest,
        )
        rows.append(
            {
                "ncbi_accession": requested_accession,
                "final_accession": final_accession,
                "accession_type_original": get_accession_type(
                    requested_accession,
                ),
                "accession_type_final": get_accession_type(final_accession),
                "conversion_status": conversion_status,
            },
        )
    return pl.DataFrame(
        rows,
        schema={
            "ncbi_accession": pl.String,
            "final_accession": pl.String,
            "accession_type_original": pl.String,
            "accession_type_final": pl.String,
            "conversion_status": pl.String,
        },
    )


def apply_accession_preferences(
    selection_frame: pl.DataFrame,
    summary_map: dict[str, set[str]],
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    prefer_genbank: bool = True,
    version_latest: bool = False,
) -> pl.DataFrame:
    """Attach preferred-accession metadata to a selected taxonomy frame."""

    if selection_frame.is_empty():
        return selection_frame.with_columns(
            pl.lit("").alias("final_accession"),
            pl.lit("").alias("accession_type_original"),
            pl.lit("").alias("accession_type_final"),
            pl.lit("").alias("conversion_status"),
        )
    preference_frame = build_accession_preference_table(
        selection_frame.get_column("ncbi_accession").to_list(),
        summary_map,
        status_map=status_map,
        prefer_genbank=prefer_genbank,
        version_latest=version_latest,
    )
    return selection_frame.join(
        preference_frame,
        on="ncbi_accession",
        how="left",
    )

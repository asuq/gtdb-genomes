"""NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
import time

import polars as pl

from gtdb_genomes.download import (
    CommandFailureRecord,
    RETRY_DELAYS_SECONDS,
)
from gtdb_genomes.subprocess_utils import (
    DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    build_spawn_error_message,
    build_subprocess_error_message,
    build_timeout_error_message,
)


ACCESSION_PATTERN = re.compile(r"(?P<prefix>GC[AF])_(?P<numeric>\d+)\.(?P<version>\d+)")
ACCESSION_STEM_PATTERN = re.compile(r"(?P<prefix>GC[AF])_(?P<numeric>\d+)")
CAMEL_CASE_BOUNDARY_PATTERN = re.compile(r"([a-z0-9])([A-Z])")
NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")
EXPLICIT_ACCESSION_FIELD_NAMES = frozenset({"accession", "paired"})


@dataclass(slots=True)
class MetadataLookupError(Exception):
    """Raised when `datasets summary genome accession` fails."""

    message: str
    failures: tuple[CommandFailureRecord, ...] = ()

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


@dataclass(frozen=True, slots=True)
class AssemblyAccession:
    """Parsed assembly accession components."""

    accession: str
    prefix: str
    numeric_identifier: str
    version: int


@dataclass(frozen=True, slots=True)
class AssemblyAccessionStem:
    """Parsed assembly accession stem components."""

    accession: str
    prefix: str
    numeric_identifier: str


@dataclass(slots=True)
class SummaryLookupResult:
    """Metadata lookup output plus retry history."""

    summary_map: dict[str, set[str]] = field(default_factory=dict)
    status_map: dict[str, "AssemblyStatusInfo"] = field(default_factory=dict)
    failures: tuple[CommandFailureRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class AssemblyStatusInfo:
    """Structured assembly status metadata from one summary record."""

    assembly_status: str | None
    suppression_reason: str | None
    paired_accession: str | None
    paired_assembly_status: str | None


@dataclass(frozen=True, slots=True)
class ParsedSummaryOutput:
    """Parsed accession pairing and status metadata from summary output."""

    summary_map: dict[str, set[str]]
    status_map: dict[str, AssemblyStatusInfo]


SUPPRESSED_ASSEMBLY_NOTE = (
    "NCBI metadata marked this assembly as suppressed; "
    "the genome payload may no longer be downloadable."
)
DATASETS_SUMMARY_JSON_ERROR = (
    "datasets summary returned incompatible JSON-lines output"
)


def build_summary_command(
    accession_file: Path,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
) -> list[str]:
    """Build the datasets summary command for assembly accessions."""

    command = [
        datasets_bin,
        "summary",
        "genome",
        "accession",
        "--inputfile",
        str(accession_file),
        "--as-json-lines",
    ]
    if ncbi_api_key:
        command.extend(["--api-key", ncbi_api_key])
    return command


def parse_assembly_accession(accession: str) -> AssemblyAccession | None:
    """Parse one assembly accession into comparable components."""

    match = ACCESSION_PATTERN.fullmatch(accession)
    if match is None:
        return None
    return AssemblyAccession(
        accession=accession,
        prefix=match.group("prefix"),
        numeric_identifier=match.group("numeric"),
        version=int(match.group("version")),
    )


def parse_assembly_accession_stem(accession: str) -> AssemblyAccessionStem | None:
    """Parse one assembly accession stem into comparable components."""

    match = ACCESSION_STEM_PATTERN.fullmatch(accession)
    if match is None:
        return None
    return AssemblyAccessionStem(
        accession=accession,
        prefix=match.group("prefix"),
        numeric_identifier=match.group("numeric"),
    )


def get_assembly_accession_stem(accession: str) -> str:
    """Return the accession stem without the version suffix."""

    parsed_accession = parse_assembly_accession(accession)
    if parsed_accession is None:
        raise ValueError(f"Invalid assembly accession: {accession}")
    return f"{parsed_accession.prefix}_{parsed_accession.numeric_identifier}"


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


def normalise_field_name(field_name: str) -> str:
    """Return a normalised field name for structured accession matching."""

    separated_name = CAMEL_CASE_BOUNDARY_PATTERN.sub(r"\1_\2", field_name.strip())
    lower_name = separated_name.lower()
    return NON_ALPHANUMERIC_PATTERN.sub("_", lower_name).strip("_")


def field_contains_assembly_accessions(field_name: str) -> bool:
    """Return whether one field name should contain assembly accessions."""

    normalised_field_name = normalise_field_name(field_name)
    if not normalised_field_name:
        return False
    if normalised_field_name in EXPLICIT_ACCESSION_FIELD_NAMES:
        return True
    return normalised_field_name.endswith("_accession") or normalised_field_name.endswith(
        "_accessions",
    )


def extract_explicit_assembly_accessions(payload: object) -> set[str]:
    """Extract exact assembly accessions from one explicit structured value."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for value in payload.values():
            found.update(extract_explicit_assembly_accessions(value))
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_explicit_assembly_accessions(value))
        return found
    if not isinstance(payload, str):
        return found
    parsed_accession = parse_assembly_accession(payload.strip())
    if parsed_accession is not None:
        found.add(parsed_accession.accession)
    return found


def extract_primary_assembly_accession(payload: object) -> str | None:
    """Extract the primary assembly accession from one summary payload."""

    candidates = [
        candidate
        for candidate in (
            get_nested_string_value(payload, "accession"),
            get_nested_string_value(payload, "assembly", "accession"),
        )
        if candidate is not None and parse_assembly_accession(candidate) is not None
    ]
    unique_candidates = tuple(dict.fromkeys(candidates))
    if not unique_candidates:
        return None
    if len(unique_candidates) > 1:
        raise MetadataLookupError(
            f"{DATASETS_SUMMARY_JSON_ERROR}: conflicting primary assembly accessions",
        )
    return unique_candidates[0]


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
    command_runner = subprocess.run if runner is None else runner
    command = build_summary_command(
        accession_file,
        ncbi_api_key=ncbi_api_key,
        datasets_bin=datasets_bin,
    )
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
                timeout=DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            error_type = "metadata_lookup_timeout"
            error_message = build_timeout_error_message(
                "metadata_lookup",
                DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
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
            ),
        )
        raise MetadataLookupError(error_message, failures=tuple(failures))
    raise AssertionError("metadata retry loop terminated unexpectedly")


def extract_structured_accessions(payload: object) -> set[str]:
    """Recursively extract assembly accessions from explicit structured fields."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if field_contains_assembly_accessions(str(key)):
                found.update(extract_explicit_assembly_accessions(value))
            if isinstance(value, dict | list):
                found.update(extract_structured_accessions(value))
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_structured_accessions(value))
        return found
    return found


def get_nested_string_value(
    payload: object,
    *path: str,
) -> str | None:
    """Return one nested string field when every parent is a mapping."""

    current: object = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if not isinstance(current, str):
        return None
    stripped_value = current.strip()
    return stripped_value or None


def build_assembly_status_info(payload: object) -> AssemblyStatusInfo:
    """Extract structured assembly status fields from one summary payload."""

    return AssemblyStatusInfo(
        assembly_status=get_nested_string_value(
            payload,
            "assemblyInfo",
            "assemblyStatus",
        ),
        suppression_reason=get_nested_string_value(
            payload,
            "assemblyInfo",
            "suppressionReason",
        ),
        paired_accession=get_nested_string_value(
            payload,
            "assemblyInfo",
            "pairedAssembly",
            "accession",
        ),
        paired_assembly_status=get_nested_string_value(
            payload,
            "assemblyInfo",
            "pairedAssembly",
            "status",
        ),
    )


def parse_summary_output(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> ParsedSummaryOutput:
    """Parse summary JSON-lines into pairing and status mappings."""

    requested = set(requested_accessions)
    summaries: dict[str, set[str]] = {}
    statuses: dict[str, AssemblyStatusInfo] = {}
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except JSONDecodeError as error:
            raise MetadataLookupError(DATASETS_SUMMARY_JSON_ERROR) from error
        discovered = extract_structured_accessions(payload)
        primary_accession = extract_primary_assembly_accession(payload)
        if primary_accession is None:
            if len(discovered) == 1:
                primary_accession = next(iter(discovered))
            elif requested.intersection(discovered):
                raise MetadataLookupError(
                    f"{DATASETS_SUMMARY_JSON_ERROR}: missing primary assembly accession",
                )
        if primary_accession is not None:
            discovered.add(primary_accession)
        matching_requested = requested.intersection(discovered)
        if not matching_requested:
            continue
        status_info = build_assembly_status_info(payload)
        for requested_accession in matching_requested:
            summaries[requested_accession] = discovered
        if primary_accession is None or primary_accession not in requested:
            continue
        if primary_accession in statuses:
            raise MetadataLookupError(
                f"{DATASETS_SUMMARY_JSON_ERROR}: duplicate primary record for "
                f"{primary_accession}",
            )
        statuses[primary_accession] = status_info
    return ParsedSummaryOutput(summary_map=summaries, status_map=statuses)


def parse_summary_json_lines(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, set[str]]:
    """Map requested accessions to the accessions discovered in summary output."""

    return parse_summary_output(raw_text, requested_accessions).summary_map


def parse_summary_status_map(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, AssemblyStatusInfo]:
    """Map requested accessions to their structured assembly status metadata."""

    return parse_summary_output(raw_text, requested_accessions).status_map


def is_suppressed_status(status: str | None) -> bool:
    """Return whether one assembly status value means suppressed."""

    return isinstance(status, str) and status.strip().lower() == "suppressed"


def find_matching_genbank_accessions(
    requested_accession: str,
    discovered_accessions: set[str],
    status_map: dict[str, AssemblyStatusInfo] | None = None,
) -> tuple[str, ...]:
    """Return matching GenBank accessions for one RefSeq assembly accession."""

    accession_status_map = {} if status_map is None else status_map
    requested_parts = parse_assembly_accession(requested_accession)
    if requested_parts is None:
        return ()
    matching_accessions = [
        parsed_accession
        for accession in discovered_accessions
        if (parsed_accession := parse_assembly_accession(accession)) is not None
        and parsed_accession.prefix == "GCA"
        and parsed_accession.numeric_identifier == requested_parts.numeric_identifier
    ]
    matching_accessions.sort(
        key=lambda accession: (
            is_suppressed_status(
                accession_status_map.get(accession.accession, AssemblyStatusInfo(
                    assembly_status=None,
                    suppression_reason=None,
                    paired_accession=None,
                    paired_assembly_status=None,
                )).assembly_status,
            ),
            -accession.version,
            accession.accession,
        ),
    )
    return tuple(accession.accession for accession in matching_accessions)


def choose_preferred_accession(
    requested_accession: str,
    discovered_accessions: set[str] | None,
    status_map: dict[str, AssemblyStatusInfo] | None = None,
    prefer_genbank: bool = True,
) -> tuple[str, str]:
    """Choose the final accession and conversion status for one request."""

    if not prefer_genbank:
        return requested_accession, "unchanged_original"
    if requested_accession.startswith("GCA_"):
        return requested_accession, "unchanged_original"
    if discovered_accessions is None:
        return requested_accession, "metadata_lookup_failed_fallback_original"
    paired_genbank = find_matching_genbank_accessions(
        requested_accession,
        discovered_accessions,
        status_map=status_map,
    )
    if paired_genbank:
        preferred_accession = paired_genbank[0]
        if is_suppressed_status(
            (status_map or {}).get(
                preferred_accession,
                AssemblyStatusInfo(
                    assembly_status=None,
                    suppression_reason=None,
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            ).assembly_status,
        ):
            return requested_accession, "paired_gca_suppressed_fallback_original"
        return paired_genbank[0], "paired_to_gca"
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
) -> pl.DataFrame:
    """Build a Polars table describing the chosen accession for each request."""

    rows: list[dict[str, str]] = []
    for requested_accession in dict.fromkeys(accessions):
        final_accession, conversion_status = choose_preferred_accession(
            requested_accession,
            summary_map.get(requested_accession),
            status_map=status_map,
            prefer_genbank=prefer_genbank,
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
    )
    return selection_frame.join(
        preference_frame,
        on="ncbi_accession",
        how="left",
    )

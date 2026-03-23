"""Structured `datasets summary` parsing helpers for assembly metadata."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from json import JSONDecodeError

from gtdb_genomes.assembly_accessions import parse_assembly_accession
from gtdb_genomes.download import CommandFailureRecord


CAMEL_CASE_BOUNDARY_PATTERN = re.compile(r"([a-z0-9])([A-Z])")
NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")
KNOWN_ACCESSION_FIELD_PATHS = frozenset(
    {
        ("accession",),
        ("paired",),
        ("paired_accession",),
        ("paired_accessions",),
        ("assembly", "accession"),
        ("assembly", "paired_accession"),
        ("assembly", "paired_accessions"),
        ("assembly_info", "paired_assembly", "accession"),
    },
)
NARROW_FALLBACK_ACCESSION_FIELD_NAMES = frozenset(
    {
        "paired",
        "paired_accession",
        "paired_accessions",
    },
)
DATASETS_SUMMARY_JSON_ERROR = (
    "datasets summary returned incompatible JSON-lines output"
)


@dataclass(slots=True)
class MetadataLookupError(Exception):
    """Raised when `datasets summary genome accession` fails."""

    message: str
    failures: tuple[CommandFailureRecord, ...] = ()

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


@dataclass(frozen=True, slots=True)
class AssemblyStatusInfo:
    """Structured assembly status metadata from one summary record."""

    assembly_status: str | None
    suppression_reason: str | None
    paired_accession: str | None
    paired_assembly_status: str | None


@dataclass(slots=True)
class SummaryLookupResult:
    """Metadata lookup output plus retry history."""

    summary_map: dict[str, set[str]] = field(default_factory=dict)
    status_map: dict[str, AssemblyStatusInfo] = field(default_factory=dict)
    incomplete_accessions: tuple[str, ...] = ()
    failures: tuple[CommandFailureRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class ParsedSummaryOutput:
    """Parsed accession pairing and status metadata from summary output."""

    summary_map: dict[str, set[str]]
    status_map: dict[str, AssemblyStatusInfo]
    incomplete_accessions: tuple[str, ...]


def normalise_field_name(field_name: str) -> str:
    """Return a normalised field name for structured accession matching."""

    separated_name = CAMEL_CASE_BOUNDARY_PATTERN.sub(r"\1_\2", field_name.strip())
    lower_name = separated_name.lower()
    return NON_ALPHANUMERIC_PATTERN.sub("_", lower_name).strip("_")


def field_contains_assembly_accessions(field_name: str) -> bool:
    """Return whether one field name is part of the narrow fallback allowlist."""

    normalised_field_name = normalise_field_name(field_name)
    return normalised_field_name in NARROW_FALLBACK_ACCESSION_FIELD_NAMES


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


def extract_known_structured_accessions(
    payload: object,
    *,
    path: tuple[str, ...] = (),
) -> set[str]:
    """Extract assembly accessions from known `datasets` summary field paths."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            normalised_key = normalise_field_name(str(key))
            if not normalised_key:
                continue
            child_path = path + (normalised_key,)
            if child_path in KNOWN_ACCESSION_FIELD_PATHS:
                found.update(extract_explicit_assembly_accessions(value))
            if isinstance(value, dict | list):
                found.update(
                    extract_known_structured_accessions(value, path=child_path),
                )
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_known_structured_accessions(value, path=path))
    return found


def extract_narrow_fallback_accessions(payload: object) -> set[str]:
    """Extract assembly accessions from narrow fallback field names only."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if field_contains_assembly_accessions(str(key)):
                found.update(extract_explicit_assembly_accessions(value))
            if isinstance(value, dict | list):
                found.update(extract_narrow_fallback_accessions(value))
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_narrow_fallback_accessions(value))
    return found


def extract_structured_accessions(payload: object) -> set[str]:
    """Extract assembly accessions from known fields plus a narrow fallback."""

    return extract_known_structured_accessions(
        payload,
    ) | extract_narrow_fallback_accessions(payload)


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


def get_first_nested_string_value(
    payload: object,
    *paths: tuple[str, ...],
) -> str | None:
    """Return the first populated nested string from alternative paths."""

    for path in paths:
        value = get_nested_string_value(payload, *path)
        if value is not None:
            return value
    return None


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


def build_assembly_status_info(payload: object) -> AssemblyStatusInfo:
    """Extract structured assembly status fields from one summary payload."""

    return AssemblyStatusInfo(
        assembly_status=get_first_nested_string_value(
            payload,
            ("assemblyInfo", "assemblyStatus"),
            ("assembly_info", "assembly_status"),
        ),
        suppression_reason=get_first_nested_string_value(
            payload,
            ("assemblyInfo", "suppressionReason"),
            ("assembly_info", "suppression_reason"),
        ),
        paired_accession=get_first_nested_string_value(
            payload,
            ("assemblyInfo", "pairedAssembly", "accession"),
            ("assembly_info", "paired_assembly", "accession"),
        ),
        paired_assembly_status=get_first_nested_string_value(
            payload,
            ("assemblyInfo", "pairedAssembly", "status"),
            ("assembly_info", "paired_assembly", "status"),
        ),
    )


def has_complete_assembly_status_info(
    status_info: AssemblyStatusInfo | None,
) -> bool:
    """Return whether one parsed status record contains usable status metadata."""

    if status_info is None:
        return False
    return status_info.assembly_status is not None


def parse_summary_output(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> ParsedSummaryOutput:
    """Parse summary JSON-lines into pairing and status mappings."""

    ordered_requested_accessions = tuple(dict.fromkeys(requested_accessions))
    requested = set(ordered_requested_accessions)
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
    incomplete_accessions = tuple(
        accession
        for accession in ordered_requested_accessions
        if accession not in summaries
        or not has_complete_assembly_status_info(statuses.get(accession))
    )
    return ParsedSummaryOutput(
        summary_map=summaries,
        status_map=statuses,
        incomplete_accessions=incomplete_accessions,
    )


def parse_summary_json_lines(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, set[str]]:
    """Map requested accessions to discovered accessions from summary output."""

    return parse_summary_output(raw_text, requested_accessions).summary_map


def parse_summary_status_map(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, AssemblyStatusInfo]:
    """Map requested accessions to structured assembly status metadata."""

    return parse_summary_output(raw_text, requested_accessions).status_map

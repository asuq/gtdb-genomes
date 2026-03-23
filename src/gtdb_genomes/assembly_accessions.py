"""Assembly accession parsing and candidate-selection helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass


ACCESSION_PATTERN = re.compile(
    r"(?P<prefix>GC[AF])_(?P<numeric>\d+)\.(?P<version>\d+)",
)
ACCESSION_STEM_PATTERN = re.compile(r"(?P<prefix>GC[AF])_(?P<numeric>\d+)")


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


def select_matching_genbank_candidates(
    requested_accession: str,
    matching_accessions: list[AssemblyAccession],
    *,
    version_latest: bool,
) -> list[AssemblyAccession]:
    """Return the GenBank candidates allowed by the version-selection mode."""

    if version_latest:
        return matching_accessions
    requested_parts = parse_assembly_accession(requested_accession)
    if requested_parts is None:
        return []
    return [
        accession
        for accession in matching_accessions
        if accession.version == requested_parts.version
    ]

"""NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from json import JSONDecodeError
import time

import polars as pl

from gtdb_genomes.download import CommandFailureRecord, RETRY_DELAYS_SECONDS


ACCESSION_PATTERN = re.compile(r"(?P<prefix>GC[AF])_(?P<numeric>\d+)\.(?P<version>\d+)")


@dataclass(slots=True)
class MetadataLookupError(Exception):
    """Raised when `datasets summary genome accession` fails."""

    message: str

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


@dataclass(slots=True)
class SummaryLookupResult:
    """Metadata lookup output plus retry history."""

    summary_map: dict[str, set[str]]
    failures: tuple[CommandFailureRecord, ...]


def build_summary_command(
    accessions: Iterable[str],
    api_key: str | None = None,
    datasets_bin: str = "datasets",
) -> list[str]:
    """Build the datasets summary command for assembly accessions."""

    command = [
        datasets_bin,
        "summary",
        "genome",
        "accession",
        *accessions,
        "--as-json-lines",
    ]
    if api_key:
        command.extend(["--api-key", api_key])
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


def run_summary_lookup(
    accessions: Iterable[str],
    api_key: str | None = None,
    datasets_bin: str = "datasets",
) -> dict[str, set[str]]:
    """Look up accession metadata through the datasets CLI."""

    ordered_accessions = tuple(dict.fromkeys(accessions))
    if not ordered_accessions:
        return {}
    command = build_summary_command(
        ordered_accessions,
        api_key=api_key,
        datasets_bin=datasets_bin,
    )
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        if not error_message:
            error_message = "datasets summary genome accession failed"
        raise MetadataLookupError(error_message)
    return parse_summary_json_lines(result.stdout, ordered_accessions)


def run_summary_lookup_with_retries(
    accessions: Iterable[str],
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    sleep_func: Callable[[float], None] = time.sleep,
) -> SummaryLookupResult:
    """Look up accession metadata with the fixed retry budget."""

    ordered_accessions = tuple(dict.fromkeys(accessions))
    if not ordered_accessions:
        return SummaryLookupResult(summary_map={}, failures=())
    command = build_summary_command(
        ordered_accessions,
        api_key=api_key,
        datasets_bin=datasets_bin,
    )
    max_attempts = len(RETRY_DELAYS_SECONDS) + 1
    failures: list[CommandFailureRecord] = []
    for attempt_index in range(1, max_attempts + 1):
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            try:
                return SummaryLookupResult(
                    summary_map=parse_summary_json_lines(
                        result.stdout,
                        ordered_accessions,
                    ),
                    failures=tuple(failures),
                )
            except MetadataLookupError as error:
                error_message = str(error)
        else:
            error_message = result.stderr.strip() or result.stdout.strip()
            if not error_message:
                error_message = "datasets summary genome accession failed"
        if attempt_index < max_attempts:
            failures.append(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=attempt_index,
                    max_attempts=max_attempts,
                    error_type="metadata_lookup",
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
                error_type="metadata_lookup",
                error_message=error_message,
                final_status="retry_exhausted",
            ),
        )
        raise MetadataLookupError(error_message)
    raise AssertionError("metadata retry loop terminated unexpectedly")


def extract_accessions(payload: object) -> set[str]:
    """Recursively extract assembly accessions from a JSON-like payload."""

    found: set[str] = set()
    if isinstance(payload, dict):
        for value in payload.values():
            found.update(extract_accessions(value))
        return found
    if isinstance(payload, list):
        for value in payload:
            found.update(extract_accessions(value))
        return found
    if isinstance(payload, str):
        found.update(
            match.group(0)
            for match in ACCESSION_PATTERN.finditer(payload)
        )
    return found


def parse_summary_json_lines(
    raw_text: str,
    requested_accessions: Iterable[str],
) -> dict[str, set[str]]:
    """Map requested accessions to the accessions discovered in summary output."""

    requested = set(requested_accessions)
    summaries: dict[str, set[str]] = {}
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except JSONDecodeError as error:
            raise MetadataLookupError("datasets summary returned invalid JSON") from error
        discovered = extract_accessions(payload)
        matching_requested = requested.intersection(discovered)
        for requested_accession in matching_requested:
            summaries[requested_accession] = discovered
    return summaries


def find_matching_genbank_accessions(
    requested_accession: str,
    discovered_accessions: set[str],
) -> tuple[str, ...]:
    """Return matching GenBank accessions for one RefSeq assembly accession."""

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
        key=lambda accession: accession.version,
        reverse=True,
    )
    return tuple(accession.accession for accession in matching_accessions)


def choose_preferred_accession(
    requested_accession: str,
    discovered_accessions: set[str] | None,
    prefer_genbank: bool = True,
) -> tuple[str, str]:
    """Choose the final accession and conversion status for one request."""

    if not prefer_genbank:
        return requested_accession, "unchanged_original"
    if discovered_accessions is None:
        return requested_accession, "metadata_lookup_failed_fallback_original"
    if requested_accession.startswith("GCA_"):
        return requested_accession, "unchanged_original"
    paired_genbank = find_matching_genbank_accessions(
        requested_accession,
        discovered_accessions,
    )
    if paired_genbank:
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
    prefer_genbank: bool = True,
) -> pl.DataFrame:
    """Build a Polars table describing the chosen accession for each request."""

    rows: list[dict[str, str]] = []
    for requested_accession in dict.fromkeys(accessions):
        final_accession, conversion_status = choose_preferred_accession(
            requested_accession,
            summary_map.get(requested_accession),
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
        prefer_genbank=prefer_genbank,
    )
    return selection_frame.join(
        preference_frame,
        on="ncbi_accession",
        how="left",
    )

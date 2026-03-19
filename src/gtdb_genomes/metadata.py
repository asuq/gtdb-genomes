"""NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl


ACCESSION_PATTERN = re.compile(r"GC[AF]_\d+\.\d+")


@dataclass(slots=True)
class MetadataLookupError(Exception):
    """Raised when `datasets summary genome accession` fails."""

    message: str

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


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
        found.update(ACCESSION_PATTERN.findall(payload))
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
        payload = json.loads(line)
        discovered = extract_accessions(payload)
        matching_requested = requested.intersection(discovered)
        if len(matching_requested) == 1:
            summaries[next(iter(matching_requested))] = discovered
    return summaries


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
    paired_gca = sorted(
        accession
        for accession in discovered_accessions
        if accession.startswith("GCA_")
    )
    if paired_gca:
        return paired_gca[0], "paired_to_gca"
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

"""Stable run-identity helpers for realised accession decisions."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from gtdb_genomes.provenance import RuntimeProvenance


ACCESSION_DECISION_FIELDS = (
    "gtdb_accession",
    "ncbi_accession",
    "selected_accession",
    "download_request_accession",
    "final_accession",
    "conversion_status",
    "download_status",
)


def normalise_accession_decision_value(value: object) -> str:
    """Return one accession-decision field as a stable string value."""

    return "" if value is None else str(value).strip()


def build_accession_decision_records(
    accession_rows: list[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Return a deduplicated, sorted accession-decision record set."""

    unique_records = {
        tuple(
            (
                field_name,
                normalise_accession_decision_value(row.get(field_name, "")),
            )
            for field_name in ACCESSION_DECISION_FIELDS
        )
        for row in accession_rows
    }
    sorted_records = sorted(
        unique_records,
        key=lambda record: tuple(value for _, value in record),
    )
    return [
        {
            field_name: field_value
            for field_name, field_value in record
        }
        for record in sorted_records
    ]


def build_accession_decision_sha256(
    accession_rows: list[Mapping[str, Any]],
) -> str:
    """Return the stable digest for the realised accession decision map."""

    encoded_records = json.dumps(
        build_accession_decision_records(accession_rows),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_records).hexdigest()


def build_deterministic_run_id(
    *,
    requested_release: str,
    resolved_release: str,
    requested_taxa: tuple[str, ...],
    include: str,
    prefer_genbank: bool,
    version_latest: bool,
    provenance: RuntimeProvenance,
    accession_decision_sha256: str,
) -> str:
    """Return a deterministic run identifier for the realised output."""

    payload = {
        "requested_release": requested_release.strip(),
        "resolved_release": resolved_release,
        "requested_taxa": list(requested_taxa),
        "include": include,
        "prefer_genbank": prefer_genbank,
        "version_latest": version_latest,
        "package_version": provenance.package_version,
        "git_revision": provenance.git_revision,
        "datasets_version": provenance.datasets_version,
        "unzip_version": provenance.unzip_version,
        "release_manifest_sha256": provenance.release_manifest_sha256,
        "bacterial_taxonomy_sha256": provenance.bacterial_taxonomy_sha256,
        "archaeal_taxonomy_sha256": provenance.archaeal_taxonomy_sha256,
        "accession_decision_sha256": accession_decision_sha256,
    }
    encoded_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()

"""Tests for stable realised-output run identity helpers."""

from __future__ import annotations

from gtdb_genomes.provenance import RuntimeProvenance
from gtdb_genomes.run_identity import (
    build_accession_decision_sha256,
    build_deterministic_run_id,
)


def build_test_provenance() -> RuntimeProvenance:
    """Return one fixed provenance payload for run-identity tests."""

    return RuntimeProvenance(
        package_version="1.2.3",
        git_revision="deadbeef",
        datasets_version="datasets 2.0",
        unzip_version="UnZip 6.00",
        release_manifest_sha256="0" * 64,
        bacterial_taxonomy_sha256="1" * 64,
        archaeal_taxonomy_sha256=None,
    )


def build_test_accession_rows() -> list[dict[str, str]]:
    """Return one stable accession-decision row set."""

    return [
        {
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "selected_accession": "GCA_000001.3",
            "download_request_accession": "GCA_000001",
            "final_accession": "GCA_000001.3",
            "conversion_status": "paired_to_gca",
            "download_status": "downloaded",
        },
        {
            "gtdb_accession": "GB_GCA_000002.1",
            "ncbi_accession": "GCA_000002.1",
            "selected_accession": "GCA_000002.1",
            "download_request_accession": "GCA_000002.1",
            "final_accession": "",
            "conversion_status": "failed_no_usable_accession",
            "download_status": "failed",
        },
    ]


def test_accession_decision_sha256_is_order_and_duplicate_stable() -> None:
    """The accession-decision digest should ignore ordering and duplicates."""

    accession_rows = build_test_accession_rows()

    assert build_accession_decision_sha256(accession_rows) == (
        build_accession_decision_sha256(
            [
                accession_rows[1],
                accession_rows[0],
                accession_rows[0],
            ],
        )
    )


def test_accession_decision_sha256_changes_with_biological_output_fields() -> None:
    """Changing realised accession fields should change the decision digest."""

    accession_rows = build_test_accession_rows()
    changed_rows = [dict(row) for row in accession_rows]
    changed_rows[0]["final_accession"] = "GCA_000001.4"

    assert build_accession_decision_sha256(accession_rows) != (
        build_accession_decision_sha256(changed_rows)
    )


def test_run_id_changes_when_accession_decision_digest_changes() -> None:
    """The run identifier should track the realised accession decision digest."""

    provenance = build_test_provenance()
    accession_rows = build_test_accession_rows()
    fixed_digest = build_accession_decision_sha256(accession_rows)
    changed_rows = [dict(row) for row in accession_rows]
    changed_rows[0]["download_request_accession"] = "GCA_000001.3"
    changed_digest = build_accession_decision_sha256(changed_rows)

    fixed_run_id = build_deterministic_run_id(
        requested_release="95",
        resolved_release="95",
        requested_taxa=("g__Example",),
        include="genome",
        prefer_genbank=True,
        version_latest=True,
        provenance=provenance,
        accession_decision_sha256=fixed_digest,
    )
    changed_run_id = build_deterministic_run_id(
        requested_release="95",
        resolved_release="95",
        requested_taxa=("g__Example",),
        include="genome",
        prefer_genbank=True,
        version_latest=True,
        provenance=provenance,
        accession_decision_sha256=changed_digest,
    )

    assert fixed_run_id != changed_run_id

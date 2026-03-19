"""Tests for NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import subprocess

import polars as pl
import pytest

from gtdb_genomes.metadata import (
    MetadataLookupError,
    apply_accession_preferences,
    build_summary_command,
    run_summary_lookup,
)


def test_build_summary_command_includes_api_key() -> None:
    """The summary command should pass the requested API key through."""

    command = build_summary_command(
        ["GCF_000001.1"],
        api_key="secret",
        datasets_bin="datasets",
    )

    assert command == [
        "datasets",
        "summary",
        "genome",
        "accession",
        "GCF_000001.1",
        "--as-json-lines",
        "--api-key",
        "secret",
    ]


def test_run_summary_lookup_parses_requested_accessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lookup runner should parse JSON-lines output into accession sets."""

    payload = (
        '{"accession":"GCF_000001.1","paired":"GCA_000001.1"}\n'
        '{"accession":"GCA_000002.1"}\n'
    )

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake successful datasets response."""

        assert command[:4] == ["datasets", "summary", "genome", "accession"]
        assert capture_output is True
        assert text is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    summary_map = run_summary_lookup(["GCF_000001.1", "GCA_000002.1"])

    assert summary_map == {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
        "GCA_000002.1": {"GCA_000002.1"},
    }


def test_run_summary_lookup_raises_on_command_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lookup failures should raise a dedicated metadata error."""

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake failed datasets response."""

        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="metadata lookup failed",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(MetadataLookupError, match="metadata lookup failed"):
        run_summary_lookup(["GCF_000001.1"])


def test_apply_accession_preferences_emits_fixed_status_values() -> None:
    """Preference mapping should emit the documented conversion statuses."""

    selection_frame = pl.DataFrame(
        {
            "requested_taxon": [
                "g__Escherichia",
                "g__Haloferax",
                "g__Bacillus",
            ],
            "taxon_slug": [
                "g__Escherichia",
                "g__Haloferax",
                "g__Bacillus",
            ],
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "GB_GCA_000002.1",
                "RS_GCF_000003.1",
            ],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCA_000002.1",
                "GCF_000003.1",
            ],
        },
    )

    summary_map = {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
        "GCA_000002.1": {"GCA_000002.1"},
    }

    mapped = apply_accession_preferences(selection_frame, summary_map)

    assert mapped.select(
        "ncbi_accession",
        "final_accession",
        "accession_type_original",
        "accession_type_final",
        "conversion_status",
    ).rows(named=True) == [
        {
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCA_000001.1",
            "accession_type_original": "GCF",
            "accession_type_final": "GCA",
            "conversion_status": "paired_to_gca",
        },
        {
            "ncbi_accession": "GCA_000002.1",
            "final_accession": "GCA_000002.1",
            "accession_type_original": "GCA",
            "accession_type_final": "GCA",
            "conversion_status": "unchanged_original",
        },
        {
            "ncbi_accession": "GCF_000003.1",
            "final_accession": "GCF_000003.1",
            "accession_type_original": "GCF",
            "accession_type_final": "GCF",
            "conversion_status": "metadata_lookup_failed_fallback_original",
        },
    ]


def test_apply_accession_preferences_honours_disabled_gca_preference() -> None:
    """Disabling GCA preference should keep the original accession."""

    selection_frame = pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
        },
    )

    mapped = apply_accession_preferences(
        selection_frame,
        {"GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"}},
        prefer_genbank=False,
    )

    assert mapped.select("final_accession", "conversion_status").rows(
        named=True,
    ) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "unchanged_original",
        },
    ]

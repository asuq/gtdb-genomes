"""Tests for NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.metadata import (
    MetadataLookupError,
    apply_accession_preferences,
    build_download_request_accession,
    build_summary_command,
    choose_preferred_accession,
    get_assembly_accession_stem,
    parse_summary_json_lines,
    run_summary_lookup_with_retries,
)


def test_build_summary_command_includes_ncbi_api_key() -> None:
    """The summary command should pass the requested API key through."""

    command = build_summary_command(
        Path("/tmp/accessions.txt"),
        ncbi_api_key="secret",
        datasets_bin="datasets",
    )

    assert command == [
        "datasets",
        "summary",
        "genome",
        "accession",
        "--inputfile",
        "/tmp/accessions.txt",
        "--as-json-lines",
        "--api-key",
        "secret",
    ]


def test_run_summary_lookup_with_retries_parses_requested_accessions(
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
        assert "--inputfile" in command
        assert "GCF_000001.1" not in command
        assert "GCA_000002.1" not in command
        assert capture_output is True
        assert text is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCF_000001.1", "GCA_000002.1"],
        Path("/tmp/accessions.txt"),
    )

    assert result.summary_map == {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
        "GCA_000002.1": {"GCA_000002.1"},
    }
    assert result.failures == ()


def test_run_summary_lookup_with_retries_raises_on_command_failure(
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
        run_summary_lookup_with_retries(
            ["GCF_000001.1"],
            Path("/tmp/accessions.txt"),
            sleep_func=lambda delay: None,
        )


def test_choose_preferred_accession_keeps_native_genbank_on_metadata_failure() -> None:
    """A native GenBank accession should stay unchanged without metadata."""

    assert choose_preferred_accession("GCA_000002.1", None) == (
        "GCA_000002.1",
        "unchanged_original",
    )


def test_build_download_request_accession_uses_stems_for_latest_mode() -> None:
    """Latest-mode requests should drop the version suffix."""

    assert build_download_request_accession(
        "GCA_000002.7",
        prefer_genbank=True,
        version_fixed=False,
    ) == "GCA_000002"
    assert build_download_request_accession(
        "GCF_000003.4",
        prefer_genbank=True,
        version_fixed=False,
    ) == "GCF_000003"
    assert build_download_request_accession(
        "GCA_000002.7",
        prefer_genbank=True,
        version_fixed=True,
    ) == "GCA_000002.7"
    assert build_download_request_accession(
        "GCF_000003.4",
        prefer_genbank=False,
        version_fixed=False,
    ) == "GCF_000003.4"


def test_get_assembly_accession_stem_rejects_invalid_values() -> None:
    """Stem parsing should reject non-assembly accessions."""

    with pytest.raises(ValueError, match="Invalid assembly accession"):
        get_assembly_accession_stem("not-an-accession")


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


def test_apply_accession_preferences_uses_shared_numeric_identifier() -> None:
    """Only GCA accessions with the same numeric identifier should pair."""

    selection_frame = pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.2"],
            "ncbi_accession": ["GCF_000001.2"],
        },
    )

    mapped = apply_accession_preferences(
        selection_frame,
        {
            "GCF_000001.2": {
                "GCF_000001.2",
                "GCA_000001.1",
                "GCA_000001.3",
                "GCA_999999.9",
            },
        },
    )

    assert mapped.select("final_accession", "conversion_status").rows(
        named=True,
    ) == [
        {
            "final_accession": "GCA_000001.3",
            "conversion_status": "paired_to_gca",
        },
    ]


def test_parse_summary_json_lines_ignores_unrelated_accession_text() -> None:
    """Structured accession fields should win over incidental free-text mentions."""

    payload = (
        '{"assembly":{"accession":"GCF_000001.2",'
        '"pairedAccessions":["GCA_000001.1","GCA_000001.3"]},'
        '"note":"Unrelated archive mention GCA_000001.9 should be ignored",'
        '"comment":"GCA_000001.8"}\n'
    )

    parsed = parse_summary_json_lines(payload, ["GCF_000001.2"])

    assert parsed == {
        "GCF_000001.2": {
            "GCF_000001.2",
            "GCA_000001.1",
            "GCA_000001.3",
        },
    }
    assert choose_preferred_accession(
        "GCF_000001.2",
        parsed["GCF_000001.2"],
    ) == (
        "GCA_000001.3",
        "paired_to_gca",
    )


def test_run_summary_lookup_with_retries_retries_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid JSON should consume the metadata retry budget."""

    attempts = iter(
        [
            subprocess.CompletedProcess(
                ["datasets"],
                0,
                stdout="{not json}\n",
                stderr="",
            ),
            subprocess.CompletedProcess(
                ["datasets"],
                0,
                stdout="{still not json}\n",
                stderr="",
            ),
            subprocess.CompletedProcess(
                ["datasets"],
                0,
                stdout='{"accession":"GCF_000001.1","paired":"GCA_000001.1"}\n',
                stderr="",
            ),
        ],
    )
    sleep_calls: list[float] = []

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return retryable metadata responses."""

        return next(attempts)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCF_000001.1"],
        Path("/tmp/accessions.txt"),
        sleep_func=sleep_calls.append,
    )

    assert result.summary_map == {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
    }
    assert sleep_calls == [5, 15]
    assert [failure.final_status for failure in result.failures] == [
        "retry_scheduled",
        "retry_scheduled",
    ]


def test_run_summary_lookup_with_retries_raises_after_full_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata lookup should fail only after the full retry budget."""

    attempts = iter([1, 1, 1, 1])
    sleep_calls: list[float] = []

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return repeated metadata lookup failures."""

        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="metadata lookup failed",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(MetadataLookupError, match="metadata lookup failed"):
        run_summary_lookup_with_retries(
            ["GCF_000001.1"],
            Path("/tmp/accessions.txt"),
            sleep_func=sleep_calls.append,
        )

    assert sleep_calls == [5, 15, 45]


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

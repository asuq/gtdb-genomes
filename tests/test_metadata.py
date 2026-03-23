"""Tests for NCBI metadata lookup and accession preference handling."""

from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    MetadataLookupError,
    apply_accession_preferences,
    build_download_request_accession,
    build_summary_command,
    choose_preferred_accession,
    get_assembly_accession_stem,
    parse_summary_json_lines,
    parse_summary_status_map,
    run_summary_lookup_with_retries,
)
from gtdb_genomes.subprocess_utils import NCBI_API_KEY_ENV_VAR

COMMAND_TEST_ACCESSION_FILE = Path("tmp") / "accessions.txt"


def test_build_summary_command_uses_input_file_without_api_key_argv() -> None:
    """The summary command should rely on the input file and omit API-key argv."""

    command = build_summary_command(
        COMMAND_TEST_ACCESSION_FILE,
        datasets_bin="datasets",
    )

    assert command == [
        "datasets",
        "summary",
        "genome",
        "accession",
        "--inputfile",
        str(COMMAND_TEST_ACCESSION_FILE),
        "--as-json-lines",
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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake successful datasets response."""

        assert command[:4] == ["datasets", "summary", "genome", "accession"]
        assert "--inputfile" in command
        assert "GCF_000001.1" not in command
        assert "GCA_000002.1" not in command
        assert capture_output is True
        assert text is True
        assert check is False
        assert env is not None
        assert NCBI_API_KEY_ENV_VAR not in env
        return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCF_000001.1", "GCA_000002.1"],
        COMMAND_TEST_ACCESSION_FILE,
    )

    assert result.summary_map == {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
        "GCA_000002.1": {"GCA_000002.1"},
    }
    assert result.status_map == {
        "GCF_000001.1": AssemblyStatusInfo(
            assembly_status=None,
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
        "GCA_000002.1": AssemblyStatusInfo(
            assembly_status=None,
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }
    assert result.failures == ()


def test_run_summary_lookup_with_retries_marks_silent_omissions_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Silent omissions should be tracked as incomplete requested metadata."""

    payload = (
        '{"accession":"GCA_000001.1",'
        '"assembly_info":{"assembly_status":"current"}}\n'
    )

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one successful lookup with a silently omitted accession."""

        del capture_output, text, check, timeout
        assert env is not None
        assert NCBI_API_KEY_ENV_VAR not in env
        return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCA_000001.1", "GCA_000002.1"],
        COMMAND_TEST_ACCESSION_FILE,
    )

    assert result.summary_map == {
        "GCA_000001.1": {"GCA_000001.1"},
    }
    assert result.incomplete_accessions == ("GCA_000002.1",)


def test_run_summary_lookup_with_retries_raises_on_command_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lookup failures should raise a dedicated metadata error."""

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake failed datasets response."""

        assert env is not None
        assert NCBI_API_KEY_ENV_VAR not in env
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
            COMMAND_TEST_ACCESSION_FILE,
            sleep_func=lambda delay: None,
        )


def test_run_summary_lookup_with_retries_passes_api_key_via_child_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata lookup should pass the API key through the child environment."""

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one successful lookup and capture the child environment."""

        del capture_output, text, check, timeout
        assert command[:4] == ["datasets", "summary", "genome", "accession"]
        assert "--api-key" not in command
        assert env is not None
        assert env[NCBI_API_KEY_ENV_VAR] == "secret"
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='{"accession":"GCF_000001.1"}\n',
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCF_000001.1"],
        COMMAND_TEST_ACCESSION_FILE,
        ncbi_api_key="secret",
    )

    assert result.summary_map == {
        "GCF_000001.1": {"GCF_000001.1"},
    }


def test_choose_preferred_accession_keeps_native_genbank_on_metadata_failure() -> None:
    """A native GenBank accession should stay unchanged without metadata."""

    assert choose_preferred_accession("GCA_000002.1", None) == (
        "GCA_000002.1",
        "unchanged_original",
    )


def test_choose_preferred_accession_keeps_exact_matching_gca_version_by_default() -> None:
    """Default GenBank preference should keep the matching versioned accession."""

    discovered_accessions = {
        "GCF_000001.2",
        "GCA_000001.2",
        "GCA_000001.3",
    }
    status_map = {
        "GCA_000001.2": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
        "GCA_000001.3": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="removed by submitter",
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }

    assert choose_preferred_accession(
        "GCF_000001.2",
        discovered_accessions,
        status_map=status_map,
    ) == (
        "GCA_000001.2",
        "paired_to_gca",
    )


def test_choose_preferred_accession_version_latest_prefers_unsuppressed_gca_over_newer_suppressed() -> None:
    """Latest-mode should still prefer an unsuppressed candidate over a newer suppressed one."""

    discovered_accessions = {
        "GCF_000001.1",
        "GCA_000001.2",
        "GCA_000001.3",
    }
    status_map = {
        "GCA_000001.2": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
        "GCA_000001.3": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="replaced by newer record",
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }

    assert choose_preferred_accession(
        "GCF_000001.1",
        discovered_accessions,
        status_map=status_map,
        version_latest=True,
    ) == (
        "GCA_000001.2",
        "paired_to_gca",
    )


def test_choose_preferred_accession_keeps_original_when_exact_gca_version_is_absent() -> None:
    """Default GenBank preference should not upgrade to a different revision."""

    discovered_accessions = {
        "GCF_000001.2",
        "GCA_000001.1",
        "GCA_000001.3",
    }
    status_map = {
        "GCA_000001.1": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
        "GCA_000001.3": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }

    assert choose_preferred_accession(
        "GCF_000001.2",
        discovered_accessions,
        status_map=status_map,
    ) == (
        "GCF_000001.2",
        "unchanged_original",
    )


def test_choose_preferred_accession_falls_back_when_all_gca_matches_are_suppressed() -> None:
    """Fully suppressed GenBank matches should keep the original accession."""

    discovered_accessions = {
        "GCF_000001.1",
        "GCA_000001.2",
        "GCA_000001.3",
    }
    status_map = {
        "GCA_000001.2": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="removed by submitter",
            paired_accession=None,
            paired_assembly_status=None,
        ),
        "GCA_000001.3": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="replaced by newer record",
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }

    assert choose_preferred_accession(
        "GCF_000001.1",
        discovered_accessions,
        status_map=status_map,
        version_latest=True,
    ) == (
        "GCF_000001.1",
        "paired_gca_suppressed_fallback_original",
    )


def test_choose_preferred_accession_falls_back_when_exact_gca_status_is_unknown() -> None:
    """Unknown exact-version metadata should never be treated as safe to promote."""

    discovered_accessions = {
        "GCF_000001.2",
        "GCA_000001.2",
    }

    assert choose_preferred_accession(
        "GCF_000001.2",
        discovered_accessions,
        status_map={},
    ) == (
        "GCF_000001.2",
        "paired_gca_metadata_incomplete_fallback_original",
    )


def test_build_download_request_accession_defaults_to_fixed_version_requests() -> None:
    """Prefer-GenBank should keep the selected version unless latest-mode is enabled."""

    assert build_download_request_accession(
        "GCA_000002.7",
        prefer_genbank=True,
        version_latest=False,
    ) == "GCA_000002.7"
    assert build_download_request_accession(
        "GCF_000003.4",
        prefer_genbank=True,
        version_latest=False,
    ) == "GCF_000003.4"
    assert build_download_request_accession(
        "GCA_000002.7",
        prefer_genbank=True,
        version_latest=True,
    ) == "GCA_000002"
    assert build_download_request_accession(
        "GCF_000003.4",
        prefer_genbank=True,
        version_latest=True,
    ) == "GCF_000003"
    assert build_download_request_accession(
        "GCF_000003.4",
        prefer_genbank=False,
        version_latest=False,
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
    status_map = {
        "GCA_000001.1": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }

    mapped = apply_accession_preferences(
        selection_frame,
        summary_map,
        status_map=status_map,
    )

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


def test_apply_accession_preferences_uses_shared_numeric_identifier_in_latest_mode() -> None:
    """Latest-mode should pair only accessions with the same numeric identifier."""

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
        status_map={
            "GCA_000001.1": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
            "GCA_000001.3": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
        version_latest=True,
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
        status_map={
            "GCA_000001.1": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
            "GCA_000001.3": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
        version_latest=True,
    ) == (
        "GCA_000001.3",
        "paired_to_gca",
    )


def test_parse_summary_status_map_extracts_suppressed_fields() -> None:
    """Structured summary payloads should preserve assembly suppression fields."""

    payload = (
        '{"accession":"GCF_003670205.1",'
        '"assemblyInfo":{"assemblyStatus":"suppressed",'
        '"suppressionReason":"removed by submitter",'
        '"pairedAssembly":{"accession":"GCA_003670205.2","status":"current"}}}\n'
    )

    parsed = parse_summary_status_map(payload, ["GCF_003670205.1"])

    assert parsed == {
        "GCF_003670205.1": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="removed by submitter",
            paired_accession="GCA_003670205.2",
            paired_assembly_status="current",
        ),
    }


def test_parse_summary_status_map_supports_snake_case_status_fields() -> None:
    """Real datasets snake_case payloads should populate suppression fields."""

    payload = (
        '{"accession":"GCF_003670205.1",'
        '"assembly_info":{"assembly_status":"suppressed",'
        '"suppression_reason":"removed because contaminated",'
        '"paired_assembly":{"accession":"GCA_003670205.1",'
        '"status":"suppressed"}}}\n'
    )

    parsed = parse_summary_status_map(payload, ["GCF_003670205.1"])

    assert parsed == {
        "GCF_003670205.1": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason="removed because contaminated",
            paired_accession="GCA_003670205.1",
            paired_assembly_status="suppressed",
        ),
    }


def test_parse_summary_status_map_stays_primary_accession_scoped() -> None:
    """Status mapping should not depend on paired-record output order."""

    requested_accessions = ["GCF_000306725.1", "GCA_000306725.1"]
    payload_gcf = (
        '{"accession":"GCF_000306725.1",'
        '"assemblyInfo":{"assemblyStatus":"suppressed",'
        '"pairedAssembly":{"accession":"GCA_000306725.1","status":"current"}}}\n'
    )
    payload_gca = (
        '{"accession":"GCA_000306725.1",'
        '"assemblyInfo":{"assemblyStatus":"current"}}\n'
    )

    parsed_a = parse_summary_status_map(
        payload_gcf + payload_gca,
        requested_accessions,
    )
    parsed_b = parse_summary_status_map(
        payload_gca + payload_gcf,
        requested_accessions,
    )

    assert parsed_a == parsed_b == {
        "GCF_000306725.1": AssemblyStatusInfo(
            assembly_status="suppressed",
            suppression_reason=None,
            paired_accession="GCA_000306725.1",
            paired_assembly_status="current",
        ),
        "GCA_000306725.1": AssemblyStatusInfo(
            assembly_status="current",
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
    }


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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return retryable metadata responses."""

        assert env is not None
        assert NCBI_API_KEY_ENV_VAR not in env
        return next(attempts)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_summary_lookup_with_retries(
        ["GCF_000001.1"],
        COMMAND_TEST_ACCESSION_FILE,
        sleep_func=sleep_calls.append,
    )

    assert result.summary_map == {
        "GCF_000001.1": {"GCF_000001.1", "GCA_000001.1"},
    }
    assert result.status_map == {
        "GCF_000001.1": AssemblyStatusInfo(
            assembly_status=None,
            suppression_reason=None,
            paired_accession=None,
            paired_assembly_status=None,
        ),
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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return repeated metadata lookup failures."""

        assert env is not None
        assert NCBI_API_KEY_ENV_VAR not in env
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
            COMMAND_TEST_ACCESSION_FILE,
            sleep_func=sleep_calls.append,
        )

    assert sleep_calls == [5, 15, 45]


def test_run_summary_lookup_with_retries_fails_fast_on_spawn_error() -> None:
    """Metadata lookup should not retry when the command cannot start."""

    with pytest.raises(
        MetadataLookupError,
        match="metadata lookup command could not start",
    ) as error:
        run_summary_lookup_with_retries(
            ["GCF_000001.1"],
            COMMAND_TEST_ACCESSION_FILE,
            sleep_func=lambda delay: None,
            runner=lambda *args, **kwargs: (_ for _ in ()).throw(
                FileNotFoundError("datasets"),
            ),
        )

    assert len(error.value.failures) == 1
    assert error.value.failures[0].error_type == "metadata_lookup_spawn_error"


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

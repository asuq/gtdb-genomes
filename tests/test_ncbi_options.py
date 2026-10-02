"""Eligibility, argument forwarding, and exclusion reporting contracts."""

from __future__ import annotations

import csv
from dataclasses import replace
import json
import logging
from pathlib import Path
import subprocess
import zipfile

import polars as pl
import pytest

from gtdb_genomes.cli import build_parser, main, parse_args
from gtdb_genomes.download import CommandFailureRecord, RetryableCommandResult
from gtdb_genomes.layout import initialise_run_directories
from gtdb_genomes.metadata import MetadataLookupError, SummaryLookupResult, run_summary_lookup_with_retries
from gtdb_genomes.metadata_summary_parsing import parse_primary_summary_output
from gtdb_genomes.workflow_eligibility import resolve_eligible_plans
from gtdb_genomes.workflow_execution import AccessionPlan, execute_direct_accession_plans
from tests.workflow_contract_helpers import build_cli_args, install_fake_release_resolution, parse_summary_log


LOGGER = logging.getLogger(__name__)


def lookup_result(records: list[dict], requested: tuple[str, ...]) -> SummaryLookupResult:
    parsed = parse_primary_summary_output(
        "\n".join(json.dumps(record) for record in records), requested,
    )
    return SummaryLookupResult(summary_map=parsed.summary_map, status_map=parsed.status_map)


def read_table(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


@pytest.mark.parametrize("include", [
    "all", "genome,rna,cds,gtf,gbff,seq-report", "genome,gff3,protein",
])
def test_extended_include_values(include: str, tmp_path: Path) -> None:
    args = parse_args(build_parser(), ["-t", "g__Example", "-o", str(tmp_path), "--include", include])
    assert args.include == include


@pytest.mark.parametrize("payload", [
    "{}", "[]", "not-json", '{"accession":"invalid"}',
    '{"accession":"GCA_1.1","paired":"GCF_1.1"}',
    '{"accession":"GCF_1.1"}\n{"accession":"GCF_1.1"}',
    '{"accession":"GCF_1.1"}\n{"accession":"GCF_1.2"}',
])
def test_primary_matching_rejects_malformed_alias_and_ambiguous_records(payload: str) -> None:
    with pytest.raises(MetadataLookupError):
        parse_primary_summary_output(payload, ("GCF_1", "GCF_1.1"))


def test_primary_lookup_preserves_version_and_forwards_filters(tmp_path: Path) -> None:
    def runner(command, **kwargs):
        assert command[-1] == "--assembly-level=complete"
        assert kwargs["env"].get("NCBI_API_KEY") is None
        return subprocess.CompletedProcess(command, 0, '{"accession":"GCA_1.3","paired":"GCF_1.2"}\n', "")

    result = run_summary_lookup_with_retries(
        ("GCA_1",), tmp_path / "input.txt", runner=runner,
        primary_only=True, filter_args=("--assembly-level=complete",),
    )
    assert result.summary_map == {"GCA_1": {"GCA_1.3"}}
    assert set(result.status_map) == {"GCA_1.3"}


@pytest.mark.parametrize("fallback_eligible", [False, True])
def test_eligibility_pins_latest_and_checks_fallbacks(
    monkeypatch, tmp_path: Path, fallback_eligible: bool,
) -> None:
    calls = []

    def lookup(accessions, inputfile, **kwargs):
        calls.append((accessions, kwargs.get("filter_args", ())))
        assert inputfile.read_text().splitlines() == list(accessions)
        records = [
            {"accession": "GCA_1.3", "paired": "GCF_1.2"}, {"accession": "GCF_1.2"},
            {"accession": "GCA_2.1"},
        ]
        if kwargs.get("filter_args"):
            records = records[:2] if fallback_eligible else records[:1]
        return lookup_result(records, accessions)

    monkeypatch.setattr("gtdb_genomes.workflow_eligibility.run_summary_lookup_with_retries", lookup)
    args = replace(build_cli_args(tmp_path), ncbi_filters=("--assembly-level=complete",))
    plans = (
        AccessionPlan("GCF_1.2", "GCA_1", "paired_to_gca"),
        AccessionPlan("GCA_2.1", "GCA_2.1", "unchanged_original"),
        AccessionPlan("GCA_3.1", "GCA_3.1", "unchanged_original"),
    )
    eligible, terminal = resolve_eligible_plans(plans, args, LOGGER)
    assert eligible == (replace(plans[0], download_request_accession="GCA_1.3", fallback_allowed=fallback_eligible),)
    assert terminal["GCA_2.1"].download_status == "excluded"
    assert terminal["GCA_3.1"].download_status == "failed"
    assert terminal["GCA_3.1"].failures[0].error_type == "filter_metadata_missing"
    assert calls[1][0] == ("GCA_1.3", "GCF_1.2", "GCA_2.1")


def test_failed_filter_lookup_never_downloads_unfiltered(monkeypatch, tmp_path: Path) -> None:
    def lookup(requests, inputfile, **kwargs):
        if kwargs.get("filter_args"):
            raise MetadataLookupError("NCBI query failed")
        return lookup_result([{"accession": "GCF_1.1"}], requests)

    monkeypatch.setattr("gtdb_genomes.workflow_eligibility.run_summary_lookup_with_retries", lookup)
    args = replace(build_cli_args(tmp_path), ncbi_filters=("--reference=true",))
    with pytest.raises(MetadataLookupError, match="NCBI query failed"):
        resolve_eligible_plans((AccessionPlan("GCF_1.1", "GCF_1.1", "unchanged_original"),), args, LOGGER)


def test_excluded_fallback_does_not_hide_primary_download_failure(monkeypatch, tmp_path: Path) -> None:
    requests = []

    def command_runner(command, **kwargs):
        requests.append(kwargs["attempted_accession"])
        assert "--no-progressbar=true" in command
        failure = CommandFailureRecord("download", 1, 1, "subprocess", "Download failed", "retry_exhausted")
        return RetryableCommandResult(False, "", "Download failed", (failure,))

    monkeypatch.setattr("gtdb_genomes.workflow_execution_direct.run_retryable_command", command_runner)
    args = replace(build_cli_args(tmp_path), ncbi_download_options=("--no-progressbar=true",))
    result = execute_direct_accession_plans(
        (AccessionPlan("GCF_1.1", "GCA_1.1", "paired_to_gca", fallback_allowed=False),),
        args, initialise_run_directories(tmp_path / "output"), LOGGER,
    )
    assert requests == ["GCA_1.1"] * 4
    assert result.executions["GCF_1.1"].download_status == "failed"
    assert result.executions["GCF_1.1"].failures

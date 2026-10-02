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


def test_native_options_preserve_values_and_split_destinations(tmp_path: Path) -> None:
    args = parse_args(build_parser(), [
        "-t", "g__Example", "-o", str(tmp_path), "--assembly-level", "complete,chromosome",
        "--annotated", "--exclude-atypical=false", "--search", "Broad Institute",
        "--search=--filename=untouched.zip", "--chromosomes", "1,2", "--chromosomes", "pEC",
        "--fast-zip-validation", "--no-progressbar", "--include", "all",
        "--api-key", "test-secret",
    ])
    assert args.ncbi_filters == (
        "--assembly-level=complete,chromosome", "--annotated=true",
        "--exclude-atypical=false", "--search=Broad Institute",
        "--search=--filename=untouched.zip",
    )
    assert args.ncbi_download_options == (
        "--chromosomes=1,2", "--chromosomes=pEC", "--fast-zip-validation=true",
        "--no-progressbar=true",
    )
    assert args.include == "all"
    assert args.ncbi_api_key == "test-secret"


@pytest.mark.parametrize("arguments", [
    ["--assembly-source", "genbank"], ["--assembly-version", "all"],
    ["--filename", "override.zip"], ["--inputfile", "override.txt"],
    ["--preview"], ["--dehydrated"], ["--include", "none"],
    ["--include", "protein"], ["--search", ""], ["--assembly-lev", "complete"],
    ["--annotated=maybe"], ["--api-key", "test-secret", "--debug"],
])
def test_invalid_or_incompatible_options_fail(arguments: list[str], tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        parse_args(build_parser(), ["-t", "g__Example", "-o", str(tmp_path), *arguments])
    assert error.value.code == 2


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


def test_verified_bacterial_and_archaeal_assembly_levels(monkeypatch, tmp_path: Path) -> None:
    """Preserve the native NCBI result for GTDB complete and scaffold assemblies."""

    fixture = json.loads((Path(__file__).parent / "fixtures/ncbi_eligibility.json").read_text())
    records = fixture["unfiltered_records"]
    accessions = tuple(record["accession"] for record in records)
    expected = set(fixture["eligible_accessions"])
    assert {lineage.split(";")[0] for lineage in fixture["gtdb_lineages"].values()} == {
        "d__Bacteria", "d__Archaea",
    }

    def lookup(requests, inputfile, **kwargs):
        selected = records
        if kwargs.get("filter_args"):
            selected = [record for record in records if record["accession"] in expected]
        return lookup_result(selected, requests)

    monkeypatch.setattr("gtdb_genomes.workflow_eligibility.run_summary_lookup_with_retries", lookup)
    plans = tuple(AccessionPlan(accession, accession, "unchanged_original") for accession in accessions)
    args = replace(build_cli_args(tmp_path), ncbi_filters=tuple(fixture["criteria"]))
    eligible, terminal = resolve_eligible_plans(plans, args, LOGGER)
    assert {plan.download_request_accession for plan in eligible} == expected
    assert set(terminal) == set(fixture["excluded_accessions"])
    assert all(execution.download_status == "excluded" for execution in terminal.values())


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


@pytest.mark.parametrize("mode,all_excluded,dry_run", [
    ("direct", False, False), ("dehydrate", False, False),
    ("threshold", False, False),
    ("direct", True, False), ("direct", True, True),
])
def test_cli_exclusion_tables_and_download_workload(
    monkeypatch, tmp_path: Path, mode: str, all_excluded: bool, dry_run: bool,
) -> None:
    install_fake_release_resolution(monkeypatch)
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    lineage = "d__Bacteria;g__Example;s__Example one"
    frame = pl.DataFrame({
        "gtdb_accession": ["RS_GCF_1.1", "RS_GCF_2.1"],
        "ncbi_accession": ["GCF_1.1", "GCF_2.1"],
        "lineage": [lineage, lineage], "taxonomy_file": ["fixture.tsv", "fixture.tsv"],
    })
    monkeypatch.setattr("gtdb_genomes.workflow_selection.load_release_taxonomy", lambda _: frame)
    monkeypatch.setattr("gtdb_genomes.workflow_selection.run_supported_preflight", lambda _: None)
    monkeypatch.setattr("gtdb_genomes.provenance.get_command_version", lambda _: "datasets 18.4.0")
    if mode in ("dehydrate", "threshold"):
        monkeypatch.setattr(
            "gtdb_genomes.download.DEHYDRATE_ACCESSION_THRESHOLD",
            1 if mode == "dehydrate" else 2,
        )

    def lookup(accessions, inputfile, **kwargs):
        records = [{"accession": "GCF_1.1"}, {"accession": "GCF_2.1"}]
        if kwargs.get("filter_args"):
            records = [] if all_excluded else records[:1]
        return lookup_result(records, accessions)

    monkeypatch.setattr("gtdb_genomes.workflow_eligibility.run_summary_lookup_with_retries", lookup)
    commands = []

    def command_runner(command, **kwargs):
        commands.append(command)
        assert not all_excluded and not dry_run
        if command[1] == "download":
            assert "--chromosomes=all" in command
            accessions = Path(command[command.index("--inputfile") + 1]).read_text().splitlines()
            assert accessions == ["GCF_1.1"]
            archive = Path(command[command.index("--filename") + 1])
            with zipfile.ZipFile(archive, "w") as handle:
                handle.writestr("ncbi_dataset/data/GCF_1.1/genomic.fna", ">fixture\nACGT\n")
        return RetryableCommandResult(True, "", "", ())

    for module in ("workflow_execution_direct", "workflow_execution_dehydrate"):
        monkeypatch.setattr(f"gtdb_genomes.{module}.run_retryable_command", command_runner)
    output = tmp_path / "output"
    arguments = [
        "-t", "g__Example", "-o", str(output), "--assembly-level", "complete",
        "--chromosomes", "all", "--include", "all",
    ]
    if dry_run:
        arguments.append("--dry-run")
    assert main(arguments) == 0
    if dry_run:
        assert not output.exists()
        assert not commands
        return
    rows = read_table(output / "accession_map.tsv")
    excluded = [row for row in rows if row["download_status"] == "excluded"]
    assert len(excluded) == (2 if all_excluded else 1)
    assert all(not row["final_accession"] and not row["output_relpaths"] for row in excluded)
    assert all("--assembly-level=complete" in row["exclusion_reason"] for row in excluded)
    assert read_table(output / "download_failures.tsv") == []
    per_taxon = read_table(output / "taxa/g__Example/taxon_accessions.tsv")
    assert sum(row["download_status"] == "excluded" for row in per_taxon) == len(excluded)
    summary = parse_summary_log(output / "run_summary.log")
    assert summary["excluded_accessions"] == str(len(excluded))
    assert summary["failed_accessions"] == "0"
    assert json.loads(summary["ncbi_options"]) == ["--assembly-level=complete", "--chromosomes=all"]
    assert read_table(output / "taxon_summary.tsv")[0]["excluded_accessions"] == str(len(excluded))
    if all_excluded:
        assert not commands
    else:
        assert (output / "taxa/g__Example/GCF_1.1/genomic.fna").is_file()
        assert ("--dehydrated" in commands[0]) == (mode == "dehydrate")

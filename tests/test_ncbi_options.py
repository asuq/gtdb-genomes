"""Native NCBI option and metadata contracts."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from gtdb_genomes.cli import build_parser, parse_args
from gtdb_genomes.metadata import MetadataLookupError, run_summary_lookup_with_retries
from gtdb_genomes.metadata_summary_parsing import parse_primary_summary_output


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

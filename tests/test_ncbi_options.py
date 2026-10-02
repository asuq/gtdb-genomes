"""Native NCBI option and metadata contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import build_parser, parse_args


@pytest.mark.parametrize("include", [
    "all", "genome,rna,cds,gtf,gbff,seq-report", "genome,gff3,protein",
])
def test_extended_include_values(include: str, tmp_path: Path) -> None:
    args = parse_args(build_parser(), ["-t", "g__Example", "-o", str(tmp_path), "--include", include])
    assert args.include == include

"""Shared helpers for workflow contract tests."""

from __future__ import annotations

import io
import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import CliArgs
from gtdb_genomes.release_resolver import ReleaseResolution


def build_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a minimal taxonomy frame for workflow tests."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1"],
            "lineage": [lineage],
            "ncbi_accession": ["GCF_000001.1"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )


def build_mixed_uba_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with one supported and one legacy UBA accession."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_000001.1", "UBA11131"],
            "lineage": [lineage, lineage],
            "ncbi_accession": ["GCF_000001.1", "UBA11131"],
            "taxonomy_file": [
                "bac120_taxonomy_r80.tsv",
                "bac120_taxonomy_r80.tsv",
            ],
        },
    )


def build_multi_accession_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame with repeated and distinct supported accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "RS_GCF_000001.1_copy",
                "RS_GCF_000002.1",
            ],
            "lineage": [lineage, lineage, lineage],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCF_000001.1",
                "GCF_000002.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
                "bac120_taxonomy_r202.tsv",
            ],
        },
    )


def build_uba_only_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame containing only unsupported UBA accessions."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["UBA11131"],
            "lineage": [lineage],
            "ncbi_accession": ["UBA11131"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv"],
        },
    )


def build_shared_preferred_taxonomy_frame(lineage: str) -> pl.DataFrame:
    """Build a taxonomy frame whose rows share one preferred accession."""

    return pl.DataFrame(
        {
            "gtdb_accession": ["RS_GCF_001881595.2", "GB_GCA_001881595.3"],
            "lineage": [lineage, lineage],
            "ncbi_accession": ["GCF_001881595.2", "GCA_001881595.3"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv", "bac120_taxonomy_r80.tsv"],
        },
    )


def install_capture_logger(
    monkeypatch: pytest.MonkeyPatch,
) -> io.StringIO:
    """Patch workflow logging to capture warning text for assertions."""

    stream = io.StringIO()

    def fake_configure_logging(
        debug: bool = False,
        dry_run: bool = False,
        output_root: Path | None = None,
    ) -> tuple[logging.Logger, Path | None]:
        """Return a predictable test logger backed by one string buffer."""

        del dry_run, output_root
        logger = logging.getLogger(f"test-workflow-{id(stream)}")
        logger.handlers.clear()
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
        return logger, None

    def fake_close_logger(logger: logging.Logger) -> None:
        """Flush and detach handlers without closing the shared string buffer."""

        for handler in tuple(logger.handlers):
            handler.flush()
            logger.removeHandler(handler)

    monkeypatch.setattr(
        "gtdb_genomes.workflow.configure_logging",
        fake_configure_logging,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow.close_logger",
        fake_close_logger,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.close_logger",
        fake_close_logger,
    )
    return stream


def resolve_requested_release(requested_release: str) -> str:
    """Normalise one requested release token for workflow contract tests."""

    release = requested_release.strip()
    if release == "latest":
        return "226.0"
    if release.startswith("release") and "/" in release:
        _, _, resolved_release = release.partition("/")
        return resolved_release
    if "." in release:
        return release
    return f"{release}.0"


def install_fake_release_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Patch workflow selection to use a synthetic validated release resolution."""

    def fake_resolve_and_validate_release(requested_release: str) -> ReleaseResolution:
        """Return one synthetic release resolution without touching checkout data."""

        resolved_release = resolve_requested_release(requested_release)
        return ReleaseResolution(
            requested_release=requested_release,
            resolved_release=resolved_release,
            bacterial_taxonomy=Path("/tmp") / resolved_release / "taxonomy.tsv.gz",
            archaeal_taxonomy=None,
            release_manifest_path=Path("/tmp") / "releases.tsv",
            release_manifest_sha256="0" * 64,
            bacterial_taxonomy_sha256="1" * 64,
            archaeal_taxonomy_sha256=None,
            bacterial_taxonomy_rows=1,
            archaeal_taxonomy_rows=None,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.resolve_and_validate_release",
        fake_resolve_and_validate_release,
    )


def parse_tsv(path: Path) -> tuple[list[str], list[list[str]]]:
    """Return the header and rows from a TSV output file."""

    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[1:]]
    return header, rows


def build_cli_args(
    output_dir: Path,
    *,
    prefer_genbank: bool = True,
) -> CliArgs:
    """Build a minimal CLI argument object for workflow unit tests."""

    return CliArgs(
        gtdb_release="80",
        gtdb_taxa=("s__Escherichia coli",),
        outdir=output_dir,
        prefer_genbank=prefer_genbank,
        version_latest=False,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )

"""Command-line interface for gtdb-genomes."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from gtdb_genomes.download import validate_include_value
from gtdb_genomes.preflight import PreflightError
from gtdb_genomes.taxon_normalisation import (
    is_complete_requested_taxon,
    normalise_requested_taxon,
)

DEFAULT_THREADS = 8


@dataclass(slots=True)
class CliArgs:
    """Normalised command-line arguments for gtdb-genomes."""

    gtdb_release: str
    gtdb_taxa: tuple[str, ...]
    outdir: Path
    prefer_genbank: bool
    version_latest: bool
    threads: int
    ncbi_api_key: str | None
    include: str
    debug: bool
    keep_temp: bool
    dry_run: bool


def normalise_release(parser: argparse.ArgumentParser, release: str) -> str:
    """Trim and validate the release argument."""

    value = release.strip()
    if not value:
        parser.error("argument --gtdb-release: value must not be empty")
    return value


def normalise_taxa(
    parser: argparse.ArgumentParser,
    taxa: Sequence[Sequence[str]],
) -> tuple[str, ...]:
    """Trim, validate, flatten, and deduplicate requested taxa."""

    ordered_taxa: list[str] = []
    seen: set[str] = set()
    for taxon_group in taxa:
        for raw_taxon in taxon_group:
            taxon = normalise_requested_taxon(raw_taxon)
            if not taxon:
                parser.error("argument --gtdb-taxon: value must not be empty")
            if not is_complete_requested_taxon(taxon):
                parser.error(
                    "argument --gtdb-taxon: each value must be one complete GTDB "
                    "taxon token with a recognised rank prefix",
                )
            if taxon in seen:
                continue
            seen.add(taxon)
            ordered_taxa.append(taxon)
    return tuple(ordered_taxa)


def normalise_include(parser: argparse.ArgumentParser, include: str) -> str:
    """Trim and validate the include argument."""

    try:
        return validate_include_value(include)
    except ValueError as error:
        parser.error(str(error))


def validate_output_path(parser: argparse.ArgumentParser, output: str) -> Path:
    """Validate the output path without creating directories."""

    path = Path(output).expanduser()
    try:
        path_exists = path.exists()
        if path_exists:
            if not path.is_dir():
                parser.error(
                    "argument --outdir: path must not be an existing file",
                )
            if any(path.iterdir()):
                parser.error(
                    "argument --outdir: directory must be empty if it already exists",
                )
    except OSError as error:
        parser.error(
            f"argument --outdir: could not inspect path {path}: {error}",
        )
    return path


def parse_args(
    parser: argparse.ArgumentParser,
    argv: Sequence[str] | None = None,
) -> CliArgs:
    """Parse, normalise, and validate command-line arguments."""

    namespace = parser.parse_args(argv)
    if namespace.threads <= 0:
        parser.error("argument --threads: value must be a positive integer")
    if namespace.version_latest and not namespace.prefer_genbank:
        parser.error("argument --version-latest: requires --prefer-genbank")
    return CliArgs(
        gtdb_release=normalise_release(parser, namespace.gtdb_release),
        gtdb_taxa=normalise_taxa(parser, namespace.gtdb_taxon),
        outdir=validate_output_path(parser, namespace.outdir),
        prefer_genbank=namespace.prefer_genbank,
        version_latest=namespace.version_latest,
        threads=namespace.threads,
        ncbi_api_key=namespace.ncbi_api_key,
        include=normalise_include(parser, namespace.include),
        debug=namespace.debug,
        keep_temp=namespace.keep_temp,
        dry_run=namespace.dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the base argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="gtdb-genomes",
        description="Download NCBI genomes by GTDB taxon and GTDB release.",
    )
    parser.add_argument(
        "--gtdb-release",
        default="latest",
        help="GTDB release alias or bundled release identifier. Default: latest.",
    )
    parser.add_argument(
        "--gtdb-taxon",
        action="append",
        nargs="+",
        required=True,
        help=(
            "Exact GTDB taxon token. Accept one or more values per use and "
            "repeat as needed. "
            "Quote species taxa with spaces, for example "
            "\"s__Altiarchaeum hamiconexum\"."
        ),
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Output directory for the run.",
    )
    parser.add_argument(
        "--prefer-genbank",
        action="store_true",
        help=(
            "Prefer paired GenBank accessions and, by default, keep the exact "
            "selected versioned accession."
        ),
    )
    parser.add_argument(
        "--version-latest",
        action="store_true",
        help=(
            "Request the latest available revision in the selected accession "
            "family; requires --prefer-genbank."
        ),
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help=(
            "Choose the worker count used by compatible workflow steps; "
            "direct downloads remain serial. Default: 8."
        ),
    )
    parser.add_argument(
        "--ncbi-api-key",
        help="NCBI API key used only for datasets commands. The tool never stores or logs it.",
    )
    parser.add_argument(
        "--include",
        default="genome",
        help="Comma-separated datasets include values; must contain genome.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep intermediate working files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve inputs without downloading genome payloads.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the gtdb-genomes command-line interface."""
    parser = build_parser()
    args = parse_args(parser, argv)
    from gtdb_genomes.workflow import run_workflow

    try:
        return run_workflow(args)
    except PreflightError as error:
        print(f"gtdb-genomes: error: {error}", file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

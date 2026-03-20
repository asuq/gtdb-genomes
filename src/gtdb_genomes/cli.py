"""Command-line interface for gtdb-genomes."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from gtdb_genomes.download import validate_include_value
from gtdb_genomes.preflight import PreflightError
from gtdb_genomes.taxon_normalisation import normalise_requested_taxon


@dataclass(slots=True)
class CliArgs:
    """Normalised command-line arguments for gtdb-genomes."""

    gtdb_release: str
    gtdb_taxa: tuple[str, ...]
    outdir: Path
    prefer_genbank: bool
    version_fixed: bool
    download_method: str
    threads: int
    ncbi_api_key: str | None
    include: str
    debug: bool
    keep_temp: bool
    dry_run: bool


def get_default_threads() -> int:
    """Return the default worker count for the local machine."""

    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    cpu_count = os.cpu_count()
    if cpu_count is None:
        return 1
    return cpu_count


def normalise_release(parser: argparse.ArgumentParser, release: str) -> str:
    """Trim and validate the release argument."""

    value = release.strip()
    if not value:
        parser.error("argument --gtdb-release: value must not be empty")
    return value


def normalise_taxa(
    parser: argparse.ArgumentParser,
    taxa: Sequence[str],
) -> tuple[str, ...]:
    """Trim, validate, and deduplicate requested taxa."""

    ordered_taxa: list[str] = []
    seen: set[str] = set()
    for raw_taxon in taxa:
        taxon = normalise_requested_taxon(raw_taxon)
        if not taxon:
            parser.error("argument --gtdb-taxon: value must not be empty")
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
    if path.exists():
        if not path.is_dir():
            parser.error(
                "argument --outdir: path must not be an existing file",
            )
        if any(path.iterdir()):
            parser.error(
                "argument --outdir: directory must be empty if it already exists",
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
    if namespace.version_fixed and not namespace.prefer_genbank:
        parser.error("argument --version-fixed: requires --prefer-genbank")
    return CliArgs(
        gtdb_release=normalise_release(parser, namespace.gtdb_release),
        gtdb_taxa=normalise_taxa(parser, namespace.gtdb_taxon),
        outdir=validate_output_path(parser, namespace.outdir),
        prefer_genbank=namespace.prefer_genbank,
        version_fixed=namespace.version_fixed,
        download_method=namespace.download_method,
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
        required=True,
        help="GTDB release alias or bundled release identifier.",
    )
    parser.add_argument(
        "--gtdb-taxon",
        action="append",
        required=True,
        help=(
            "Exact GTDB taxon token. Repeat to request multiple taxa. "
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
            "Prefer paired GenBank accessions and, by default, request the latest "
            "available revision in the chosen family, which may differ from the "
            "RefSeq version."
        ),
    )
    parser.add_argument(
        "--version-fixed",
        action="store_true",
        help="Pin the exact selected version; requires --prefer-genbank.",
    )
    parser.add_argument(
        "--download-method",
        choices=("auto", "direct", "dehydrate"),
        default="auto",
        help="Download mode selection strategy.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=get_default_threads(),
        help="Worker count to use; defaults to all available CPU threads.",
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

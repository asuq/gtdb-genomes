"""Command-line interface for gtdb-genomes."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from gtdb_genomes.download import validate_include_value
from gtdb_genomes.layout import (
    build_leftover_run_abort_message,
    find_leftover_run_artefacts,
)
from gtdb_genomes.preflight import PreflightError
from gtdb_genomes.subprocess_utils import NCBI_API_KEY_ENV_VAR
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


def normalise_optional_api_key(api_key: str | None) -> str | None:
    """Trim one optional API key value and normalise blank inputs to `None`."""

    if api_key is None:
        return None
    value = api_key.strip()
    if not value:
        return None
    return value


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
                    "taxon with a recognised rank prefix",
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


def resolve_output_path(output: str | None) -> Path:
    """Resolve one optional output path to the effective filesystem location."""

    if output is None:
        return Path.cwd()
    return Path(output).expanduser()


def validate_output_path(
    parser: argparse.ArgumentParser,
    output: str | None,
) -> Path:
    """Validate the output path without creating directories."""

    path = resolve_output_path(output)
    try:
        if path.exists():
            if not path.is_dir():
                parser.error(
                    "argument --outdir: path must not be an existing file",
                )
            leftover_artefacts = find_leftover_run_artefacts(path)
            if leftover_artefacts:
                parser.error(
                    "argument --outdir: "
                    + build_leftover_run_abort_message(
                        path,
                        leftover_artefacts,
                    ),
                )
    except OSError as error:
        parser.error(
            f"argument --outdir: could not inspect path {path}: {error}",
        )
    return path


def resolve_effective_ncbi_api_key(
    explicit_api_key: str | None,
    environment: Mapping[str, str] | None = None,
) -> str | None:
    """Resolve the effective NCBI API key from CLI input or the environment."""

    normalised_explicit_api_key = normalise_optional_api_key(explicit_api_key)
    if normalised_explicit_api_key is not None:
        return normalised_explicit_api_key
    source_environment = os.environ if environment is None else environment
    return normalise_optional_api_key(source_environment.get(NCBI_API_KEY_ENV_VAR))


def parse_args(
    parser: argparse.ArgumentParser,
    argv: Sequence[str] | None = None,
) -> CliArgs:
    """Parse, normalise, and validate command-line arguments."""

    effective_argv = tuple(sys.argv[1:] if argv is None else argv)
    if not effective_argv:
        parser.parse_args(["--help"])
    namespace = parser.parse_args(effective_argv)
    effective_ncbi_api_key = resolve_effective_ncbi_api_key(namespace.ncbi_api_key)
    if namespace.threads <= 0:
        parser.error("argument --threads: value must be a positive integer")
    if namespace.version_latest and not namespace.prefer_genbank:
        parser.error("argument --version-latest: requires --prefer-genbank")
    if namespace.debug and effective_ncbi_api_key:
        parser.error(
            "argument --debug: cannot be used while an NCBI API key is active "
            "because upstream datasets debug output may expose the API key",
        )
    return CliArgs(
        gtdb_release=normalise_release(parser, namespace.gtdb_release),
        gtdb_taxa=normalise_taxa(parser, namespace.gtdb_taxon),
        outdir=validate_output_path(parser, namespace.outdir),
        prefer_genbank=namespace.prefer_genbank,
        version_latest=namespace.version_latest,
        threads=namespace.threads,
        ncbi_api_key=effective_ncbi_api_key,
        include=normalise_include(parser, namespace.include),
        debug=namespace.debug,
        keep_temp=namespace.keep_temp,
        dry_run=namespace.dry_run,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the base argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        add_help=False,
        prog="gtdb-genomes",
        description="Download NCBI genomes by GTDB taxon and GTDB release",
        usage=(
            "gtdb-genomes -t GTDB_TAXON [GTDB_TAXON ...] "
            "[-o OUTDIR] [-h] [-r GTDB_RELEASE] [--prefer-genbank] "
            "[--version-latest] [-j THREADS] "
            "[--ncbi-api-key NCBI_API_KEY] [--include INCLUDE] "
            "[--debug] [--keep-tmp] [-d]"
        ),
    )
    mandatory_options = parser.add_argument_group("mandatory options")
    optional_options = parser.add_argument_group("optional options")
    optional_options.add_argument(
        "-h",
        "--help",
        action="help",
        help="show this help message and exit",
    )
    optional_options.add_argument(
        "-r",
        "--gtdb-release",
        default="latest",
        help="GTDB release alias or included release identifier; default: latest",
    )
    mandatory_options.add_argument(
        "-t",
        "--gtdb-taxon",
        action="append",
        nargs="+",
        required=True,
        help=(
            "Exact GTDB taxon. You can give one or more values after the flag "
            "and repeat it as needed. Quote species names with spaces, for "
            "example \"s__Altiarchaeum hamiconexum\""
        ),
    )
    optional_options.add_argument(
        "-o",
        "--outdir",
        help="Output directory for the run; default: current working directory",
    )
    optional_options.add_argument(
        "--prefer-genbank",
        action="store_true",
        help=(
            "Prefer paired GenBank accessions discovered from current NCBI "
            "metadata and, by default, keep the exact selected versioned "
            "accession"
        ),
    )
    optional_options.add_argument(
        "--version-latest",
        action="store_true",
        help=(
            "Request the latest available revision in the selected paired "
            "GenBank family when explicit pairing is available, otherwise in "
            "the selected accession family from current NCBI metadata; "
            "requires --prefer-genbank"
        ),
    )
    optional_options.add_argument(
        "-j",
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help=(
            "Choose the worker count used by compatible workflow steps; "
            "direct downloads remain serial; default: 8"
        ),
    )
    optional_options.add_argument(
        "--ncbi-api-key",
        help=(
            "NCBI API key used only for datasets commands; overrides "
            f"{NCBI_API_KEY_ENV_VAR} from the environment; the tool does not "
            "write it to its own logs or manifests"
        ),
    )
    optional_options.add_argument(
        "--include",
        default="genome",
        help="Comma-separated datasets include values; must contain genome or all",
    )
    optional_options.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Enable debug logging; cannot be used while an NCBI API key is "
            "active"
        ),
    )
    optional_options.add_argument(
        "--keep-tmp",
        dest="keep_temp",
        action="store_true",
        help="Keep intermediate working files",
    )
    optional_options.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        help=(
            "Resolve inputs without downloading genome payloads"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the gtdb-genomes command-line interface."""

    try:
        parser = build_parser()
        args = parse_args(parser, argv)
        from gtdb_genomes.workflow import run_workflow
        from gtdb_genomes.logging_utils import redact_text

        return run_workflow(args)
    except KeyboardInterrupt:
        print("gtdb-genomes: error: interrupted by user", file=sys.stderr)
        return 130
    except PreflightError as error:
        print(f"gtdb-genomes: error: {error}", file=sys.stderr)
        return 5
    except Exception as error:  # pragma: no cover - last-resort guard
        print(
            "gtdb-genomes: error: unexpected internal failure: "
            f"{redact_text(str(error), (args.ncbi_api_key,))}",
            file=sys.stderr,
        )
        return 9


if __name__ == "__main__":
    sys.exit(main())

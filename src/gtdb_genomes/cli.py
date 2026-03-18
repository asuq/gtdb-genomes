"""Command-line interface for gtdb-genomes."""

from __future__ import annotations

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Build the base argument parser for the CLI."""
    return argparse.ArgumentParser(
        prog="gtdb-genomes",
        description="Download NCBI genomes by GTDB taxon and GTDB release.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the gtdb-genomes command-line interface."""
    parser = build_parser()
    parser.parse_args(argv)
    return 0

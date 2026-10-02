"""Native CLI options forwarded to NCBI without duplicating its filter rules."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class NcbiOption:
    """One compatible upstream option and the command that consumes it."""

    name: str
    destination: Literal["filter", "download"]
    kind: Literal["value", "bool", "repeat"]
    help: str


NCBI_OPTIONS = (
    NcbiOption("assembly-level", "filter", "value", "Assembly levels, comma-separated: complete, chromosome, scaffold, contig"),
    NcbiOption("annotated", "filter", "bool", "Limit to annotated genomes"),
    NcbiOption("exclude-atypical", "filter", "bool", "Exclude atypical assemblies"),
    NcbiOption("exclude-multi-isolate", "filter", "bool", "Exclude assemblies from multi-isolate projects"),
    NcbiOption("from-type", "filter", "bool", "Limit to assemblies from type material"),
    NcbiOption("mag", "filter", "value", "Metagenome assembled genomes: all, only, or exclude"),
    NcbiOption("reference", "filter", "bool", "Limit to reference genomes"),
    NcbiOption("released-after", "filter", "value", "Limit to genomes released on or after this date"),
    NcbiOption("released-before", "filter", "value", "Limit to genomes released on or before this date"),
    NcbiOption("search", "filter", "repeat", "Search NCBI genome metadata; may be repeated"),
    NcbiOption("chromosomes", "download", "repeat", "Chromosomes to download, comma-separated, or all"),
    NcbiOption("fast-zip-validation", "download", "bool", "Skip upstream ZIP checksum validation"),
    NcbiOption("no-progressbar", "download", "bool", "Hide the upstream download progress bar"),
)


def add_ncbi_options(parser: argparse.ArgumentParser) -> None:
    """Expose compatible options as native flags using one registry."""

    group = parser.add_argument_group("NCBI genome options")
    for option in NCBI_OPTIONS:
        group.add_argument(
            f"--{option.name}",
            default=None,
            help=option.help,
            action="append" if option.kind == "repeat" else "store",
            nargs="?" if option.kind == "bool" else None,
            const="true" if option.kind == "bool" else None,
            choices=("true", "false") if option.kind == "bool" else None,
        )


def collect_ncbi_options(
    parser: argparse.ArgumentParser,
    namespace: argparse.Namespace,
    destination: Literal["filter", "download"],
) -> tuple[str, ...]:
    """Build deterministic argv tokens while preserving repeated values."""

    arguments: list[str] = []
    for option in NCBI_OPTIONS:
        if option.destination != destination:
            continue
        value = getattr(namespace, option.name.replace("-", "_"))
        if value is None:
            continue
        values = value if option.kind == "repeat" else [value]
        for item in values:
            if not item.strip():
                parser.error(f"argument --{option.name}: value must not be empty")
            # An equals token preserves values starting with '-' and needs no shell.
            arguments.append(f"--{option.name}={item}")
    return tuple(arguments)

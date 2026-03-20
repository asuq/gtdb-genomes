"""Requested GTDB taxon normalisation helpers."""

from __future__ import annotations

from collections.abc import Sequence


def normalise_requested_taxon(requested_taxon: str) -> str:
    """Trim only surrounding whitespace from one requested GTDB taxon."""

    return requested_taxon.strip()


def normalise_requested_taxa(requested_taxa: Sequence[str]) -> tuple[str, ...]:
    """Normalise requested GTDB taxa while preserving order and duplicates."""

    return tuple(
        normalise_requested_taxon(requested_taxon)
        for requested_taxon in requested_taxa
    )

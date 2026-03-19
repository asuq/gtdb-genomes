"""Taxon matching and accession selection."""

from __future__ import annotations

from collections.abc import Sequence
import hashlib
import re

import polars as pl


UNSAFE_TAXON_CHARACTER_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
EXCESS_UNDERSCORE_PATTERN = re.compile(r"_{3,}")


def add_lineage_tokens(frame: pl.DataFrame) -> pl.DataFrame:
    """Add a split-token lineage column for descendant membership checks."""

    return frame.with_columns(
        pl.col("lineage").str.split(";").alias("lineage_tokens"),
    )


def empty_selection_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return an empty selection frame with the selection columns attached."""

    return frame.head(0).drop("lineage_tokens").with_columns(
        pl.lit("").alias("requested_taxon"),
    )


def select_taxa(
    frame: pl.DataFrame,
    requested_taxa: Sequence[str],
) -> pl.DataFrame:
    """Select taxonomy rows whose lineage contains any requested taxon."""

    tokenised = add_lineage_tokens(frame)
    selections: list[pl.DataFrame] = []
    for requested_taxon in requested_taxa:
        selected = tokenised.filter(
            pl.col("lineage_tokens").list.contains(requested_taxon),
        ).with_columns(
            pl.lit(requested_taxon).alias("requested_taxon"),
        )
        selections.append(selected.drop("lineage_tokens"))
    if not selections:
        return empty_selection_frame(tokenised)
    return pl.concat(selections, how="vertical")


def build_base_taxon_slug(requested_taxon: str) -> str:
    """Build a filesystem-safe slug while preserving GTDB rank markers."""

    slug = UNSAFE_TAXON_CHARACTER_PATTERN.sub("_", requested_taxon.strip())
    slug = EXCESS_UNDERSCORE_PATTERN.sub("_", slug)
    return slug or "_"


def build_taxon_slug_map(requested_taxa: Sequence[str]) -> dict[str, str]:
    """Build deterministic taxon slugs with collision handling."""

    base_slugs = {
        requested_taxon: build_base_taxon_slug(requested_taxon)
        for requested_taxon in requested_taxa
    }
    slug_counts: dict[str, int] = {}
    for slug in base_slugs.values():
        slug_counts[slug] = slug_counts.get(slug, 0) + 1

    slug_map: dict[str, str] = {}
    for requested_taxon, slug in base_slugs.items():
        if slug_counts[slug] == 1:
            slug_map[requested_taxon] = slug
            continue
        slug_hash = hashlib.sha1(requested_taxon.encode("ascii")).hexdigest()[:8]
        slug_map[requested_taxon] = f"{slug}__{slug_hash}"
    return slug_map


def attach_taxon_slugs(
    selection_frame: pl.DataFrame,
    requested_taxa: Sequence[str],
) -> pl.DataFrame:
    """Attach the deterministic taxon slug for each selected row."""

    slug_map = build_taxon_slug_map(requested_taxa)
    return selection_frame.with_columns(
        pl.col("requested_taxon").replace_strict(slug_map).alias("taxon_slug"),
    )

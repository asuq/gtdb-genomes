"""Taxon matching and accession selection."""

from __future__ import annotations

from collections.abc import Sequence
import hashlib
import re

import polars as pl


UNSAFE_TAXON_CHARACTER_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
EXCESS_UNDERSCORE_PATTERN = re.compile(r"_{3,}")


def empty_selection_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return an empty selection frame with the selection columns attached."""

    return frame.head(0).with_columns(
        pl.lit("").alias("requested_taxon"),
    )


def build_lineage_token_expression() -> pl.Expr:
    """Return the exact-match lineage token expression for GTDB selection."""

    return (
        pl.col("lineage")
        .str.split(";")
        .list.eval(pl.element().str.strip_chars())
    )


def normalise_requested_taxon(requested_taxon: str) -> str:
    """Trim only surrounding whitespace from one requested GTDB taxon."""

    return requested_taxon.strip()


def select_taxa(
    frame: pl.DataFrame,
    requested_taxa: Sequence[str],
) -> pl.DataFrame:
    """Select rows whose GTDB lineage contains an exact requested token."""

    if not requested_taxa:
        return empty_selection_frame(frame)

    tokenised_frame = frame.with_row_index("_row_order").with_columns(
        build_lineage_token_expression().alias("_lineage_tokens"),
    )

    matched_frames: list[pl.DataFrame] = []
    for requested_order, raw_requested_taxon in enumerate(requested_taxa):
        requested_taxon = normalise_requested_taxon(raw_requested_taxon)
        if not requested_taxon:
            continue
        matched_rows = tokenised_frame.filter(
            pl.col("_lineage_tokens").list.contains(requested_taxon),
        )
        if matched_rows.is_empty():
            continue
        matched_frames.append(
            matched_rows.with_columns(
                pl.lit(requested_taxon).alias("requested_taxon"),
                pl.lit(requested_order).alias("_requested_order"),
            ),
        )

    if not matched_frames:
        return empty_selection_frame(frame)

    return (
        pl.concat(matched_frames, how="vertical")
        .sort(["_requested_order", "_row_order"])
        .drop("_requested_order", "_row_order", "_lineage_tokens")
        .select([*frame.columns, "requested_taxon"])
    )


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

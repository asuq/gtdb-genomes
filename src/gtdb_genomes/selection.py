"""Taxon matching and accession selection."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl


def add_lineage_tokens(frame: pl.DataFrame) -> pl.DataFrame:
    """Add a split-token lineage column for descendant membership checks."""

    return frame.with_columns(
        pl.col("lineage").str.split(";").alias("lineage_tokens"),
    )


def empty_selection_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Return an empty selection frame with the selection columns attached."""

    return frame.head(0).with_columns(
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
        selections.append(selected)
    if not selections:
        return empty_selection_frame(tokenised)
    return pl.concat(selections, how="vertical")


def get_unique_accessions(selection_frame: pl.DataFrame) -> pl.DataFrame:
    """Return the deduplicated accession set for downstream planning."""

    if selection_frame.is_empty():
        return selection_frame.select("gtdb_accession", "ncbi_accession")
    return selection_frame.select("gtdb_accession", "ncbi_accession").unique(
        subset=["gtdb_accession"],
        keep="first",
        maintain_order=True,
    )

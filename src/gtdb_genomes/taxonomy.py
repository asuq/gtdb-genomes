"""GTDB taxonomy table loading and normalisation."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from gtdb_genomes.release_resolver import ReleaseResolution


TAXONOMY_COLUMNS = ["gtdb_accession", "lineage"]


def get_logical_taxonomy_filename(path: Path) -> str:
    """Return a stable taxonomy filename for manifests and output tables."""

    if path.name.endswith(".gz"):
        return path.name[:-3]
    return path.name


def load_taxonomy_table(path: Path) -> pl.DataFrame:
    """Load one bundled GTDB taxonomy table."""

    frame = pl.read_csv(
        path,
        separator="\t",
        has_header=False,
        new_columns=TAXONOMY_COLUMNS,
    )
    accession_column = pl.col("gtdb_accession")
    return frame.with_columns(
        pl.when(
            accession_column.str.starts_with("RS_")
            | accession_column.str.starts_with("GB_"),
        ).then(
            accession_column.str.slice(3),
        ).otherwise(
            accession_column,
        ).alias("ncbi_accession"),
        pl.lit(get_logical_taxonomy_filename(path)).alias("taxonomy_file"),
    )


def load_release_taxonomy(resolution: ReleaseResolution) -> pl.DataFrame:
    """Load and combine the bundled taxonomy tables for a resolved release."""

    frames: list[pl.DataFrame] = []
    if resolution.bacterial_taxonomy is not None:
        frames.append(load_taxonomy_table(resolution.bacterial_taxonomy))
    if resolution.archaeal_taxonomy is not None:
        frames.append(load_taxonomy_table(resolution.archaeal_taxonomy))
    if not frames:
        return pl.DataFrame(
            schema={
                "gtdb_accession": pl.String,
                "lineage": pl.String,
                "ncbi_accession": pl.String,
                "taxonomy_file": pl.String,
            },
        )
    return pl.concat(frames, how="vertical")

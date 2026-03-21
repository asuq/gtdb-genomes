"""Fixture-driven tests for real GTDB taxon export membership."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.selection import select_taxa


FIXTURE_DIRECTORY = Path(__file__).resolve().parent / "fixtures" / "gtdb_taxon_exports"
EXPECTED_FIXTURE_ROW_COUNTS = {
    "g__Frigididesulfovibrio": 24,
    "o__Altiarchaeales": 19,
    "s__Altiarchaeum hamiconexum": 9,
    "s__Frigididesulfovibrio sp031556355": 1,
}


@dataclass(frozen=True, slots=True)
class TaxonFixtureRow:
    """One expected GTDB export row for a requested taxon."""

    accession: str
    lineage: str


def get_fixture_paths() -> tuple[Path, ...]:
    """Return the GTDB export fixture files in a deterministic order."""

    return tuple(sorted(FIXTURE_DIRECTORY.glob("*.csv")))


def normalise_lineage(lineage: str) -> str:
    """Normalise GTDB lineage spacing from exported fixture rows."""

    return lineage.replace("; ", ";").strip()


def get_lineage_tokens(lineage: str) -> tuple[str, ...]:
    """Split one GTDB lineage into stripped taxonomy tokens."""

    return tuple(token.strip() for token in lineage.split(";"))


def load_fixture_rows(fixture_path: Path) -> tuple[TaxonFixtureRow, ...]:
    """Load one GTDB export fixture file."""

    with fixture_path.open("r", encoding="ascii", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(
            TaxonFixtureRow(
                accession=row["accession"].strip(),
                lineage=normalise_lineage(row["gtdb_taxonomy"]),
            )
            for row in reader
        )


@lru_cache(maxsize=1)
def get_release_226_taxonomy() -> pl.DataFrame:
    """Build a fixture-backed taxonomy frame shaped like release 226.0."""

    rows: list[dict[str, str]] = []
    for fixture_path in get_fixture_paths():
        for row in load_fixture_rows(fixture_path):
            rows.append(
                {
                    "gtdb_accession": row.accession,
                    "lineage": row.lineage,
                    "ncbi_accession": row.accession,
                    "taxonomy_file": "fixture_release_226.tsv",
                },
            )
    rows.append(
        {
            "gtdb_accession": "GCF_900143255.1",
            "lineage": (
                "d__Bacteria;p__Desulfobacterota;c__Desulfovibrionia;"
                "o__Desulfovibrionales;f__Desulfovibrionaceae;"
                "g__Frigididesulfovibrio_A;s__Frigididesulfovibrio_A sp900143255"
            ),
            "ncbi_accession": "GCF_900143255.1",
            "taxonomy_file": "fixture_release_226.tsv",
        },
    )
    return pl.DataFrame(rows).unique(subset=["ncbi_accession"], keep="first")


@lru_cache(maxsize=1)
def get_release_226_lineage_by_accession() -> dict[str, str]:
    """Return bundled release 226.0 lineage text keyed by NCBI accession."""

    return dict(
        get_release_226_taxonomy().select("ncbi_accession", "lineage").iter_rows(),
    )


@pytest.mark.parametrize("fixture_path", get_fixture_paths(), ids=lambda path: path.stem)
def test_fixture_rows_match_bundled_release_226_membership(fixture_path: Path) -> None:
    """Real GTDB export fixtures should align with bundled release 226.0."""

    requested_taxon = fixture_path.stem
    fixture_rows = load_fixture_rows(fixture_path)
    lineage_by_accession = get_release_226_lineage_by_accession()

    assert len(fixture_rows) == EXPECTED_FIXTURE_ROW_COUNTS[requested_taxon]

    for row in fixture_rows:
        assert requested_taxon in get_lineage_tokens(row.lineage)
        assert row.accession in lineage_by_accession
        assert lineage_by_accession[row.accession] == row.lineage


@pytest.mark.parametrize("fixture_path", get_fixture_paths(), ids=lambda path: path.stem)
def test_select_taxa_matches_real_gtdb_export_fixture(fixture_path: Path) -> None:
    """Exact taxon matching should reproduce the real GTDB export accession set."""

    requested_taxon = fixture_path.stem
    fixture_rows = load_fixture_rows(fixture_path)
    taxonomy_frame = get_release_226_taxonomy()

    selected = select_taxa(taxonomy_frame, [requested_taxon])

    assert selected["requested_taxon"].to_list() == [requested_taxon] * len(fixture_rows)
    assert sorted(selected["ncbi_accession"].to_list()) == sorted(
        row.accession for row in fixture_rows
    )


def test_release_226_excludes_suffix_variant_from_exact_genus_selection() -> None:
    """Exact genus selection should exclude suffixed GTDB genus variants."""

    taxonomy_frame = get_release_226_taxonomy()

    selected = select_taxa(taxonomy_frame, ["g__Frigididesulfovibrio"])

    assert "GCF_900143255.1" not in selected["ncbi_accession"].to_list()


def test_release_226_incomplete_species_token_returns_zero_matches() -> None:
    """Incomplete species taxa should not match bundled release 226.0 rows."""

    taxonomy_frame = get_release_226_taxonomy()

    selected = select_taxa(taxonomy_frame, ["s__Altiarchaeum"])

    assert selected.is_empty()

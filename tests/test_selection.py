"""Tests for GTDB taxon selection and slugging."""

from __future__ import annotations

import polars as pl

from gtdb_genomes.selection import (
    attach_taxon_slugs,
    build_taxon_slug_map,
    select_taxa,
)


def build_test_frame() -> pl.DataFrame:
    """Build a small taxonomy frame for selection tests."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "GB_GCA_000002.1",
                "RS_GCF_000003.1",
            ],
            "lineage": [
                (
                    "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;"
                    "o__Enterobacterales;f__Enterobacteriaceae;"
                    "g__Escherichia;s__Escherichia coli"
                ),
                (
                    "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;"
                    "o__Enterobacterales;f__Enterobacteriaceae;"
                    "g__Escherichia;s__Escherichia albertii"
                ),
                (
                    "d__Archaea;p__Halobacteriota;c__Methanosarcinia;"
                    "o__Methanosarcinales;f__Methanosarcinaceae;"
                    "g__Methanosarcina;s__Methanosarcina mazei"
                ),
            ],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCA_000002.1",
                "GCF_000003.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r95.tsv",
                "bac120_taxonomy_r95.tsv",
                "ar122_taxonomy_r95.tsv",
            ],
        },
    )


def test_select_taxa_matches_lineage_tokens() -> None:
    """Requested taxa should match rows by exact lineage token membership."""

    selected = select_taxa(
        build_test_frame(),
        ["g__Escherichia", "s__Escherichia coli"],
    )

    assert selected.height == 3
    assert "lineage_tokens" not in selected.columns
    assert selected["requested_taxon"].to_list() == [
        "g__Escherichia",
        "g__Escherichia",
        "s__Escherichia coli",
    ]
    assert selected["gtdb_accession"].to_list() == [
        "RS_GCF_000001.1",
        "GB_GCA_000002.1",
        "RS_GCF_000001.1",
    ]

def test_build_taxon_slug_map_handles_collisions() -> None:
    """Colliding taxon slugs should receive deterministic hash suffixes."""

    slug_map = build_taxon_slug_map(
        ["g__Escherichia", "s__Escherichia coli", "s__Escherichia/coli"],
    )

    assert slug_map["g__Escherichia"] == "g__Escherichia"
    assert slug_map["s__Escherichia coli"].startswith("s__Escherichia_coli__")
    assert slug_map["s__Escherichia/coli"].startswith("s__Escherichia_coli__")
    assert slug_map["s__Escherichia coli"] != slug_map["s__Escherichia/coli"]


def test_attach_taxon_slugs_adds_slug_column() -> None:
    """Selected rows should receive the requested taxon slug."""

    selected = select_taxa(build_test_frame(), ["g__Escherichia"])
    with_slugs = attach_taxon_slugs(selected, ["g__Escherichia"])

    assert "taxon_slug" in with_slugs.columns
    assert with_slugs["taxon_slug"].to_list() == ["g__Escherichia", "g__Escherichia"]


def test_select_taxa_does_not_treat_uba_taxon_names_as_uba_accessions() -> None:
    """UBA taxon names should not be confused with unsupported UBA accessions."""

    frame = pl.DataFrame(
        {
            "gtdb_accession": ["GB_GCA_123456789.1"],
            "lineage": [
                (
                    "d__Bacteria;p__Proteobacteria;c__Gammaproteobacteria;"
                    "o__Enterobacterales;f__UBA509aceae;g__UBA509;"
                    "s__UBA509 bacterium"
                ),
            ],
            "ncbi_accession": ["GCA_123456789.1"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    selected = select_taxa(frame, ["g__UBA509"])

    assert selected.height == 1
    assert selected["requested_taxon"].to_list() == ["g__UBA509"]
    assert selected["ncbi_accession"].to_list() == ["GCA_123456789.1"]

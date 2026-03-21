"""Tests for bundled GTDB release resolution."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.release_resolver import (
    BundledDataError,
    get_release_manifest_path,
    load_release_manifest,
    resolve_and_validate_release,
    resolve_release,
)
from gtdb_genomes.taxonomy import load_release_taxonomy


def write_manifest(manifest_path: Path, row: str) -> None:
    """Write a minimal test manifest to disk."""

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest"
                ),
                row,
            ],
        )
        + "\n",
        encoding="ascii",
    )


def write_manifest_text(manifest_path: Path, text: str) -> None:
    """Write a manifest file with custom content for negative test cases."""

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(text, encoding="ascii")


def write_gzip_text(path: Path, content: str) -> None:
    """Write one gzipped text file for a bundled-data test case."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="ascii", newline="") as handle:
        handle.write(content)


def write_gzip_bytes(path: Path, content: bytes) -> None:
    """Write one gzipped binary file for a bundled-data test case."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as handle:
        handle.write(content)


def test_load_release_manifest_reads_real_bundled_manifest() -> None:
    """The real bundled manifest should load successfully."""

    entries = load_release_manifest()

    assert entries
    assert any(entry.is_latest for entry in entries)
    assert any(entry.resolved_release == "226.0" for entry in entries)


def test_resolve_release_supports_latest_alias() -> None:
    """The latest alias should resolve from the manifest latest marker."""

    resolution = resolve_release("latest")

    assert resolution.resolved_release == "226.0"
    assert resolution.bacterial_taxonomy is not None
    assert resolution.archaeal_taxonomy is not None


def test_resolve_release_requires_one_latest_marker(tmp_path: Path) -> None:
    """The manifest should define exactly one latest release."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue",
                "214.0\t214,214.0\tbac214.tsv\tar214.tsv\ttrue",
            ],
        ),
    )

    with pytest.raises(BundledDataError, match="exactly one latest"):
        resolve_release("latest", data_root=data_root)


def test_load_release_manifest_rejects_duplicate_aliases(tmp_path: Path) -> None:
    """A CLI alias should not resolve silently to multiple release rows."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue",
                "214.0\t95,214.0\tbac214.tsv\tar214.tsv\tfalse",
            ],
        ),
    )

    with pytest.raises(BundledDataError, match="duplicate alias"):
        load_release_manifest(get_release_manifest_path(data_root))


def test_resolve_and_validate_release_uses_local_taxonomy_files(
    tmp_path: Path,
) -> None:
    """A known release should validate when its local payload files exist."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    write_gzip_text(release_dir / "bac.tsv.gz", "RS_GCF_000001.1\tlineage\n")
    write_gzip_text(release_dir / "ar.tsv.gz", "RS_GCF_000002.1\tlineage\n")

    resolution = resolve_and_validate_release("95", data_root=data_root)

    assert resolution.resolved_release == "95.0"
    assert resolution.bacterial_taxonomy == release_dir / "bac.tsv.gz"
    assert resolution.archaeal_taxonomy == release_dir / "ar.tsv.gz"


def test_load_release_taxonomy_reads_gzipped_tables_and_keeps_logical_names(
    tmp_path: Path,
) -> None:
    """Gzipped taxonomy tables should load without changing output filenames."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    write_gzip_text(
        release_dir / "bac.tsv.gz",
        "RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
    )
    write_gzip_text(
        release_dir / "ar.tsv.gz",
        "GB_GCA_000002.1\td__Archaea;g__Methanobrevibacter\n",
    )

    taxonomy_frame = load_release_taxonomy(
        resolve_and_validate_release("95", data_root=data_root),
    )

    assert taxonomy_frame["taxonomy_file"].to_list() == ["bac.tsv", "ar.tsv"]
    assert taxonomy_frame["ncbi_accession"].to_list() == [
        "GCF_000001.1",
        "GCA_000002.1",
    ]


def test_load_release_taxonomy_keeps_legacy_uba_accessions(tmp_path: Path) -> None:
    """Legacy UBA rows should stay intact in loaded taxonomy tables."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "80.0\t80,80.0\tbac.tsv.gz\t\ttrue",
    )
    write_gzip_text(
        data_root / "80.0" / "bac.tsv.gz",
        "UBA0001\td__Bacteria;g__Legacy\n",
    )

    taxonomy_frame = load_release_taxonomy(
        resolve_and_validate_release("80", data_root=data_root),
    )

    assert taxonomy_frame.filter(
        taxonomy_frame.get_column("ncbi_accession").str.starts_with("UBA"),
    ).height > 0


def test_load_release_manifest_accepts_extra_build_columns(tmp_path: Path) -> None:
    """Runtime manifest loading should ignore named build-only metadata columns."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue\t"
                    "https://example.invalid/release95/95.0/\tMD5SUM\t"
                    "bac.tsv.gz\tar.tsv.gz"
                ),
            ],
        )
        + "\n",
    )

    entries = load_release_manifest(get_release_manifest_path(data_root))

    assert entries[0].resolved_release == "95.0"


def test_load_release_taxonomy_raises_for_invalid_gzip_payload(
    tmp_path: Path,
) -> None:
    """Malformed bundled taxonomy tables should raise bundled-data errors."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    write_gzip_bytes(release_dir / "bac.tsv.gz", b"\xff\xfe\xfd")
    write_gzip_text(
        release_dir / "ar.tsv.gz",
        "GB_GCA_000002.1\td__Archaea;g__Methanobrevibacter\n",
    )

    with pytest.raises(BundledDataError, match="could not be parsed"):
        load_release_taxonomy(resolve_and_validate_release("95", data_root=data_root))


def test_load_release_manifest_raises_for_missing_manifest(tmp_path: Path) -> None:
    """Missing manifests should raise a bundled-data error."""

    missing_manifest = tmp_path / "gtdb_taxonomy" / "releases.tsv"

    with pytest.raises(BundledDataError):
        load_release_manifest(missing_manifest)


def test_load_release_manifest_rejects_missing_required_headers(
    tmp_path: Path,
) -> None:
    """Manifest loading should fail when a required header is absent."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "resolved_release\tbacterial_taxonomy\tarchaeal_taxonomy\tis_latest",
                "95.0\tbac.tsv\tar.tsv\ttrue",
            ],
        )
        + "\n",
    )

    with pytest.raises(BundledDataError, match="missing required columns"):
        load_release_manifest(get_release_manifest_path(data_root))


def test_load_release_manifest_rejects_blank_required_fields(
    tmp_path: Path,
) -> None:
    """Blank required values should fail manifest loading."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "resolved_release\taliases\tbacterial_taxonomy\t"
                "archaeal_taxonomy\tis_latest",
                "95.0\t \tbac.tsv\tar.tsv\ttrue",
            ],
        )
        + "\n",
    )

    with pytest.raises(BundledDataError, match="blank field aliases"):
        load_release_manifest(get_release_manifest_path(data_root))


def test_load_release_manifest_rejects_rows_with_too_many_columns(
    tmp_path: Path,
) -> None:
    """Rows with extra columns should fail manifest loading."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "resolved_release\taliases\tbacterial_taxonomy\t"
                "archaeal_taxonomy\tis_latest",
                "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue\textra",
            ],
        )
        + "\n",
    )

    with pytest.raises(BundledDataError, match="too many columns"):
        load_release_manifest(get_release_manifest_path(data_root))


def test_resolve_release_raises_for_unknown_alias(tmp_path: Path) -> None:
    """Unknown release aliases should raise a bundled-data error."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue",
    )

    with pytest.raises(BundledDataError):
        resolve_release("214", data_root=data_root)


def test_resolve_and_validate_release_raises_for_malformed_manifest(
    tmp_path: Path,
) -> None:
    """Malformed manifests should surface as bundled-data errors."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "resolved_release\taliases\tbacterial_taxonomy\t"
                "archaeal_taxonomy\tis_latest",
                "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue\textra",
            ],
        )
        + "\n",
    )

    with pytest.raises(BundledDataError, match="too many columns"):
        resolve_and_validate_release("95", data_root=data_root)


def test_cli_returns_exit_code_three_for_malformed_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The CLI should keep the documented exit code for malformed manifests."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                "resolved_release\taliases\tbacterial_taxonomy\t"
                "archaeal_taxonomy\tis_latest",
                "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue\textra",
            ],
        )
        + "\n",
    )
    monkeypatch.setattr(
        "gtdb_genomes.release_resolver.get_bundled_data_root",
        lambda: data_root,
    )

    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path / "output"),
        ],
    )

    assert exit_code == 3


def test_cli_returns_exit_code_three_for_malformed_taxonomy_table(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The CLI should keep the bundled-data exit code on bad taxonomy payloads."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    write_gzip_bytes(release_dir / "bac.tsv.gz", b"\xff\xfe\xfd")
    write_gzip_text(
        release_dir / "ar.tsv.gz",
        "GB_GCA_000002.1\td__Archaea;g__Methanobrevibacter\n",
    )
    monkeypatch.setattr(
        "gtdb_genomes.release_resolver.get_bundled_data_root",
        lambda: data_root,
    )

    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path / "output"),
        ],
    )

    assert exit_code == 3


def test_resolve_and_validate_release_raises_for_missing_taxonomy_file(
    tmp_path: Path,
) -> None:
    """Missing taxonomy files should raise a bundled-data error."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    release_dir.mkdir(parents=True, exist_ok=True)
    write_gzip_text(release_dir / "bac.tsv.gz", "acc\tlineage\n")

    with pytest.raises(BundledDataError):
        resolve_and_validate_release("95", data_root=data_root)

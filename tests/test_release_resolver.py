"""Tests for bundled GTDB release resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.release_resolver import (
    BundledDataError,
    get_release_manifest_path,
    load_release_manifest,
    resolve_and_validate_release,
    resolve_release,
)


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


def test_load_release_manifest_reads_real_bundled_manifest() -> None:
    """The real bundled manifest should load successfully."""

    entries = load_release_manifest()

    assert entries
    assert any(entry.is_latest for entry in entries)
    assert any(entry.resolved_release == "226.0" for entry in entries)


def test_resolve_release_supports_latest_alias() -> None:
    """The latest alias should resolve from bundled data."""

    resolution = resolve_release("latest")

    assert resolution.resolved_release == "226.0"
    assert resolution.bacterial_taxonomy is not None
    assert resolution.archaeal_taxonomy is not None


def test_resolve_and_validate_release_uses_bundled_taxonomy_files() -> None:
    """A known release should validate against the bundled payload."""

    resolution = resolve_and_validate_release("95")

    assert resolution.resolved_release == "95.0"
    assert resolution.bacterial_taxonomy is not None
    assert resolution.bacterial_taxonomy.is_file()
    assert resolution.archaeal_taxonomy is not None
    assert resolution.archaeal_taxonomy.is_file()


def test_load_release_manifest_raises_for_missing_manifest(tmp_path: Path) -> None:
    """Missing manifests should raise a bundled-data error."""

    missing_manifest = tmp_path / "gtdb_taxonomy" / "releases.tsv"

    with pytest.raises(BundledDataError):
        load_release_manifest(missing_manifest)


def test_resolve_release_raises_for_unknown_alias(tmp_path: Path) -> None:
    """Unknown release aliases should raise a bundled-data error."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue",
    )

    with pytest.raises(BundledDataError):
        resolve_release("214", data_root=data_root)


def test_resolve_and_validate_release_raises_for_missing_taxonomy_file(
    tmp_path: Path,
) -> None:
    """Missing taxonomy files should raise a bundled-data error."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv\tar.tsv\ttrue",
    )
    release_dir = data_root / "95.0"
    release_dir.mkdir(parents=True, exist_ok=True)
    (release_dir / "bac.tsv").write_text("acc\tlineage\n", encoding="ascii")

    with pytest.raises(BundledDataError):
        resolve_and_validate_release("95", data_root=data_root)

"""Tests for taxonomy manifest refresh and bootstrap helpers."""

from __future__ import annotations

import gzip
import hashlib
from pathlib import Path

import pytest

from gtdb_genomes.release_resolver import BundledDataError, validate_taxonomy_file
from gtdb_genomes.taxonomy_bundle import (
    BOOTSTRAP_COMMAND,
    TaxonomyBundleError,
    bootstrap_taxonomy_bundle,
    compress_tsv_bytes,
    refresh_taxonomy_bundle_manifest,
)


def write_manifest_text(path: Path, text: str) -> None:
    """Write one manifest fixture to disk."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="ascii")


def write_bytes(path: Path, content: bytes) -> None:
    """Write raw bytes to one fixture path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def build_md5_line(filename: str, content: bytes) -> str:
    """Return one checksum-file line for a fixture payload."""

    checksum = hashlib.md5(content).hexdigest()
    return f"{checksum} ./{filename}"


def write_checksum_file(
    root: Path,
    filename: str,
    payloads: dict[str, bytes],
) -> None:
    """Write one checksum listing for a fake mirror release directory."""

    content = "\n".join(
        build_md5_line(payload_name, payload)
        for payload_name, payload in payloads.items()
    )
    write_bytes(root / filename, (content + "\n").encode("ascii"))


def read_gzip_text(path: Path) -> str:
    """Read one gzipped text fixture."""

    with gzip.open(path, "rt", encoding="ascii") as handle:
        return handle.read()


def test_refresh_manifest_adds_uq_source_metadata_for_plain_tsv_release(
    tmp_path: Path,
) -> None:
    """Refresh should map release80 target names to plain upstream TSV files."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest"
                ),
                "80.0\t80,80.0\tbac_taxonomy_r80.tsv.gz\t\tfalse",
            ],
        )
        + "\n",
    )
    release_root = tmp_path / "mirror" / "release80" / "80.0"
    payloads = {
        "bac_taxonomy_r80.tsv": b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
    }
    write_checksum_file(release_root, "MD5SUM", payloads)

    entries = refresh_taxonomy_bundle_manifest(
        manifest_path,
        releases_root_url=(tmp_path / "mirror").as_uri() + "/",
    )

    assert len(entries) == 1
    assert entries[0].source_root_url == release_root.as_uri() + "/"
    assert entries[0].checksum_filename == "MD5SUM"
    assert entries[0].bacterial_source_name == "bac_taxonomy_r80.tsv"
    assert entries[0].archaeal_source_name is None
    manifest_text = manifest_path.read_text(encoding="ascii")
    assert "source_root_url" in manifest_text
    assert "bacterial_source_name" in manifest_text


def test_refresh_manifest_prefers_precompressed_source_when_available(
    tmp_path: Path,
) -> None:
    """Refresh should keep using upstream gzip files when the mirror exposes them."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest"
                ),
                (
                    "95.0\t95,95.0\tbac120_taxonomy_r95.tsv.gz\t"
                    "ar122_taxonomy_r95.tsv.gz\ttrue"
                ),
            ],
        )
        + "\n",
    )
    release_root = tmp_path / "mirror" / "release95" / "95.0"
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    archaeal_plain = b"RS_GCF_000002.1\td__Archaea;g__Methanobrevibacter\n"
    payloads = {
        "bac120_taxonomy_r95.tsv": bacterial_plain,
        "bac120_taxonomy_r95.tsv.gz": gzip.compress(bacterial_plain, mtime=7),
        "ar122_taxonomy_r95.tsv": archaeal_plain,
        "ar122_taxonomy_r95.tsv.gz": gzip.compress(archaeal_plain, mtime=7),
    }
    write_checksum_file(release_root, "MD5SUM", payloads)

    entries = refresh_taxonomy_bundle_manifest(
        manifest_path,
        releases_root_url=(tmp_path / "mirror").as_uri() + "/",
    )

    assert entries[0].checksum_filename == "MD5SUM"
    assert entries[0].bacterial_source_name == "bac120_taxonomy_r95.tsv.gz"
    assert entries[0].archaeal_source_name == "ar122_taxonomy_r95.tsv.gz"


def test_bootstrap_taxonomy_bundle_gzips_plain_tsv_payloads_deterministically(
    tmp_path: Path,
) -> None:
    """Bootstrap should gzip plain mirror TSV files with deterministic output."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    bacterial_plain = b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "80.0\t80,80.0\tbac_taxonomy_r80.tsv.gz\t\tfalse\t"
                    f"{(tmp_path / 'mirror' / 'release80' / '80.0').as_uri()}/\t"
                    "MD5SUM\tbac_taxonomy_r80.tsv\t"
                ),
            ],
        )
        + "\n",
    )
    release_root = tmp_path / "mirror" / "release80" / "80.0"
    payloads = {"bac_taxonomy_r80.tsv": bacterial_plain}
    write_checksum_file(release_root, "MD5SUM", payloads)
    write_bytes(release_root / "bac_taxonomy_r80.tsv", bacterial_plain)

    generated_paths = bootstrap_taxonomy_bundle(manifest_path, data_root=data_root)

    output_path = data_root / "80.0" / "bac_taxonomy_r80.tsv.gz"
    assert generated_paths == (output_path,)
    assert output_path.read_bytes() == compress_tsv_bytes(bacterial_plain)
    assert read_gzip_text(output_path) == bacterial_plain.decode("ascii")


def test_bootstrap_taxonomy_bundle_preserves_upstream_gzip_payloads(
    tmp_path: Path,
) -> None:
    """Bootstrap should keep upstream gzipped taxonomy files unchanged."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    archaeal_plain = b"RS_GCF_000002.1\td__Archaea;g__Methanobrevibacter\n"
    archaeal_gzip = gzip.compress(archaeal_plain, mtime=123)
    source_root = tmp_path / "mirror" / "release226" / "226.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "226.0\t226,latest\t\tar53_taxonomy_r226.tsv.gz\ttrue\t"
                    f"{source_root.as_uri()}/\tMD5SUM.txt\t\t"
                    "ar53_taxonomy_r226.tsv.gz"
                ),
            ],
        )
        + "\n",
    )
    payloads = {"ar53_taxonomy_r226.tsv.gz": archaeal_gzip}
    write_checksum_file(source_root, "MD5SUM.txt", payloads)
    write_bytes(source_root / "ar53_taxonomy_r226.tsv.gz", archaeal_gzip)

    bootstrap_taxonomy_bundle(manifest_path, data_root=data_root)

    output_path = data_root / "226.0" / "ar53_taxonomy_r226.tsv.gz"
    assert output_path.read_bytes() == archaeal_gzip


def test_bootstrap_taxonomy_bundle_rejects_missing_checksum_file(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when a release directory has no checksum file."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "80.0\t80,80.0\tbac_taxonomy_r80.tsv.gz\t\tfalse\t"
                    f"{(tmp_path / 'mirror' / 'release80' / '80.0').as_uri()}/\t"
                    "MD5SUM\tbac_taxonomy_r80.tsv\t"
                ),
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="Could not read URL"):
        bootstrap_taxonomy_bundle(manifest_path, data_root=manifest_path.parent)


def test_bootstrap_taxonomy_bundle_rejects_missing_checksum_entry(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when the checksum listing omits one source file."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "95.0\t95,95.0\tbac120_taxonomy_r95.tsv.gz\t\ttrue\t"
                    f"{source_root.as_uri()}/\tMD5SUM\t"
                    "bac120_taxonomy_r95.tsv.gz\t"
                ),
            ],
        )
        + "\n",
    )
    write_checksum_file(source_root, "MD5SUM", {})
    write_bytes(
        source_root / "bac120_taxonomy_r95.tsv.gz",
        gzip.compress(b"row\n", mtime=0),
    )

    with pytest.raises(TaxonomyBundleError, match="Checksum entry"):
        bootstrap_taxonomy_bundle(manifest_path, data_root=data_root)


def test_bootstrap_taxonomy_bundle_rejects_checksum_mismatch(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail when a downloaded file does not match the MD5 entry."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    data_root = manifest_path.parent
    source_root = tmp_path / "mirror" / "release95" / "95.0"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest\tsource_root_url\t"
                    "checksum_filename\tbacterial_source_name\t"
                    "archaeal_source_name"
                ),
                (
                    "95.0\t95,95.0\tbac120_taxonomy_r95.tsv.gz\t\ttrue\t"
                    f"{source_root.as_uri()}/\tMD5SUM\t"
                    "bac120_taxonomy_r95.tsv.gz\t"
                ),
            ],
        )
        + "\n",
    )
    payload = gzip.compress(b"row\n", mtime=0)
    write_checksum_file(
        source_root,
        "MD5SUM",
        {"bac120_taxonomy_r95.tsv.gz": gzip.compress(b"other\n", mtime=0)},
    )
    write_bytes(source_root / "bac120_taxonomy_r95.tsv.gz", payload)

    with pytest.raises(TaxonomyBundleError, match="Checksum mismatch"):
        bootstrap_taxonomy_bundle(manifest_path, data_root=data_root)


def test_bootstrap_taxonomy_bundle_requires_refreshed_source_metadata(
    tmp_path: Path,
) -> None:
    """Bootstrap should fail clearly when the manifest lacks build metadata."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tis_latest"
                ),
                "95.0\t95,95.0\tbac120_taxonomy_r95.tsv.gz\t\ttrue",
            ],
        )
        + "\n",
    )

    with pytest.raises(TaxonomyBundleError, match="Run the refresh command first"):
        bootstrap_taxonomy_bundle(manifest_path, data_root=manifest_path.parent)


def test_missing_taxonomy_error_recommends_bootstrap_command(
    tmp_path: Path,
) -> None:
    """Missing local taxonomy should point source checkouts at the bootstrap command."""

    missing_path = tmp_path / "data" / "gtdb_taxonomy" / "95.0" / "bac.tsv.gz"

    with pytest.raises(BundledDataError) as error_info:
        validate_taxonomy_file(missing_path)

    assert BOOTSTRAP_COMMAND in str(error_info.value)

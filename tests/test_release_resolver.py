"""Tests for bundled GTDB release resolution."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from gtdb_genomes.bundled_data_validation import (
    describe_taxonomy_file,
    hash_sha256_file,
)
from gtdb_genomes.cli import main
from gtdb_genomes.release_resolver import (
    BundledDataError,
    ReleaseResolution,
    get_release_manifest_path,
    load_release_manifest,
    resolve_and_validate_release,
    resolve_release,
    validate_release_payload,
)
from gtdb_genomes.taxonomy import load_release_taxonomy


def write_manifest(manifest_path: Path, row: str) -> None:
    """Write a minimal test manifest to disk."""

    expanded_rows = []
    for raw_row in row.splitlines():
        cells = raw_row.split("\t")
        if len(cells) == 5:
            resolved_release, aliases, bacterial_taxonomy, archaeal_taxonomy, is_latest = (
                cells
            )
            bacterial_sha256 = (
                "0" * 64 if bacterial_taxonomy.strip() else ""
            )
            archaeal_sha256 = (
                "0" * 64 if archaeal_taxonomy.strip() else ""
            )
            bacterial_rows = "1" if bacterial_taxonomy.strip() else ""
            archaeal_rows = "1" if archaeal_taxonomy.strip() else ""
            expanded_rows.append(
                "\t".join(
                    (
                        resolved_release,
                        aliases,
                        bacterial_taxonomy,
                        archaeal_taxonomy,
                        bacterial_sha256,
                        archaeal_sha256,
                        bacterial_rows,
                        archaeal_rows,
                        is_latest,
                    ),
                ),
            )
            continue
        expanded_rows.append(raw_row)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                    "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                    "archaeal_taxonomy_rows\tis_latest"
                ),
                *expanded_rows,
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


def write_bytes(path: Path, content: bytes) -> None:
    """Write raw bytes for a bundled-data test case."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def replace_manifest_row(
    manifest_path: Path,
    row: str,
) -> None:
    """Replace one test manifest with an explicit fully-populated row."""

    write_manifest_text(
        manifest_path,
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                    "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                    "archaeal_taxonomy_rows\tis_latest"
                ),
                row,
            ],
        )
        + "\n",
    )


def build_integrity_row(path: Path | None) -> tuple[str, str]:
    """Return one manifest SHA256 and row-count pair for a local taxonomy file."""

    if path is None:
        return "", ""
    digest, row_count = describe_taxonomy_file(path)
    return digest, str(row_count)


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
    bacterial_sha256, bacterial_rows = build_integrity_row(
        release_dir / "bac.tsv.gz",
    )
    archaeal_sha256, archaeal_rows = build_integrity_row(
        release_dir / "ar.tsv.gz",
    )
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{bacterial_sha256}\t{archaeal_sha256}\t"
            f"{bacterial_rows}\t{archaeal_rows}\ttrue"
        ),
    )

    resolution = resolve_and_validate_release("95", data_root=data_root)

    assert resolution.resolved_release == "95.0"
    assert resolution.bacterial_taxonomy == release_dir / "bac.tsv.gz"
    assert resolution.archaeal_taxonomy == release_dir / "ar.tsv.gz"


def test_validate_release_payload_requires_integrity_metadata(
    tmp_path: Path,
) -> None:
    """Payload validation should fail explicitly when integrity metadata is missing."""

    taxonomy_path = tmp_path / "95.0" / "bac.tsv.gz"
    write_gzip_text(taxonomy_path, "RS_GCF_000001.1\tlineage\n")

    resolution = ReleaseResolution(
        requested_release="95",
        resolved_release="95.0",
        bacterial_taxonomy=taxonomy_path,
        archaeal_taxonomy=None,
        release_manifest_path=tmp_path / "releases.tsv",
        release_manifest_sha256="0" * 64,
        bacterial_taxonomy_sha256=None,
        archaeal_taxonomy_sha256=None,
        bacterial_taxonomy_rows=1,
        archaeal_taxonomy_rows=None,
    )

    with pytest.raises(BundledDataError, match="integrity metadata is missing"):
        validate_release_payload(resolution)


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
    bacterial_sha256, bacterial_rows = build_integrity_row(
        release_dir / "bac.tsv.gz",
    )
    archaeal_sha256, archaeal_rows = build_integrity_row(
        release_dir / "ar.tsv.gz",
    )
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{bacterial_sha256}\t{archaeal_sha256}\t"
            f"{bacterial_rows}\t{archaeal_rows}\ttrue"
        ),
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
    bacterial_sha256, bacterial_rows = build_integrity_row(
        data_root / "80.0" / "bac.tsv.gz",
    )
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "80.0\t80,80.0\tbac.tsv.gz\t\t"
            f"{bacterial_sha256}\t\t{bacterial_rows}\t\ttrue"
        ),
    )

    taxonomy_frame = load_release_taxonomy(
        resolve_and_validate_release("80", data_root=data_root),
    )

    assert taxonomy_frame.filter(
        taxonomy_frame.get_column("ncbi_accession").str.starts_with("UBA"),
    ).height > 0


@pytest.mark.parametrize(
    "payload_bytes, expected_bacterial_sha256, expected_bacterial_rows, expected_message",
    [
        (
            gzip.compress(
                b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n",
                mtime=0,
            ),
            "0" * 64,
            "1",
            "checksum mismatch",
        ),
        (
            gzip.compress(
                (
                    b"RS_GCF_000001.1\td__Bacteria;g__Escherichia\n"
                    b"RS_GCF_000002.1\td__Bacteria;g__Escherichia\n"
                ),
                mtime=0,
            ),
            None,
            "1",
            "row count mismatch",
        ),
        (
            gzip.compress(b"\xff\xfe\xfd", mtime=0),
            None,
            "1",
            "could not be decoded as UTF-8",
        ),
        (
            b"not-a-gzip-payload",
            None,
            "1",
            "could not be decompressed",
        ),
    ],
)
def test_load_release_taxonomy_surfaces_bundled_data_corruption(
    tmp_path: Path,
    payload_bytes: bytes,
    expected_bacterial_sha256: str | None,
    expected_bacterial_rows: str,
    expected_message: str,
) -> None:
    """Runtime taxonomy loading should own bundled-data corruption checks."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest(
        get_release_manifest_path(data_root),
        "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\ttrue",
    )
    release_dir = data_root / "95.0"
    write_bytes(release_dir / "bac.tsv.gz", payload_bytes)
    write_gzip_text(
        release_dir / "ar.tsv.gz",
        "GB_GCA_000002.1\td__Archaea;g__Methanobrevibacter\n",
    )
    observed_bacterial_sha256 = hash_sha256_file(release_dir / "bac.tsv.gz")
    archaeal_sha256, archaeal_rows = build_integrity_row(release_dir / "ar.tsv.gz")
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{expected_bacterial_sha256 or observed_bacterial_sha256}\t"
            f"{archaeal_sha256}\t{expected_bacterial_rows}\t{archaeal_rows}\ttrue"
        ),
    )

    resolution = resolve_release("95", data_root=data_root)

    with pytest.raises(BundledDataError, match=expected_message):
        load_release_taxonomy(resolution)


def test_load_release_manifest_accepts_extra_build_columns(tmp_path: Path) -> None:
    """Runtime manifest loading should ignore named build-only metadata columns."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        "\n".join(
            [
                (
                    "resolved_release\taliases\tbacterial_taxonomy\t"
                    "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                    "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                    "archaeal_taxonomy_rows\tis_latest\tsource_root_url\t"
                    "checksum_filename"
                ),
                (
                    "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
                    f"{'0' * 64}\t{'1' * 64}\t1\t1\ttrue\t"
                    "https://example.invalid/release95/95.0/\tMD5SUM"
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
    archaeal_sha256, archaeal_rows = build_integrity_row(release_dir / "ar.tsv.gz")
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{hash_sha256_file(release_dir / 'bac.tsv.gz')}\t{archaeal_sha256}\t"
            f"1\t{archaeal_rows}\ttrue"
        ),
    )

    with pytest.raises(BundledDataError, match="could not be decoded as UTF-8"):
        resolve_and_validate_release("95", data_root=data_root)


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


def test_load_release_manifest_rejects_missing_bacterial_taxonomy_header(
    tmp_path: Path,
) -> None:
    """Manifest loading should require the bacterial taxonomy column."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        (
            "resolved_release\taliases\tarchaeal_taxonomy\t"
            "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
            "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest\n"
        ),
    )

    with pytest.raises(BundledDataError, match="bacterial_taxonomy"):
        load_release_manifest(get_release_manifest_path(data_root))


def test_load_release_manifest_rejects_missing_archaeal_taxonomy_header(
    tmp_path: Path,
) -> None:
    """Manifest loading should require the archaeal taxonomy column."""

    data_root = tmp_path / "gtdb_taxonomy"
    write_manifest_text(
        get_release_manifest_path(data_root),
        (
            "resolved_release\taliases\tbacterial_taxonomy\t"
            "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
            "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest\n"
        ),
    )

    with pytest.raises(BundledDataError, match="archaeal_taxonomy"):
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
                "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                "archaeal_taxonomy_rows\tis_latest",
                f"95.0\t \tbac.tsv\tar.tsv\t{'0' * 64}\t{'1' * 64}\t1\t1\ttrue",
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
                "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                "archaeal_taxonomy_rows\tis_latest",
                f"95.0\t95,95.0\tbac.tsv\tar.tsv\t{'0' * 64}\t{'1' * 64}\t1\t1\ttrue\textra",
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
                "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                "archaeal_taxonomy_rows\tis_latest",
                f"95.0\t95,95.0\tbac.tsv\tar.tsv\t{'0' * 64}\t{'1' * 64}\t1\t1\ttrue\textra",
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
                "archaeal_taxonomy\tbacterial_taxonomy_sha256\t"
                "archaeal_taxonomy_sha256\tbacterial_taxonomy_rows\t"
                "archaeal_taxonomy_rows\tis_latest",
                f"95.0\t95,95.0\tbac.tsv\tar.tsv\t{'0' * 64}\t{'1' * 64}\t1\t1\ttrue\textra",
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
    archaeal_sha256, archaeal_rows = build_integrity_row(release_dir / "ar.tsv.gz")
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{hash_sha256_file(release_dir / 'bac.tsv.gz')}\t{archaeal_sha256}\t"
            f"1\t{archaeal_rows}\ttrue"
        ),
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
    bacterial_sha256, bacterial_rows = build_integrity_row(
        release_dir / "bac.tsv.gz",
    )
    replace_manifest_row(
        get_release_manifest_path(data_root),
        (
            "95.0\t95,95.0\tbac.tsv.gz\tar.tsv.gz\t"
            f"{bacterial_sha256}\t{'1' * 64}\t{bacterial_rows}\t1\ttrue"
        ),
    )

    with pytest.raises(BundledDataError):
        resolve_and_validate_release("95", data_root=data_root)

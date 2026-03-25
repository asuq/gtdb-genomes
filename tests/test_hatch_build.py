"""Tests for the custom Hatch build hook."""

from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
from pathlib import Path
import zipfile

import pytest

pytest.importorskip("hatchling.builders.hooks.plugin.interface")

from hatch_build import (
    CustomBuildHook,
    append_requires_external_metadata,
    patch_wheel_metadata,
)
from hatch_metadata import get_external_runtime_requirements
from gtdb_genomes.release_resolver import BundledDataError


def write_taxonomy_payload(
    payload_path: Path,
    taxonomy_text: str,
) -> tuple[str, str]:
    """Write one compressed taxonomy payload and return its integrity record."""

    payload_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(payload_path, "wb") as handle:
        handle.write(taxonomy_text.encode("utf-8"))
    return (
        hashlib.sha256(payload_path.read_bytes()).hexdigest(),
        str(sum(1 for line in taxonomy_text.splitlines() if line.strip())),
    )


def write_release_manifest(
    manifest_path: Path,
    *,
    bacterial_sha256: str,
    bacterial_rows: str,
    archaeal_sha256: str,
    archaeal_rows: str,
) -> None:
    """Write one minimal synthetic bundled-release manifest."""

    manifest_path.write_text(
        (
            "resolved_release\taliases\tbacterial_taxonomy\tarchaeal_taxonomy\t"
            "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
            "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest\n"
            "999.0\t999,999.0,latest\tbac120_taxonomy_r999.tsv.gz\t"
            "ar53_taxonomy_r999.tsv.gz\t"
            f"{bacterial_sha256}\t{archaeal_sha256}\t"
            f"{bacterial_rows}\t{archaeal_rows}\ttrue\n"
        ),
        encoding="ascii",
    )


def write_minimal_wheel(wheel_path: Path) -> None:
    """Write one minimal synthetic wheel with a placeholder `RECORD`."""

    with zipfile.ZipFile(wheel_path, "w") as handle:
        handle.writestr(
            "gtdb_genomes/__init__.py",
            "__version__ = '0.2.0'\n",
        )
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/WHEEL",
            (
                "Wheel-Version: 1.0\n"
                "Generator: test\n"
                "Root-Is-Purelib: true\n"
                "Tag: py3-none-any\n"
            ),
        )
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/METADATA",
            "Metadata-Version: 2.4\nName: gtdb-genomes\nVersion: 0.2.0\n",
        )
        handle.writestr(
            "gtdb_genomes-0.2.0.dist-info/RECORD",
            "gtdb_genomes-0.2.0.dist-info/RECORD,,\n",
        )


def build_expected_record_hash(payload_bytes: bytes) -> str:
    """Return the expected wheel `RECORD` hash for one payload."""

    digest = hashlib.sha256(payload_bytes).digest()
    return (
        "sha256="
        f"{base64.urlsafe_b64encode(digest).decode('ascii').rstrip('=')}"
    )


def read_wheel_record_rows(
    wheel_path: Path,
) -> tuple[dict[str, tuple[str, str]], dict[str, bytes]]:
    """Return one wheel's `RECORD` rows plus the hashed file payloads."""

    with zipfile.ZipFile(wheel_path) as handle:
        payloads = {
            info.filename: handle.read(info.filename)
            for info in handle.infolist()
            if not info.is_dir()
        }
    record_member_name = next(
        member_name
        for member_name in payloads
        if member_name.endswith(".dist-info/RECORD")
    )
    record_rows = tuple(
        csv.reader(io.StringIO(payloads[record_member_name].decode("utf-8"))),
    )
    return (
        {
            row[0]: (row[1], row[2])
            for row in record_rows
        },
        payloads,
    )


def test_initialise_build_info_requires_force_include_dict(
    tmp_path: Path,
) -> None:
    """The build hook should reject non-dict force-include state explicitly."""

    hook = CustomBuildHook.__new__(CustomBuildHook)
    hook.directory = str(tmp_path)

    with pytest.raises(RuntimeError, match="force_include"):
        hook.initialise_build_info(build_data={"force_include": []})


def test_append_requires_external_metadata_appends_known_runtime_requirements() -> None:
    """Built metadata should advertise the documented external runtime tools once."""

    metadata_text = append_requires_external_metadata(
        "Metadata-Version: 2.4\nName: gtdb-genomes\nVersion: 0.2.0\n",
    )

    for requirement in get_external_runtime_requirements():
        assert f"Requires-External: {requirement}" in metadata_text
        assert metadata_text.count(f"Requires-External: {requirement}") == 1


def test_patch_wheel_metadata_regenerates_record_entries(
    tmp_path: Path,
) -> None:
    """Wheel metadata patching should keep `RECORD` hashes and sizes consistent."""

    wheel_path = tmp_path / "gtdb_genomes-0.2.0-py3-none-any.whl"
    write_minimal_wheel(wheel_path)

    patch_wheel_metadata(wheel_path)

    record_rows, payloads = read_wheel_record_rows(wheel_path)
    metadata_member_name = next(
        member_name
        for member_name in payloads
        if member_name.endswith(".dist-info/METADATA")
    )
    record_member_name = next(
        member_name
        for member_name in payloads
        if member_name.endswith(".dist-info/RECORD")
    )

    metadata_text = payloads[metadata_member_name].decode("utf-8")
    for requirement in get_external_runtime_requirements():
        assert f"Requires-External: {requirement}" in metadata_text

    for member_name, payload_bytes in payloads.items():
        recorded_hash, recorded_size = record_rows[member_name]
        if member_name == record_member_name:
            assert recorded_hash == ""
            assert recorded_size == ""
            continue
        assert recorded_hash == build_expected_record_hash(payload_bytes)
        assert recorded_size == str(len(payload_bytes))


def test_validate_bundled_taxonomy_accepts_complete_synthetic_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The build hook should accept a fully materialised synthetic payload."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    release_root = manifest_path.parent / "999.0"
    bacterial_sha256, bacterial_rows = write_taxonomy_payload(
        release_root / "bac120_taxonomy_r999.tsv.gz",
        "GB_GCA_999999.1\td__Bacteria;g__Syntheticus\n",
    )
    archaeal_sha256, archaeal_rows = write_taxonomy_payload(
        release_root / "ar53_taxonomy_r999.tsv.gz",
        "GB_GCA_999998.1\td__Archaea;g__Syntheticus\n",
    )
    write_release_manifest(
        manifest_path,
        bacterial_sha256=bacterial_sha256,
        bacterial_rows=bacterial_rows,
        archaeal_sha256=archaeal_sha256,
        archaeal_rows=archaeal_rows,
    )

    hook = CustomBuildHook.__new__(CustomBuildHook)
    monkeypatch.setattr(
        "hatch_build.get_release_manifest_path",
        lambda: manifest_path,
    )

    hook.validate_bundled_taxonomy()


def test_validate_bundled_taxonomy_rejects_invalid_synthetic_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The build hook should fail when one synthetic payload is invalid."""

    manifest_path = tmp_path / "data" / "gtdb_taxonomy" / "releases.tsv"
    release_root = manifest_path.parent / "999.0"
    _, bacterial_rows = write_taxonomy_payload(
        release_root / "bac120_taxonomy_r999.tsv.gz",
        "GB_GCA_999999.1\td__Bacteria;g__Syntheticus\n",
    )
    archaeal_sha256, archaeal_rows = write_taxonomy_payload(
        release_root / "ar53_taxonomy_r999.tsv.gz",
        "GB_GCA_999998.1\td__Archaea;g__Syntheticus\n",
    )
    write_release_manifest(
        manifest_path,
        bacterial_sha256="0" * 64,
        bacterial_rows=bacterial_rows,
        archaeal_sha256=archaeal_sha256,
        archaeal_rows=archaeal_rows,
    )

    hook = CustomBuildHook.__new__(CustomBuildHook)
    monkeypatch.setattr(
        "hatch_build.get_release_manifest_path",
        lambda: manifest_path,
    )

    with pytest.raises(BundledDataError, match="checksum mismatch"):
        hook.validate_bundled_taxonomy()

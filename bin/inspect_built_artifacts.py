"""Inspect built wheel and sdist archives for required packaged content."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import sys
import tarfile
import zipfile
from pathlib import Path


SDIST_REQUIRED_SUFFIXES = (
    "README.md",
    "LICENSE",
    "NOTICE",
    "licenses/CC-BY-SA-4.0.txt",
    "data/gtdb_taxonomy/releases.tsv",
)
SDIST_REQUIRED_FRAGMENTS = (
    "src/gtdb_genomes/",
    "data/gtdb_taxonomy/",
)
WHEEL_REQUIRED_SUFFIXES = (
    "gtdb_genomes/__init__.py",
    "gtdb_genomes/_build_info.json",
    "gtdb_genomes/data/gtdb_taxonomy/releases.tsv",
)
WHEEL_REQUIRED_FRAGMENTS = (
    "gtdb_genomes/data/gtdb_taxonomy/",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command-line arguments for the archive inspector."""

    parser = argparse.ArgumentParser(
        description="Inspect built gtdb-genomes distribution artifacts.",
    )
    parser.add_argument(
        "dist_dir",
        nargs="?",
        default="dist",
        help="Distribution directory that contains one wheel and one sdist.",
    )
    return parser.parse_args(argv)


def read_archive_members(archive_path: Path) -> tuple[str, ...]:
    """Return the ordered member names from one archive."""

    if archive_path.suffix == ".whl":
        with zipfile.ZipFile(archive_path) as handle:
            return tuple(handle.namelist())
    with tarfile.open(archive_path, "r:gz") as handle:
        return tuple(handle.getnames())


def build_record_hash(payload_bytes: bytes) -> str:
    """Return the expected wheel `RECORD` hash field for one payload."""

    digest = hashlib.sha256(payload_bytes).digest()
    encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"sha256={encoded_digest}"


def require_single_artifact(dist_dir: Path, pattern: str) -> Path:
    """Return the single artifact that matches one glob pattern."""

    matches = sorted(dist_dir.glob(pattern))
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one {pattern!r} artifact under {dist_dir}, "
            f"found {len(matches)}",
        )
    return matches[0]


def require_suffixes(
    members: tuple[str, ...],
    suffixes: tuple[str, ...],
    archive_label: str,
) -> None:
    """Require archive members whose names end with the selected suffixes."""

    missing = [
        suffix
        for suffix in suffixes
        if not any(member.endswith(suffix) for member in members)
    ]
    if missing:
        raise ValueError(
            f"{archive_label} is missing required members: {', '.join(missing)}",
        )


def require_fragments(
    members: tuple[str, ...],
    fragments: tuple[str, ...],
    archive_label: str,
) -> None:
    """Require archive members whose names contain the selected fragments."""

    missing = [
        fragment
        for fragment in fragments
        if not any(fragment in member for member in members)
    ]
    if missing:
        raise ValueError(
            f"{archive_label} is missing required content paths: "
            f"{', '.join(missing)}",
        )


def validate_wheel_record(wheel_path: Path) -> None:
    """Validate that one wheel has a self-consistent `RECORD`."""

    with zipfile.ZipFile(wheel_path) as handle:
        file_infos = {
            info.filename: info
            for info in handle.infolist()
            if not info.is_dir()
        }
        record_members = [
            member_name
            for member_name in file_infos
            if member_name.endswith(".dist-info/RECORD")
        ]
        if len(record_members) != 1:
            raise ValueError(
                f"{wheel_path.name} must contain exactly one dist-info RECORD file",
            )
        record_member_name = record_members[0]
        record_rows = tuple(
            csv.reader(
                io.StringIO(handle.read(record_member_name).decode("utf-8")),
            ),
        )
        record_map: dict[str, tuple[str, str]] = {}
        for row in record_rows:
            if len(row) != 3:
                raise ValueError(
                    f"{wheel_path.name} has a malformed RECORD row: {row!r}",
                )
            record_map[row[0]] = (row[1], row[2])

        missing_rows = [
            member_name
            for member_name in file_infos
            if member_name not in record_map
        ]
        if missing_rows:
            raise ValueError(
                f"{wheel_path.name} RECORD is missing file rows: "
                f"{', '.join(sorted(missing_rows))}",
            )

        unknown_rows = [
            member_name
            for member_name in record_map
            if member_name not in file_infos
        ]
        if unknown_rows:
            raise ValueError(
                f"{wheel_path.name} RECORD references missing files: "
                f"{', '.join(sorted(unknown_rows))}",
            )

        for member_name, member_info in file_infos.items():
            recorded_hash, recorded_size = record_map[member_name]
            if member_name == record_member_name:
                if recorded_hash or recorded_size:
                    raise ValueError(
                        f"{wheel_path.name} RECORD row for {record_member_name} "
                        "must not contain a hash or size",
                    )
                continue
            payload_bytes = handle.read(member_name)
            expected_hash = build_record_hash(payload_bytes)
            expected_size = str(member_info.file_size)
            if recorded_hash != expected_hash or recorded_size != expected_size:
                raise ValueError(
                    f"{wheel_path.name} RECORD mismatch for {member_name}",
                )


def inspect_artifacts(dist_dir: Path) -> None:
    """Validate the built sdist and wheel contents in one directory."""

    sdist_path = require_single_artifact(dist_dir, "*.tar.gz")
    wheel_path = require_single_artifact(dist_dir, "*.whl")
    sdist_members = read_archive_members(sdist_path)
    wheel_members = read_archive_members(wheel_path)

    require_suffixes(sdist_members, SDIST_REQUIRED_SUFFIXES, sdist_path.name)
    require_fragments(sdist_members, SDIST_REQUIRED_FRAGMENTS, sdist_path.name)
    require_suffixes(wheel_members, WHEEL_REQUIRED_SUFFIXES, wheel_path.name)
    require_fragments(wheel_members, WHEEL_REQUIRED_FRAGMENTS, wheel_path.name)
    validate_wheel_record(wheel_path)


def main(argv: list[str] | None = None) -> int:
    """Run the built-artifact inspection command."""

    args = parse_args(argv)
    dist_dir = Path(args.dist_dir)
    try:
        inspect_artifacts(dist_dir)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(f"Validated built artifacts under {dist_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

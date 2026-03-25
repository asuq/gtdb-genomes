"""Validation helpers for bundled GTDB taxonomy payloads."""

from __future__ import annotations

import gzip
import hashlib
from io import StringIO
from pathlib import Path, PurePosixPath, PureWindowsPath


def hash_sha256_bytes(data: bytes) -> str:
    """Return the SHA256 digest for one byte payload."""

    return hashlib.sha256(data).hexdigest()


def hash_sha256_file(path: Path) -> str:
    """Return the SHA256 digest for one file on disk."""

    return hash_sha256_bytes(path.read_bytes())


def normalise_optional_sha256(raw_value: str | None) -> str | None:
    """Normalise an optional SHA256 field from a manifest cell."""

    if raw_value is None:
        return None
    value = raw_value.strip().lower()
    if not value:
        return None
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("must be a 64-character hexadecimal SHA256 value")
    return value


def normalise_optional_row_count(raw_value: str | None) -> int | None:
    """Normalise an optional positive row-count field from a manifest cell."""

    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    if not value.isdigit():
        raise ValueError("must be a positive integer row count")
    row_count = int(value)
    if row_count <= 0:
        raise ValueError("must be a positive integer row count")
    return row_count


def normalise_optional_taxonomy_relative_path(raw_value: str | None) -> str | None:
    """Normalise one optional relative taxonomy path from a manifest cell."""

    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    normalised_value = value.replace("\\", "/")
    windows_path = PureWindowsPath(normalised_value)
    if windows_path.drive:
        raise ValueError("must not be drive-rooted")
    posix_path = PurePosixPath(normalised_value)
    if posix_path.is_absolute():
        raise ValueError("must be a relative path")
    if any(part in {".", ".."} for part in posix_path.parts):
        raise ValueError("must not contain parent-directory references")
    return str(posix_path)


def decode_taxonomy_bytes(
    data: bytes,
    *,
    compressed: bool,
    source_label: str,
) -> str:
    """Return decoded taxonomy text from one materialised payload."""

    try:
        decoded_bytes = gzip.decompress(data) if compressed else data
    except (OSError, gzip.BadGzipFile) as error:
        raise ValueError(
            f"Bundled taxonomy file could not be decompressed: {source_label}",
        ) from error
    try:
        return decoded_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"Bundled taxonomy file could not be decoded as UTF-8: {source_label}",
        ) from error


def count_and_validate_taxonomy_rows(
    taxonomy_text: str,
    *,
    source_label: str,
) -> int:
    """Validate taxonomy row structure and return the decompressed row count."""

    row_count = 0
    for line_number, raw_line in enumerate(
        StringIO(taxonomy_text),
        start=1,
    ):
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line:
            raise ValueError(
                f"Bundled taxonomy file contains a blank line at row {line_number}: "
                f"{source_label}",
            )
        accession, separator, lineage = line.partition("\t")
        if separator != "\t" or not accession or not lineage:
            raise ValueError(
                "Bundled taxonomy file has an invalid row structure at row "
                f"{line_number}: {source_label}",
            )
        row_count += 1
    if row_count == 0:
        raise ValueError(
            f"Bundled taxonomy file is empty: {source_label}",
        )
    return row_count


def describe_taxonomy_bytes(
    data: bytes,
    *,
    compressed: bool,
    source_label: str,
) -> tuple[str, int]:
    """Return the SHA256 digest and validated row count for one payload."""

    taxonomy_text = decode_taxonomy_bytes(
        data,
        compressed=compressed,
        source_label=source_label,
    )
    return hash_sha256_bytes(data), count_and_validate_taxonomy_rows(
        taxonomy_text,
        source_label=source_label,
    )


def load_validated_taxonomy_text(
    path: Path,
    *,
    expected_sha256: str,
    expected_row_count: int,
) -> str:
    """Return validated taxonomy text from one materialised payload file."""

    data = path.read_bytes()
    observed_sha256 = hash_sha256_bytes(data)
    if observed_sha256 != expected_sha256:
        raise ValueError(
            "Bundled taxonomy file checksum mismatch for "
            f"{path}: expected {expected_sha256}, observed {observed_sha256}",
        )
    taxonomy_text = decode_taxonomy_bytes(
        data,
        compressed=path.suffix == ".gz",
        source_label=str(path),
    )
    observed_row_count = count_and_validate_taxonomy_rows(
        taxonomy_text,
        source_label=str(path),
    )
    if observed_row_count != expected_row_count:
        raise ValueError(
            "Bundled taxonomy file row count mismatch for "
            f"{path}: expected {expected_row_count}, observed {observed_row_count}",
        )
    return taxonomy_text


def describe_taxonomy_file(path: Path) -> tuple[str, int]:
    """Return the SHA256 digest and validated row count for one file."""

    data = path.read_bytes()
    return describe_taxonomy_bytes(
        data,
        compressed=path.suffix == ".gz",
        source_label=str(path),
    )


def validate_taxonomy_file(
    path: Path,
    *,
    expected_sha256: str,
    expected_row_count: int,
) -> tuple[str, int]:
    """Validate one materialised taxonomy file against its manifest record."""

    observed_sha256, observed_row_count = describe_taxonomy_file(path)
    if observed_sha256 != expected_sha256:
        raise ValueError(
            "Bundled taxonomy file checksum mismatch for "
            f"{path}: expected {expected_sha256}, observed {observed_sha256}",
        )
    if observed_row_count != expected_row_count:
        raise ValueError(
            "Bundled taxonomy file row count mismatch for "
            f"{path}: expected {expected_row_count}, observed {observed_row_count}",
        )
    return observed_sha256, observed_row_count

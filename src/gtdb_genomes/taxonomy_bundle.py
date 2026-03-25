"""Helpers for refreshing and bootstrapping bundled GTDB taxonomy data."""

from __future__ import annotations

import csv
import gzip
import hashlib
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import urlopen

from gtdb_genomes.bundled_data_validation import (
    describe_taxonomy_file,
    normalise_optional_row_count,
    normalise_optional_sha256,
    normalise_optional_taxonomy_relative_path,
)
from gtdb_genomes.manifest_validation import (
    ManifestHeaderValidationError,
    ManifestIntegrityPairingError,
    ManifestInvalidFieldError,
    ManifestRequiredFieldError,
    get_required_manifest_field_value,
    normalise_manifest_headers,
    parse_optional_manifest_field,
    validate_manifest_integrity_pairing,
    validate_required_manifest_headers,
)


UQ_RELEASES_ROOT = "https://data.ace.uq.edu.au/public/gtdb/data/releases/"
BOOTSTRAP_COMMAND = "uv run python -m gtdb_genomes.bootstrap_taxonomy"
BUILD_MANIFEST_FIELDS = (
    "resolved_release",
    "aliases",
    "bacterial_taxonomy",
    "archaeal_taxonomy",
    "bacterial_taxonomy_sha256",
    "archaeal_taxonomy_sha256",
    "bacterial_taxonomy_rows",
    "archaeal_taxonomy_rows",
    "is_latest",
    "source_root_url",
    "checksum_filename",
)
REQUIRED_RUNTIME_FIELDS = (
    "resolved_release",
    "aliases",
    "bacterial_taxonomy",
    "archaeal_taxonomy",
    "bacterial_taxonomy_sha256",
    "archaeal_taxonomy_sha256",
    "bacterial_taxonomy_rows",
    "archaeal_taxonomy_rows",
    "is_latest",
)
CHECKSUM_CANDIDATE_FILENAMES = ("MD5SUM.txt", "MD5SUM")


@dataclass(frozen=True, slots=True)
class TaxonomyBundleError(Exception):
    """Raised when manifest refresh or bootstrap fails."""

    message: str

    def __str__(self) -> str:
        """Return the stored error message."""

        return self.message


@dataclass(frozen=True, slots=True)
class TaxonomyBundleEntry:
    """One release row from the GTDB bundling manifest."""

    resolved_release: str
    aliases: str
    bacterial_taxonomy: str | None
    archaeal_taxonomy: str | None
    bacterial_taxonomy_sha256: str | None
    archaeal_taxonomy_sha256: str | None
    bacterial_taxonomy_rows: int | None
    archaeal_taxonomy_rows: int | None
    is_latest: str
    source_root_url: str | None
    checksum_filename: str | None


def normalise_optional_field(raw_value: str | None) -> str | None:
    """Return a stripped optional manifest field or ``None``."""

    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    return value


def normalise_optional_taxonomy_path(raw_value: str | None) -> str | None:
    """Return one validated optional taxonomy-relative manifest field."""

    try:
        return normalise_optional_taxonomy_relative_path(raw_value)
    except ValueError as error:
        raise TaxonomyBundleError(str(error)) from error


def get_required_manifest_field(
    row: dict[str, str | None],
    field_name: str,
    manifest_path: Path,
    line_number: int,
) -> str:
    """Return one required manifest field or raise a manifest error."""

    try:
        return get_required_manifest_field_value(row, field_name)
    except ManifestRequiredFieldError as error:
        if error.kind == "missing_field":
            raise TaxonomyBundleError(
                f"Manifest row {line_number} is missing field {field_name}: "
                f"{manifest_path}",
            ) from error
        raise TaxonomyBundleError(
            f"Manifest row {line_number} has a blank field {field_name}: "
            f"{manifest_path}",
        ) from error


def parse_manifest_integrity_field(
    raw_value: str | None,
    *,
    field_name: str,
    manifest_path: Path,
    line_number: int,
    parser,
) -> str | int | None:
    """Parse one optional runtime-integrity field from the manifest."""

    try:
        return parse_optional_manifest_field(
            raw_value,
            field_name=field_name,
            parser=parser,
        )
    except ManifestInvalidFieldError as error:
        raise TaxonomyBundleError(
            f"Manifest row {line_number} has an invalid field {field_name}: "
            f"{manifest_path} ({error.detail})",
        ) from error


def validate_entry_integrity_fields(
    entry: TaxonomyBundleEntry,
    *,
    manifest_path: Path,
    line_number: int,
) -> None:
    """Validate one manifest row's taxonomy and integrity field pairing."""

    field_sets = (
        (
            "bacterial_taxonomy",
            entry.bacterial_taxonomy,
            "bacterial_taxonomy_sha256",
            entry.bacterial_taxonomy_sha256,
            "bacterial_taxonomy_rows",
            entry.bacterial_taxonomy_rows,
        ),
        (
            "archaeal_taxonomy",
            entry.archaeal_taxonomy,
            "archaeal_taxonomy_sha256",
            entry.archaeal_taxonomy_sha256,
            "archaeal_taxonomy_rows",
            entry.archaeal_taxonomy_rows,
        ),
    )
    for (
        taxonomy_field_name,
        taxonomy_path,
        sha256_field_name,
        sha256_value,
        row_count_field_name,
        row_count_value,
    ) in field_sets:
        try:
            validate_manifest_integrity_pairing(
                taxonomy_field_name=taxonomy_field_name,
                taxonomy_path=taxonomy_path,
                sha256_field_name=sha256_field_name,
                sha256_value=sha256_value,
                row_count_field_name=row_count_field_name,
                row_count_value=row_count_value,
            )
        except ManifestIntegrityPairingError as error:
            if error.kind == "orphan_integrity":
                raise TaxonomyBundleError(
                    f"Manifest row {line_number} defines {sha256_field_name} or "
                    f"{row_count_field_name} without {error.taxonomy_field_name}: "
                    f"{manifest_path}",
                ) from error
            raise TaxonomyBundleError(
                f"Manifest row {line_number} is missing field "
                f"{error.related_field_name}: {manifest_path}",
            ) from error


def validate_manifest_header(
    fieldnames: list[str | None] | None,
    manifest_path: Path,
) -> None:
    """Validate a manifest header for refresh and bootstrap operations."""

    try:
        normalised_fieldnames = normalise_manifest_headers(fieldnames)
        validate_required_manifest_headers(
            normalised_fieldnames,
            REQUIRED_RUNTIME_FIELDS,
        )
    except ManifestHeaderValidationError as error:
        if error.kind == "missing_header":
            raise TaxonomyBundleError(
                f"Manifest is missing a header row: {manifest_path}",
            ) from error
        if error.kind == "malformed_header":
            raise TaxonomyBundleError(
                f"Manifest has a malformed header row: {manifest_path}",
            ) from error
        if error.kind == "missing_required_fields":
            missing_text = ", ".join(error.missing_fields)
            raise TaxonomyBundleError(
                f"Manifest is missing required columns: {missing_text}",
            ) from error
        raise RuntimeError("Unexpected manifest header validation state") from error


def parse_manifest_row(
    row: dict[str, str | None],
    manifest_path: Path,
    line_number: int,
) -> TaxonomyBundleEntry:
    """Parse one manifest row into a bundling entry."""

    if None in row:
        raise TaxonomyBundleError(
            f"Manifest row {line_number} has too many columns: {manifest_path}",
        )
    entry = TaxonomyBundleEntry(
        resolved_release=get_required_manifest_field(
            row,
            "resolved_release",
            manifest_path,
            line_number,
        ),
        aliases=get_required_manifest_field(
            row,
            "aliases",
            manifest_path,
            line_number,
        ),
        bacterial_taxonomy=normalise_optional_taxonomy_path(
            row.get("bacterial_taxonomy"),
        ),
        archaeal_taxonomy=normalise_optional_taxonomy_path(
            row.get("archaeal_taxonomy"),
        ),
        bacterial_taxonomy_sha256=parse_manifest_integrity_field(
            row.get("bacterial_taxonomy_sha256"),
            field_name="bacterial_taxonomy_sha256",
            manifest_path=manifest_path,
            line_number=line_number,
            parser=normalise_optional_sha256,
        ),
        archaeal_taxonomy_sha256=parse_manifest_integrity_field(
            row.get("archaeal_taxonomy_sha256"),
            field_name="archaeal_taxonomy_sha256",
            manifest_path=manifest_path,
            line_number=line_number,
            parser=normalise_optional_sha256,
        ),
        bacterial_taxonomy_rows=parse_manifest_integrity_field(
            row.get("bacterial_taxonomy_rows"),
            field_name="bacterial_taxonomy_rows",
            manifest_path=manifest_path,
            line_number=line_number,
            parser=normalise_optional_row_count,
        ),
        archaeal_taxonomy_rows=parse_manifest_integrity_field(
            row.get("archaeal_taxonomy_rows"),
            field_name="archaeal_taxonomy_rows",
            manifest_path=manifest_path,
            line_number=line_number,
            parser=normalise_optional_row_count,
        ),
        is_latest=get_required_manifest_field(
            row,
            "is_latest",
            manifest_path,
            line_number,
        ),
        source_root_url=normalise_optional_field(row.get("source_root_url")),
        checksum_filename=normalise_optional_field(row.get("checksum_filename")),
    )
    validate_entry_integrity_fields(
        entry,
        manifest_path=manifest_path,
        line_number=line_number,
    )
    return entry


def load_taxonomy_bundle_manifest(
    manifest_path: Path,
) -> tuple[TaxonomyBundleEntry, ...]:
    """Load the manifest used to refresh and bootstrap taxonomy payloads."""

    if not manifest_path.exists():
        raise TaxonomyBundleError(f"Manifest is missing: {manifest_path}")
    try:
        with manifest_path.open("r", encoding="ascii", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            validate_manifest_header(reader.fieldnames, manifest_path)
            entries = tuple(
                parse_manifest_row(row, manifest_path, line_number)
                for line_number, row in enumerate(reader, start=2)
            )
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        raise TaxonomyBundleError(
            f"Manifest could not be read: {manifest_path}",
        ) from error
    return entries


def serialise_manifest_value(value: str | int | None) -> str:
    """Convert an optional manifest value into a writeable cell string."""

    if value is None:
        return ""
    return str(value)


def write_taxonomy_bundle_manifest(
    manifest_path: Path,
    entries: tuple[TaxonomyBundleEntry, ...],
) -> None:
    """Write the extended GTDB bundling manifest to disk."""

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(
        prefix=f".{manifest_path.stem}.write.",
        dir=manifest_path.parent,
    ) as temp_root:
        temp_path = Path(temp_root) / manifest_path.name
        with temp_path.open("w", encoding="ascii", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                delimiter="\t",
                fieldnames=BUILD_MANIFEST_FIELDS,
                lineterminator="\n",
            )
            writer.writeheader()
            for entry in entries:
                writer.writerow(
                    {
                        "resolved_release": entry.resolved_release,
                        "aliases": entry.aliases,
                        "bacterial_taxonomy": serialise_manifest_value(
                            entry.bacterial_taxonomy,
                        ),
                        "archaeal_taxonomy": serialise_manifest_value(
                            entry.archaeal_taxonomy,
                        ),
                        "bacterial_taxonomy_sha256": serialise_manifest_value(
                            entry.bacterial_taxonomy_sha256,
                        ),
                        "archaeal_taxonomy_sha256": serialise_manifest_value(
                            entry.archaeal_taxonomy_sha256,
                        ),
                        "bacterial_taxonomy_rows": serialise_manifest_value(
                            entry.bacterial_taxonomy_rows,
                        ),
                        "archaeal_taxonomy_rows": serialise_manifest_value(
                            entry.archaeal_taxonomy_rows,
                        ),
                        "is_latest": entry.is_latest,
                        "source_root_url": serialise_manifest_value(
                            entry.source_root_url,
                        ),
                        "checksum_filename": serialise_manifest_value(
                            entry.checksum_filename,
                        ),
                    },
                )
        temp_path.replace(manifest_path)


def normalise_directory_url(directory_url: str) -> str:
    """Return one directory URL with a trailing slash."""

    return directory_url if directory_url.endswith("/") else f"{directory_url}/"


def build_release_source_root_url(
    resolved_release: str,
    releases_root_url: str = UQ_RELEASES_ROOT,
) -> str:
    """Build the UQ mirror release directory URL for one release row."""

    release_number = resolved_release.split(".", maxsplit=1)[0]
    return normalise_directory_url(
        urljoin(
            normalise_directory_url(releases_root_url),
            f"release{release_number}/{resolved_release}/",
        ),
    )


def join_directory_url(directory_url: str, filename: str) -> str:
    """Join one directory URL and filename without dropping the directory."""

    return urljoin(normalise_directory_url(directory_url), filename)


def read_url_bytes(url: str) -> bytes:
    """Read bytes from one URL or raise a bundling error."""

    try:
        with urlopen(url, timeout=60) as response:
            return response.read()
    except (URLError, OSError) as error:
        raise TaxonomyBundleError(f"Could not read URL: {url}") from error


def read_url_text(url: str) -> str:
    """Read ASCII-compatible text from one URL."""

    try:
        return read_url_bytes(url).decode("utf-8")
    except UnicodeDecodeError as error:
        raise TaxonomyBundleError(f"Could not decode text URL: {url}") from error


def parse_checksum_lines(
    checksum_text: str,
    checksum_url: str,
) -> dict[str, tuple[str, ...]]:
    """Parse one mirror checksum listing into filename-to-MD5 entries."""

    tokens = checksum_text.split()
    if len(tokens) % 2 != 0:
        raise TaxonomyBundleError(
            f"Checksum file has a malformed token count: {checksum_url}",
        )
    mapping: dict[str, list[str]] = {}
    for index in range(0, len(tokens), 2):
        checksum = tokens[index].strip().lower()
        filename = tokens[index + 1].strip().removeprefix("./")
        if len(checksum) != 32:
            raise TaxonomyBundleError(
                f"Checksum file has an invalid MD5 value for {filename}: "
                f"{checksum_url}",
            )
        entry_hashes = mapping.setdefault(filename, [])
        if checksum not in entry_hashes:
            entry_hashes.append(checksum)
    return {
        filename: tuple(entry_hashes)
        for filename, entry_hashes in mapping.items()
    }


def load_checksum_mapping(
    source_root_url: str,
    checksum_filename: str,
) -> dict[str, tuple[str, ...]]:
    """Load one release checksum mapping from the mirror."""

    checksum_url = join_directory_url(source_root_url, checksum_filename)
    return parse_checksum_lines(read_url_text(checksum_url), checksum_url)


def detect_checksum_mapping(
    source_root_url: str,
) -> tuple[str, dict[str, tuple[str, ...]]]:
    """Detect the checksum file and return its parsed mapping."""

    last_error: TaxonomyBundleError | None = None
    for candidate_name in CHECKSUM_CANDIDATE_FILENAMES:
        try:
            checksum_mapping = load_checksum_mapping(source_root_url, candidate_name)
        except TaxonomyBundleError as error:
            last_error = error
            continue
        return candidate_name, checksum_mapping
    raise TaxonomyBundleError(
        f"Could not find a checksum file under {source_root_url}",
    ) from last_error


def resolve_source_name(
    target_name: str | None,
    available_filenames: dict[str, tuple[str, ...]],
) -> str | None:
    """Resolve the best mirror source filename for one target taxonomy file."""

    if target_name is None:
        return None
    if target_name in available_filenames:
        return target_name
    if target_name.endswith(".gz"):
        uncompressed_name = target_name[:-3]
        if uncompressed_name in available_filenames:
            return uncompressed_name
    raise TaxonomyBundleError(
        f"Could not find a mirror source matching {target_name!r}",
    )


def refresh_manifest_entries(
    entries: tuple[TaxonomyBundleEntry, ...],
    releases_root_url: str = UQ_RELEASES_ROOT,
    logger: logging.Logger | None = None,
) -> tuple[TaxonomyBundleEntry, ...]:
    """Fill build-only source metadata for each configured release row."""

    refreshed_entries: list[TaxonomyBundleEntry] = []
    for entry in entries:
        source_root_url = build_release_source_root_url(
            entry.resolved_release,
            releases_root_url=releases_root_url,
        )
        checksum_filename, checksum_mapping = detect_checksum_mapping(
            source_root_url,
        )
        resolve_source_name(entry.bacterial_taxonomy, checksum_mapping)
        resolve_source_name(entry.archaeal_taxonomy, checksum_mapping)
        if logger is not None:
            logger.info(
                "Refreshed release %s from %s",
                entry.resolved_release,
                source_root_url,
            )
        refreshed_entries.append(
            TaxonomyBundleEntry(
                resolved_release=entry.resolved_release,
                aliases=entry.aliases,
                bacterial_taxonomy=entry.bacterial_taxonomy,
                archaeal_taxonomy=entry.archaeal_taxonomy,
                bacterial_taxonomy_sha256=entry.bacterial_taxonomy_sha256,
                archaeal_taxonomy_sha256=entry.archaeal_taxonomy_sha256,
                bacterial_taxonomy_rows=entry.bacterial_taxonomy_rows,
                archaeal_taxonomy_rows=entry.archaeal_taxonomy_rows,
                is_latest=entry.is_latest,
                source_root_url=source_root_url,
                checksum_filename=checksum_filename,
            ),
        )
    return tuple(refreshed_entries)


def refresh_taxonomy_bundle_manifest(
    manifest_path: Path,
    releases_root_url: str = UQ_RELEASES_ROOT,
    logger: logging.Logger | None = None,
) -> tuple[TaxonomyBundleEntry, ...]:
    """Refresh build-only source metadata in ``releases.tsv``."""

    refreshed_entries = refresh_manifest_entries(
        load_taxonomy_bundle_manifest(manifest_path),
        releases_root_url=releases_root_url,
        logger=logger,
    )
    write_taxonomy_bundle_manifest(manifest_path, refreshed_entries)
    return refreshed_entries


def get_checksum_for_source(
    source_name: str | None,
    checksum_mapping: dict[str, tuple[str, ...]],
    source_root_url: str,
) -> str | None:
    """Return the published checksum for one configured source file."""

    if source_name is None:
        return None
    checksums = checksum_mapping.get(source_name)
    if checksums is None:
        raise TaxonomyBundleError(
            f"Checksum entry for {source_name!r} is missing under "
            f"{source_root_url}",
        )
    if len(checksums) > 1:
        checksum_text = ", ".join(checksums)
        raise TaxonomyBundleError(
            "Checksum file defines conflicting entries for selected source file "
            f"{source_name!r} under {source_root_url}: {checksum_text}",
        )
    return checksums[0]


def verify_md5_checksum(
    data: bytes,
    expected_checksum: str,
    source_url: str,
) -> None:
    """Validate one downloaded payload against the published MD5 checksum."""

    observed_checksum = hashlib.md5(data).hexdigest()
    if observed_checksum != expected_checksum:
        raise TaxonomyBundleError(
            f"Checksum mismatch for {source_url}: expected {expected_checksum}, "
            f"observed {observed_checksum}",
        )


def compress_tsv_bytes(data: bytes) -> bytes:
    """Return deterministic gzip bytes for one plain TSV payload."""

    return gzip.compress(data, compresslevel=9, mtime=0)


def materialise_taxonomy_file(
    source_root_url: str,
    target_name: str | None,
    target_path: Path | None,
    checksum_mapping: dict[str, tuple[str, ...]],
) -> None:
    """Download, verify, and materialise one configured taxonomy file."""

    if target_name is None or target_path is None:
        return
    source_name = resolve_source_name(target_name, checksum_mapping)
    source_url = join_directory_url(source_root_url, source_name)
    expected_checksum = get_checksum_for_source(
        source_name,
        checksum_mapping,
        source_root_url,
    )
    if expected_checksum is None:
        raise TaxonomyBundleError(
            f"Checksum entry for {source_name!r} is missing under "
            f"{source_root_url}",
        )
    data = read_url_bytes(source_url)
    verify_md5_checksum(data, expected_checksum, source_url)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_name.endswith(".tsv.gz"):
        target_path.write_bytes(data)
        return
    if source_name.endswith(".tsv"):
        target_path.write_bytes(compress_tsv_bytes(data))
        return
    raise TaxonomyBundleError(
        f"Unsupported taxonomy source format for {source_url}",
    )


def validate_bootstrap_entry(
    entry: TaxonomyBundleEntry,
    *,
    allow_file_urls: bool = False,
) -> None:
    """Validate that one manifest row contains the source metadata bootstrap needs."""

    if entry.source_root_url is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing source_root_url in the "
            "manifest. Run the refresh command first.",
        )
    if not entry.source_root_url.startswith("https://"):
        if not allow_file_urls or not entry.source_root_url.startswith("file://"):
            raise TaxonomyBundleError(
                f"Release {entry.resolved_release} must use an HTTPS "
                f"source_root_url, got {entry.source_root_url!r}",
            )
    if entry.checksum_filename is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing checksum_filename in the "
            "manifest. Run the refresh command first.",
        )


def get_bootstrap_source_metadata(
    entry: TaxonomyBundleEntry,
    *,
    allow_file_urls: bool = False,
) -> tuple[str, str]:
    """Return the validated source URL and checksum filename for one entry."""

    validate_bootstrap_entry(entry, allow_file_urls=allow_file_urls)
    source_root_url = entry.source_root_url
    checksum_filename = entry.checksum_filename
    if source_root_url is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing source_root_url in the "
            "manifest. Run the refresh command first.",
        )
    if checksum_filename is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing checksum_filename in the "
            "manifest. Run the refresh command first.",
        )
    return source_root_url, checksum_filename


def describe_local_taxonomy_payload(path: Path | None) -> tuple[str | None, int | None]:
    """Return local taxonomy integrity details for one materialised payload."""

    if path is None:
        return None, None
    digest, row_count = describe_taxonomy_file(path)
    return digest, row_count


def refresh_runtime_integrity_entries(
    entries: tuple[TaxonomyBundleEntry, ...],
    data_root: Path,
) -> tuple[TaxonomyBundleEntry, ...]:
    """Refresh runtime integrity fields from the local materialised payloads."""

    refreshed_entries: list[TaxonomyBundleEntry] = []
    for entry in entries:
        release_directory = data_root / entry.resolved_release
        bacterial_path = (
            release_directory / entry.bacterial_taxonomy
            if entry.bacterial_taxonomy is not None
            else None
        )
        archaeal_path = (
            release_directory / entry.archaeal_taxonomy
            if entry.archaeal_taxonomy is not None
            else None
        )
        if bacterial_path is not None and bacterial_path.exists():
            bacterial_sha256, bacterial_rows = describe_local_taxonomy_payload(
                bacterial_path,
            )
        else:
            bacterial_sha256 = entry.bacterial_taxonomy_sha256
            bacterial_rows = entry.bacterial_taxonomy_rows
        if archaeal_path is not None and archaeal_path.exists():
            archaeal_sha256, archaeal_rows = describe_local_taxonomy_payload(
                archaeal_path,
            )
        else:
            archaeal_sha256 = entry.archaeal_taxonomy_sha256
            archaeal_rows = entry.archaeal_taxonomy_rows
        refreshed_entries.append(
            TaxonomyBundleEntry(
                resolved_release=entry.resolved_release,
                aliases=entry.aliases,
                bacterial_taxonomy=entry.bacterial_taxonomy,
                archaeal_taxonomy=entry.archaeal_taxonomy,
                bacterial_taxonomy_sha256=bacterial_sha256,
                archaeal_taxonomy_sha256=archaeal_sha256,
                bacterial_taxonomy_rows=bacterial_rows,
                archaeal_taxonomy_rows=archaeal_rows,
                is_latest=entry.is_latest,
                source_root_url=entry.source_root_url,
                checksum_filename=entry.checksum_filename,
            ),
        )
    return tuple(refreshed_entries)


def refresh_runtime_manifest(
    manifest_path: Path,
    entries: tuple[TaxonomyBundleEntry, ...],
    data_root: Path,
) -> None:
    """Write one manifest refreshed from the currently materialised payloads."""

    write_taxonomy_bundle_manifest(
        manifest_path,
        refresh_runtime_integrity_entries(entries, data_root),
    )


def swap_release_directories(
    staged_release_directory: Path,
    release_directory: Path,
) -> Path | None:
    """Atomically replace one release directory while preserving a rollback copy."""

    backup_directory = release_directory.parent / (
        f".{release_directory.name}.backup"
    )
    if backup_directory.exists():
        shutil.rmtree(backup_directory)
    if release_directory.exists():
        release_directory.rename(backup_directory)
    try:
        staged_release_directory.rename(release_directory)
    except Exception:
        if backup_directory.exists():
            if release_directory.exists():
                shutil.rmtree(release_directory)
            backup_directory.rename(release_directory)
        raise
    return backup_directory if backup_directory.exists() else None


def restore_release_directory(
    release_directory: Path,
    backup_directory: Path | None,
) -> None:
    """Restore one release directory from its backup copy if present."""

    if release_directory.exists():
        shutil.rmtree(release_directory)
    if backup_directory is not None and backup_directory.exists():
        backup_directory.rename(release_directory)


def discard_release_backup(backup_directory: Path | None) -> None:
    """Remove one staged release backup copy when replacement succeeds."""

    if backup_directory is not None and backup_directory.exists():
        shutil.rmtree(backup_directory)


def bootstrap_manifest_entries(
    entries: tuple[TaxonomyBundleEntry, ...],
    data_root: Path,
    logger: logging.Logger | None = None,
    *,
    manifest_path: Path | None = None,
    allow_file_urls: bool = False,
) -> tuple[Path, ...]:
    """Materialise all configured taxonomy payloads under ``data_root``."""

    generated_paths: list[Path] = []
    for entry in entries:
        source_root_url, checksum_filename = get_bootstrap_source_metadata(
            entry,
            allow_file_urls=allow_file_urls,
        )
        release_directory = data_root / entry.resolved_release
        checksum_mapping = load_checksum_mapping(
            source_root_url,
            checksum_filename,
        )
        with TemporaryDirectory(
            prefix=f".{entry.resolved_release}.bootstrap.",
            dir=data_root,
        ) as temp_root:
            staging_root = Path(temp_root)
            staged_release_directory = staging_root / entry.resolved_release
            staged_release_directory.mkdir(parents=True, exist_ok=True)
            bacterial_target = (
                staged_release_directory / entry.bacterial_taxonomy
                if entry.bacterial_taxonomy is not None
                else None
            )
            archaeal_target = (
                staged_release_directory / entry.archaeal_taxonomy
                if entry.archaeal_taxonomy is not None
                else None
            )
            materialise_taxonomy_file(
                source_root_url,
                entry.bacterial_taxonomy,
                bacterial_target,
                checksum_mapping,
            )
            materialise_taxonomy_file(
                source_root_url,
                entry.archaeal_taxonomy,
                archaeal_target,
                checksum_mapping,
            )
            backup_directory = swap_release_directories(
                staged_release_directory,
                release_directory,
            )
        try:
            if manifest_path is not None:
                refresh_runtime_manifest(manifest_path, entries, data_root)
        except Exception:
            restore_release_directory(release_directory, backup_directory)
            raise
        else:
            discard_release_backup(backup_directory)
        if logger is not None:
            logger.info("Bootstrapped release %s", entry.resolved_release)
        for generated_path in (
            release_directory / entry.bacterial_taxonomy
            if entry.bacterial_taxonomy is not None
            else None,
            release_directory / entry.archaeal_taxonomy
            if entry.archaeal_taxonomy is not None
            else None,
        ):
            if generated_path is not None:
                generated_paths.append(generated_path)
    return tuple(generated_paths)


def bootstrap_taxonomy_bundle(
    manifest_path: Path,
    data_root: Path,
    logger: logging.Logger | None = None,
    *,
    allow_file_urls: bool = False,
) -> tuple[Path, ...]:
    """Download and materialise all manifest-configured taxonomy payloads."""

    entries = load_taxonomy_bundle_manifest(manifest_path)
    generated_paths = bootstrap_manifest_entries(
        entries,
        data_root=data_root,
        logger=logger,
        manifest_path=manifest_path,
        allow_file_urls=allow_file_urls,
    )
    return generated_paths

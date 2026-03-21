"""Helpers for refreshing and bootstrapping bundled GTDB taxonomy data."""

from __future__ import annotations

import csv
import gzip
import hashlib
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import urlopen


UQ_RELEASES_ROOT = "https://data.ace.uq.edu.au/public/gtdb/data/releases/"
BOOTSTRAP_COMMAND = "uv run python -m gtdb_genomes.bootstrap_taxonomy"
BUILD_MANIFEST_FIELDS = (
    "resolved_release",
    "aliases",
    "bacterial_taxonomy",
    "archaeal_taxonomy",
    "is_latest",
    "source_root_url",
    "checksum_filename",
    "bacterial_source_name",
    "archaeal_source_name",
)
REQUIRED_RUNTIME_FIELDS = (
    "resolved_release",
    "aliases",
    "bacterial_taxonomy",
    "archaeal_taxonomy",
    "is_latest",
)
OPTIONAL_BUILD_FIELDS = BUILD_MANIFEST_FIELDS[len(REQUIRED_RUNTIME_FIELDS) :]
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
    is_latest: str
    source_root_url: str | None
    checksum_filename: str | None
    bacterial_source_name: str | None
    archaeal_source_name: str | None


def normalise_optional_field(raw_value: str | None) -> str | None:
    """Return a stripped optional manifest field or ``None``."""

    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    return value


def get_required_manifest_field(
    row: dict[str, str | None],
    field_name: str,
    manifest_path: Path,
    line_number: int,
) -> str:
    """Return one required manifest field or raise a manifest error."""

    raw_value = row.get(field_name)
    if raw_value is None:
        raise TaxonomyBundleError(
            f"Manifest row {line_number} is missing field {field_name}: "
            f"{manifest_path}",
        )
    value = raw_value.strip()
    if not value:
        raise TaxonomyBundleError(
            f"Manifest row {line_number} has a blank field {field_name}: "
            f"{manifest_path}",
        )
    return value


def validate_manifest_header(
    fieldnames: list[str | None] | None,
    manifest_path: Path,
) -> None:
    """Validate a manifest header for refresh and bootstrap operations."""

    if fieldnames is None:
        raise TaxonomyBundleError(
            f"Manifest is missing a header row: {manifest_path}",
        )
    normalised_fieldnames = [
        "" if fieldname is None else fieldname.strip() for fieldname in fieldnames
    ]
    if any(not fieldname for fieldname in normalised_fieldnames):
        raise TaxonomyBundleError(
            f"Manifest has a malformed header row: {manifest_path}",
        )
    missing_fields = [
        field_name
        for field_name in REQUIRED_RUNTIME_FIELDS
        if field_name not in normalised_fieldnames
    ]
    if missing_fields:
        missing_text = ", ".join(missing_fields)
        raise TaxonomyBundleError(
            f"Manifest is missing required columns: {missing_text}",
        )


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
    return TaxonomyBundleEntry(
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
        bacterial_taxonomy=normalise_optional_field(row.get("bacterial_taxonomy")),
        archaeal_taxonomy=normalise_optional_field(row.get("archaeal_taxonomy")),
        is_latest=get_required_manifest_field(
            row,
            "is_latest",
            manifest_path,
            line_number,
        ),
        source_root_url=normalise_optional_field(row.get("source_root_url")),
        checksum_filename=normalise_optional_field(row.get("checksum_filename")),
        bacterial_source_name=normalise_optional_field(
            row.get("bacterial_source_name"),
        ),
        archaeal_source_name=normalise_optional_field(row.get("archaeal_source_name")),
    )


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


def serialise_manifest_value(value: str | None) -> str:
    """Convert an optional manifest value into a writeable cell string."""

    if value is None:
        return ""
    return value


def write_taxonomy_bundle_manifest(
    manifest_path: Path,
    entries: tuple[TaxonomyBundleEntry, ...],
) -> None:
    """Write the extended GTDB bundling manifest to disk."""

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="ascii", newline="") as handle:
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
                    "is_latest": entry.is_latest,
                    "source_root_url": serialise_manifest_value(entry.source_root_url),
                    "checksum_filename": serialise_manifest_value(
                        entry.checksum_filename,
                    ),
                    "bacterial_source_name": serialise_manifest_value(
                        entry.bacterial_source_name,
                    ),
                    "archaeal_source_name": serialise_manifest_value(
                        entry.archaeal_source_name,
                    ),
                },
            )


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


def parse_checksum_lines(checksum_text: str, checksum_url: str) -> dict[str, str]:
    """Parse one mirror checksum listing into a filename-to-MD5 mapping."""

    tokens = checksum_text.split()
    if len(tokens) % 2 != 0:
        raise TaxonomyBundleError(
            f"Checksum file has a malformed token count: {checksum_url}",
        )
    mapping: dict[str, str] = {}
    for index in range(0, len(tokens), 2):
        checksum = tokens[index].strip().lower()
        filename = tokens[index + 1].strip().removeprefix("./")
        if len(checksum) != 32:
            raise TaxonomyBundleError(
                f"Checksum file has an invalid MD5 value for {filename}: "
                f"{checksum_url}",
            )
        if filename in mapping:
            raise TaxonomyBundleError(
                f"Checksum file defines {filename!r} more than once: {checksum_url}",
            )
        mapping[filename] = checksum
    return mapping


def load_checksum_mapping(
    source_root_url: str,
    checksum_filename: str,
) -> dict[str, str]:
    """Load one release checksum mapping from the mirror."""

    checksum_url = join_directory_url(source_root_url, checksum_filename)
    return parse_checksum_lines(read_url_text(checksum_url), checksum_url)


def detect_checksum_filename(source_root_url: str) -> str:
    """Detect the checksum filename used by one release directory."""

    last_error: TaxonomyBundleError | None = None
    for candidate_name in CHECKSUM_CANDIDATE_FILENAMES:
        try:
            load_checksum_mapping(source_root_url, candidate_name)
        except TaxonomyBundleError as error:
            last_error = error
            continue
        return candidate_name
    raise TaxonomyBundleError(
        f"Could not find a checksum file under {source_root_url}",
    ) from last_error


def resolve_source_name(
    target_name: str | None,
    available_filenames: dict[str, str],
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
        checksum_filename = detect_checksum_filename(source_root_url)
        checksum_mapping = load_checksum_mapping(source_root_url, checksum_filename)
        bacterial_source_name = resolve_source_name(
            entry.bacterial_taxonomy,
            checksum_mapping,
        )
        archaeal_source_name = resolve_source_name(
            entry.archaeal_taxonomy,
            checksum_mapping,
        )
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
                is_latest=entry.is_latest,
                source_root_url=source_root_url,
                checksum_filename=checksum_filename,
                bacterial_source_name=bacterial_source_name,
                archaeal_source_name=archaeal_source_name,
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
    checksum_mapping: dict[str, str],
    source_root_url: str,
) -> str | None:
    """Return the published checksum for one configured source file."""

    if source_name is None:
        return None
    checksum = checksum_mapping.get(source_name)
    if checksum is None:
        raise TaxonomyBundleError(
            f"Checksum entry for {source_name!r} is missing under "
            f"{source_root_url}",
        )
    return checksum


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
    source_name: str | None,
    target_path: Path | None,
    checksum_mapping: dict[str, str],
) -> None:
    """Download, verify, and materialise one configured taxonomy file."""

    if source_name is None or target_path is None:
        return
    source_url = join_directory_url(source_root_url, source_name)
    expected_checksum = get_checksum_for_source(
        source_name,
        checksum_mapping,
        source_root_url,
    )
    assert expected_checksum is not None
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


def validate_bootstrap_entry(entry: TaxonomyBundleEntry) -> None:
    """Validate that one manifest row contains the source metadata bootstrap needs."""

    if entry.source_root_url is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing source_root_url in the "
            "manifest. Run the refresh command first.",
        )
    if entry.checksum_filename is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing checksum_filename in the "
            "manifest. Run the refresh command first.",
        )
    if entry.bacterial_taxonomy is not None and entry.bacterial_source_name is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing bacterial_source_name in "
            "the manifest. Run the refresh command first.",
        )
    if entry.archaeal_taxonomy is not None and entry.archaeal_source_name is None:
        raise TaxonomyBundleError(
            f"Release {entry.resolved_release} is missing archaeal_source_name in "
            "the manifest. Run the refresh command first.",
        )


def clear_release_directory(release_directory: Path) -> None:
    """Remove one generated release directory before re-materialising it."""

    if release_directory.exists():
        shutil.rmtree(release_directory)


def bootstrap_manifest_entries(
    entries: tuple[TaxonomyBundleEntry, ...],
    data_root: Path,
    logger: logging.Logger | None = None,
) -> tuple[Path, ...]:
    """Materialise all configured taxonomy payloads under ``data_root``."""

    generated_paths: list[Path] = []
    for entry in entries:
        validate_bootstrap_entry(entry)
        assert entry.source_root_url is not None
        assert entry.checksum_filename is not None
        checksum_mapping = load_checksum_mapping(
            entry.source_root_url,
            entry.checksum_filename,
        )
        release_directory = data_root / entry.resolved_release
        clear_release_directory(release_directory)
        bacterial_target = (
            release_directory / entry.bacterial_taxonomy
            if entry.bacterial_taxonomy is not None
            else None
        )
        archaeal_target = (
            release_directory / entry.archaeal_taxonomy
            if entry.archaeal_taxonomy is not None
            else None
        )
        materialise_taxonomy_file(
            entry.source_root_url,
            entry.bacterial_source_name,
            bacterial_target,
            checksum_mapping,
        )
        materialise_taxonomy_file(
            entry.source_root_url,
            entry.archaeal_source_name,
            archaeal_target,
            checksum_mapping,
        )
        if logger is not None:
            logger.info("Bootstrapped release %s", entry.resolved_release)
        for generated_path in (bacterial_target, archaeal_target):
            if generated_path is not None:
                generated_paths.append(generated_path)
    return tuple(generated_paths)


def bootstrap_taxonomy_bundle(
    manifest_path: Path,
    data_root: Path,
    logger: logging.Logger | None = None,
) -> tuple[Path, ...]:
    """Download and materialise all manifest-configured taxonomy payloads."""

    entries = load_taxonomy_bundle_manifest(manifest_path)
    return bootstrap_manifest_entries(entries, data_root=data_root, logger=logger)

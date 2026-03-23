"""Bundled GTDB release manifest loading."""

from __future__ import annotations

import csv
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from gtdb_genomes.bundled_data_validation import (
    hash_sha256_file,
    normalise_optional_row_count,
    normalise_optional_sha256,
    validate_taxonomy_file,
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
from gtdb_genomes.taxonomy_bundle import BOOTSTRAP_COMMAND


@dataclass(frozen=True, slots=True)
class BundledDataError(Exception):
    """Raised when bundled GTDB data cannot be loaded."""

    message: str

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


@dataclass(frozen=True, slots=True)
class ReleaseManifestEntry:
    """One row from the bundled GTDB release manifest."""

    resolved_release: str
    aliases: tuple[str, ...]
    bacterial_taxonomy: str | None
    archaeal_taxonomy: str | None
    bacterial_taxonomy_sha256: str | None
    archaeal_taxonomy_sha256: str | None
    bacterial_taxonomy_rows: int | None
    archaeal_taxonomy_rows: int | None
    is_latest: bool


@dataclass(frozen=True, slots=True)
class ReleaseResolution:
    """Resolved bundled release information for a user request."""

    requested_release: str
    resolved_release: str
    bacterial_taxonomy: Path | None
    archaeal_taxonomy: Path | None
    release_manifest_path: Path
    release_manifest_sha256: str
    bacterial_taxonomy_sha256: str | None
    archaeal_taxonomy_sha256: str | None
    bacterial_taxonomy_rows: int | None
    archaeal_taxonomy_rows: int | None


REQUIRED_MANIFEST_FIELDS = (
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


def get_bundled_data_root() -> Path:
    """Return the bundled GTDB taxonomy root for repo or installed use."""

    package_root = Path(__file__).resolve().parent
    candidate_paths = (
        package_root.parents[1] / "data" / "gtdb_taxonomy",
        package_root / "data" / "gtdb_taxonomy",
    )
    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return candidate_path
    return candidate_paths[0]


def get_release_manifest_path(data_root: Path | None = None) -> Path:
    """Return the bundled release manifest path."""

    root = get_bundled_data_root() if data_root is None else data_root
    return root / "releases.tsv"


def parse_aliases(raw_aliases: str) -> tuple[str, ...]:
    """Parse the comma-separated aliases field."""

    aliases = [alias.strip() for alias in raw_aliases.split(",") if alias.strip()]
    if not aliases:
        raise BundledDataError("Bundled release manifest row is missing aliases")
    return tuple(aliases)


def parse_optional_path(raw_path: str | None) -> str | None:
    """Convert an optional TSV path field into a normalised value."""

    if raw_path is None:
        return None
    value = raw_path.strip()
    if not value:
        return None
    return value


def parse_is_latest(raw_value: str) -> bool:
    """Parse the manifest latest-release flag."""

    value = raw_value.strip().lower()
    if value not in {"true", "false"}:
        raise BundledDataError(
            "Bundled release manifest row has an invalid is_latest value",
        )
    return value == "true"


def parse_manifest_integrity_field(
    raw_value: str | None,
    *,
    field_name: str,
    path: Path,
    line_number: int,
    parser,
) -> str | int | None:
    """Parse one optional integrity field with a bundled-data error wrapper."""

    try:
        return parse_optional_manifest_field(
            raw_value,
            field_name=field_name,
            parser=parser,
        )
    except ManifestInvalidFieldError as error:
        raise BundledDataError(
            "Bundled release manifest row "
            f"{line_number} has an invalid {field_name} value: "
            f"{path} ({error.detail})",
        ) from error


def validate_manifest_entry_integrity(
    entry: ReleaseManifestEntry,
    *,
    path: Path,
    line_number: int,
) -> None:
    """Validate one manifest row's taxonomy-path and integrity-field pairing."""

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
                raise BundledDataError(
                    "Bundled release manifest row "
                    f"{line_number} defines {sha256_field_name} or "
                    f"{row_count_field_name} without {error.taxonomy_field_name}: "
                    f"{path}",
                ) from error
            raise BundledDataError(
                f"Bundled release manifest row {line_number} is missing "
                f"{error.related_field_name}: {path}",
            ) from error


def validate_manifest_headers(
    fieldnames: Sequence[str | None] | None,
    path: Path,
) -> None:
    """Validate that a bundled release manifest exposes the required columns."""

    try:
        normalised_fieldnames = normalise_manifest_headers(fieldnames)
        validate_required_manifest_headers(
            normalised_fieldnames,
            REQUIRED_MANIFEST_FIELDS,
        )
    except ManifestHeaderValidationError as error:
        if error.kind == "missing_header":
            raise BundledDataError(
                f"Bundled release manifest is missing a header row: {path}",
            ) from error
        if error.kind == "malformed_header":
            raise BundledDataError(
                f"Bundled release manifest has a malformed header row: {path}",
            ) from error
        if error.kind == "missing_required_fields":
            missing_text = ", ".join(error.missing_fields)
            raise BundledDataError(
                "Bundled release manifest is missing required columns: "
                f"{missing_text}",
            ) from error
        raise RuntimeError("Unexpected manifest header validation state") from error


def get_required_manifest_value(
    row: dict[str, str | None],
    field_name: str,
    path: Path,
    line_number: int,
) -> str:
    """Return one required manifest value or raise a bundled-data error."""

    try:
        return get_required_manifest_field_value(row, field_name)
    except ManifestRequiredFieldError as error:
        if error.kind == "missing_field":
            raise BundledDataError(
                f"Bundled release manifest row {line_number} is missing field "
                f"{field_name}: {path}",
            ) from error
        raise BundledDataError(
            f"Bundled release manifest row {line_number} has a blank field "
            f"{field_name}: {path}",
        ) from error


def parse_manifest_entry(
    row: dict[str, str | None],
    path: Path,
    line_number: int,
) -> ReleaseManifestEntry:
    """Parse one bundled manifest row into a release entry."""

    if None in row:
        raise BundledDataError(
            f"Bundled release manifest row {line_number} has too many columns: "
            f"{path}",
        )
    entry = ReleaseManifestEntry(
        resolved_release=get_required_manifest_value(
            row,
            "resolved_release",
            path,
            line_number,
        ),
        aliases=parse_aliases(
            get_required_manifest_value(
                row,
                "aliases",
                path,
                line_number,
            ),
        ),
        bacterial_taxonomy=parse_optional_path(row.get("bacterial_taxonomy")),
        archaeal_taxonomy=parse_optional_path(row.get("archaeal_taxonomy")),
        bacterial_taxonomy_sha256=parse_manifest_integrity_field(
            row.get("bacterial_taxonomy_sha256"),
            field_name="bacterial_taxonomy_sha256",
            path=path,
            line_number=line_number,
            parser=normalise_optional_sha256,
        ),
        archaeal_taxonomy_sha256=parse_manifest_integrity_field(
            row.get("archaeal_taxonomy_sha256"),
            field_name="archaeal_taxonomy_sha256",
            path=path,
            line_number=line_number,
            parser=normalise_optional_sha256,
        ),
        bacterial_taxonomy_rows=parse_manifest_integrity_field(
            row.get("bacterial_taxonomy_rows"),
            field_name="bacterial_taxonomy_rows",
            path=path,
            line_number=line_number,
            parser=normalise_optional_row_count,
        ),
        archaeal_taxonomy_rows=parse_manifest_integrity_field(
            row.get("archaeal_taxonomy_rows"),
            field_name="archaeal_taxonomy_rows",
            path=path,
            line_number=line_number,
            parser=normalise_optional_row_count,
        ),
        is_latest=parse_is_latest(
            get_required_manifest_value(
                row,
                "is_latest",
                path,
                line_number,
            ),
        ),
    )
    validate_manifest_entry_integrity(entry, path=path, line_number=line_number)
    return entry


def validate_manifest_aliases(
    entries: Sequence[ReleaseManifestEntry],
    path: Path,
) -> None:
    """Validate that manifest aliases are unique across release rows."""

    alias_map: dict[str, str] = {}
    for entry in entries:
        for alias in entry.aliases:
            existing_release = alias_map.get(alias)
            if existing_release is not None:
                raise BundledDataError(
                    "Bundled release manifest defines duplicate alias "
                    f"{alias!r} for releases {existing_release} and "
                    f"{entry.resolved_release}: {path}",
                )
            alias_map[alias] = entry.resolved_release


def load_release_manifest(
    manifest_path: Path | None = None,
) -> tuple[ReleaseManifestEntry, ...]:
    """Load the bundled GTDB release manifest from disk."""

    path = get_release_manifest_path() if manifest_path is None else manifest_path
    if not path.exists():
        raise BundledDataError(
            f"Bundled release manifest is missing: {path}",
        )
    try:
        with path.open("r", encoding="ascii", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            validate_manifest_headers(reader.fieldnames, path)
            entries = [
                parse_manifest_entry(row, path, line_number)
                for line_number, row in enumerate(reader, start=2)
            ]
            validate_manifest_aliases(entries, path)
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        raise BundledDataError(
            f"Bundled release manifest could not be read: {path}",
        ) from error
    return tuple(entries)


def find_manifest_entry(
    requested_release: str,
    entries: tuple[ReleaseManifestEntry, ...],
) -> ReleaseManifestEntry:
    """Find the manifest entry for a requested release alias."""

    release = requested_release.strip()
    if not release:
        raise BundledDataError("Requested release must not be empty")
    if release == "latest":
        latest_entries = [entry for entry in entries if entry.is_latest]
        if len(latest_entries) != 1:
            raise BundledDataError(
                "Bundled release manifest must mark exactly one latest release",
            )
        return latest_entries[0]
    for entry in entries:
        if release in entry.aliases:
            return entry
    raise BundledDataError(f"Bundled release is not available: {release}")


def build_taxonomy_path(
    data_root: Path,
    resolved_release: str,
    relative_path: str | None,
) -> Path | None:
    """Build the absolute taxonomy path for a bundled manifest field."""

    if relative_path is None:
        return None
    return data_root / resolved_release / relative_path


def build_release_resolution(
    entry: ReleaseManifestEntry,
    *,
    requested_release: str,
    data_root: Path,
    manifest_path: Path,
) -> ReleaseResolution:
    """Build one release resolution from a previously loaded manifest row."""

    return ReleaseResolution(
        requested_release=requested_release.strip(),
        resolved_release=entry.resolved_release,
        bacterial_taxonomy=build_taxonomy_path(
            data_root,
            entry.resolved_release,
            entry.bacterial_taxonomy,
        ),
        archaeal_taxonomy=build_taxonomy_path(
            data_root,
            entry.resolved_release,
            entry.archaeal_taxonomy,
        ),
        release_manifest_path=manifest_path,
        release_manifest_sha256=hash_sha256_file(manifest_path),
        bacterial_taxonomy_sha256=entry.bacterial_taxonomy_sha256,
        archaeal_taxonomy_sha256=entry.archaeal_taxonomy_sha256,
        bacterial_taxonomy_rows=entry.bacterial_taxonomy_rows,
        archaeal_taxonomy_rows=entry.archaeal_taxonomy_rows,
    )


def resolve_release(
    requested_release: str,
    data_root: Path | None = None,
) -> ReleaseResolution:
    """Resolve a release alias against the bundled GTDB manifest."""

    root = get_bundled_data_root() if data_root is None else data_root
    manifest_path = get_release_manifest_path(root)
    entries = load_release_manifest(manifest_path)
    entry = find_manifest_entry(requested_release, entries)
    return build_release_resolution(
        entry,
        requested_release=requested_release,
        data_root=root,
        manifest_path=manifest_path,
    )


def validate_configured_taxonomy_file(
    path: Path | None,
    *,
    expected_sha256: str | None,
    expected_row_count: int | None,
) -> None:
    """Validate one configured bundled taxonomy file path and integrity record."""

    if path is None:
        return
    if not path.exists():
        raise BundledDataError(
            f"Bundled taxonomy file is missing: {path}. For a source checkout, "
            f"run `{BOOTSTRAP_COMMAND}` first.",
        )
    if not path.is_file():
        raise BundledDataError(f"Bundled taxonomy path is not a file: {path}")
    if expected_sha256 is None or expected_row_count is None:
        raise BundledDataError(
            f"Bundled taxonomy integrity metadata is missing for {path}",
        )


def validate_release_resolution(resolution: ReleaseResolution) -> ReleaseResolution:
    """Validate the taxonomy file paths in a release resolution."""

    if resolution.bacterial_taxonomy is None and resolution.archaeal_taxonomy is None:
        raise BundledDataError(
            f"Bundled release has no taxonomy files configured: {resolution.resolved_release}",
        )
    validate_configured_taxonomy_file(
        resolution.bacterial_taxonomy,
        expected_sha256=resolution.bacterial_taxonomy_sha256,
        expected_row_count=resolution.bacterial_taxonomy_rows,
    )
    validate_configured_taxonomy_file(
        resolution.archaeal_taxonomy,
        expected_sha256=resolution.archaeal_taxonomy_sha256,
        expected_row_count=resolution.archaeal_taxonomy_rows,
    )
    return resolution


def validate_release_payload(resolution: ReleaseResolution) -> ReleaseResolution:
    """Fully validate one resolved bundled release payload for packaging."""

    validate_release_resolution(resolution)
    file_entries = (
        (
            resolution.bacterial_taxonomy,
            resolution.bacterial_taxonomy_sha256,
            resolution.bacterial_taxonomy_rows,
        ),
        (
            resolution.archaeal_taxonomy,
            resolution.archaeal_taxonomy_sha256,
            resolution.archaeal_taxonomy_rows,
        ),
    )
    for path, expected_sha256, expected_row_count in file_entries:
        if path is None:
            continue
        if expected_sha256 is None or expected_row_count is None:
            raise BundledDataError(
                "Bundled release payload is missing integrity metadata for "
                f"{path}",
            )
        try:
            validate_taxonomy_file(
                path,
                expected_sha256=expected_sha256,
                expected_row_count=expected_row_count,
            )
        except (OSError, ValueError) as error:
            raise BundledDataError(str(error)) from error
    return resolution


def resolve_and_validate_release(
    requested_release: str,
    data_root: Path | None = None,
) -> ReleaseResolution:
    """Resolve a release alias and fully validate the bundled taxonomy payload."""

    return validate_release_payload(
        resolve_release(requested_release, data_root=data_root),
    )

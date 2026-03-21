"""Bundled GTDB release manifest loading."""

from __future__ import annotations

import csv
import gzip
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

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
    is_latest: bool


@dataclass(frozen=True, slots=True)
class ReleaseResolution:
    """Resolved bundled release information for a user request."""

    requested_release: str
    resolved_release: str
    bacterial_taxonomy: Path | None
    archaeal_taxonomy: Path | None


REQUIRED_MANIFEST_FIELDS = (
    "resolved_release",
    "aliases",
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


def validate_manifest_headers(
    fieldnames: Sequence[str | None] | None,
    path: Path,
) -> None:
    """Validate that a bundled release manifest exposes the required columns."""

    if fieldnames is None:
        raise BundledDataError(
            f"Bundled release manifest is missing a header row: {path}",
        )
    normalised_fieldnames = tuple(
        "" if fieldname is None else fieldname.strip()
        for fieldname in fieldnames
    )
    if any(not fieldname for fieldname in normalised_fieldnames):
        raise BundledDataError(
            f"Bundled release manifest has a malformed header row: {path}",
        )
    missing_fields = [
        field_name
        for field_name in REQUIRED_MANIFEST_FIELDS
        if field_name not in normalised_fieldnames
    ]
    if missing_fields:
        missing_text = ", ".join(missing_fields)
        raise BundledDataError(
            "Bundled release manifest is missing required columns: "
            f"{missing_text}",
        )


def get_required_manifest_value(
    row: dict[str, str | None],
    field_name: str,
    path: Path,
    line_number: int,
) -> str:
    """Return one required manifest value or raise a bundled-data error."""

    raw_value = row.get(field_name)
    if raw_value is None:
        raise BundledDataError(
            f"Bundled release manifest row {line_number} is missing field "
            f"{field_name}: {path}",
        )
    value = raw_value.strip()
    if not value:
        raise BundledDataError(
            f"Bundled release manifest row {line_number} has a blank field "
            f"{field_name}: {path}",
        )
    return value


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
    return ReleaseManifestEntry(
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
        is_latest=parse_is_latest(
            get_required_manifest_value(
                row,
                "is_latest",
                path,
                line_number,
            ),
        ),
    )


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


def resolve_release(
    requested_release: str,
    data_root: Path | None = None,
) -> ReleaseResolution:
    """Resolve a release alias against the bundled GTDB manifest."""

    root = get_bundled_data_root() if data_root is None else data_root
    entries = load_release_manifest(get_release_manifest_path(root))
    entry = find_manifest_entry(requested_release, entries)
    return ReleaseResolution(
        requested_release=requested_release.strip(),
        resolved_release=entry.resolved_release,
        bacterial_taxonomy=build_taxonomy_path(
            root,
            entry.resolved_release,
            entry.bacterial_taxonomy,
        ),
        archaeal_taxonomy=build_taxonomy_path(
            root,
            entry.resolved_release,
            entry.archaeal_taxonomy,
        ),
    )


def validate_taxonomy_file(path: Path | None) -> None:
    """Validate a bundled taxonomy file path when one is configured."""

    if path is None:
        return
    if not path.exists():
        raise BundledDataError(
            f"Bundled taxonomy file is missing: {path}. For a source checkout, "
            f"run `{BOOTSTRAP_COMMAND}` first.",
        )
    if not path.is_file():
        raise BundledDataError(f"Bundled taxonomy path is not a file: {path}")
    try:
        if path.name.endswith(".gz"):
            with gzip.open(path, "rt", encoding="ascii", errors="ignore") as handle:
                handle.read(1)
        else:
            with path.open("r", encoding="ascii", errors="ignore") as handle:
                handle.read(1)
    except (OSError, gzip.BadGzipFile) as error:
        raise BundledDataError(
            f"Bundled taxonomy file could not be read: {path}",
        ) from error


def validate_release_resolution(resolution: ReleaseResolution) -> ReleaseResolution:
    """Validate the taxonomy file paths in a release resolution."""

    if resolution.bacterial_taxonomy is None and resolution.archaeal_taxonomy is None:
        raise BundledDataError(
            f"Bundled release has no taxonomy files configured: {resolution.resolved_release}",
        )
    validate_taxonomy_file(resolution.bacterial_taxonomy)
    validate_taxonomy_file(resolution.archaeal_taxonomy)
    return resolution


def resolve_and_validate_release(
    requested_release: str,
    data_root: Path | None = None,
) -> ReleaseResolution:
    """Resolve a release alias and validate the bundled taxonomy files."""

    return validate_release_resolution(
        resolve_release(requested_release, data_root=data_root),
    )

"""Bundled GTDB release manifest loading."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


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


def get_bundled_data_root() -> Path:
    """Return the repository path for bundled GTDB taxonomy data."""

    return Path(__file__).resolve().parents[2] / "data" / "gtdb_taxonomy"


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


def parse_optional_path(raw_path: str) -> str | None:
    """Convert an optional TSV path field into a normalised value."""

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
            entries = [
                ReleaseManifestEntry(
                    resolved_release=row["resolved_release"].strip(),
                    aliases=parse_aliases(row["aliases"]),
                    bacterial_taxonomy=parse_optional_path(row["bacterial_taxonomy"]),
                    archaeal_taxonomy=parse_optional_path(row["archaeal_taxonomy"]),
                    is_latest=parse_is_latest(row["is_latest"]),
                )
                for row in reader
            ]
    except OSError as error:
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
        raise BundledDataError(f"Bundled taxonomy file is missing: {path}")
    if not path.is_file():
        raise BundledDataError(f"Bundled taxonomy path is not a file: {path}")
    try:
        with path.open("r", encoding="ascii", errors="ignore"):
            pass
    except OSError as error:
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

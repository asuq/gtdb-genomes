"""Runtime and build provenance helpers."""

from __future__ import annotations

import json
import subprocess
import tomllib
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path


BUILD_INFO_FILENAME = "_build_info.json"
UNKNOWN_GIT_REVISION = "unknown"
UNKNOWN_TOOL_VERSION = ""


@dataclass(frozen=True, slots=True)
class RuntimeProvenance:
    """Resolved provenance details for one workflow run."""

    package_version: str
    git_revision: str
    datasets_version: str
    unzip_version: str
    release_manifest_sha256: str
    bacterial_taxonomy_sha256: str | None
    archaeal_taxonomy_sha256: str | None


def get_package_root() -> Path:
    """Return the installed or source package root."""

    return Path(__file__).resolve().parent


def get_repository_root() -> Path | None:
    """Return the repository root when running from a source checkout."""

    package_root = get_package_root()
    candidate_root = package_root.parents[1]
    if (candidate_root / "pyproject.toml").is_file():
        return candidate_root
    return None


def read_pyproject_version(project_root: Path) -> str:
    """Read the package version from one local pyproject file."""

    pyproject_path = project_root / "pyproject.toml"
    with pyproject_path.open("rb") as handle:
        pyproject = tomllib.load(handle)
    return str(pyproject["project"]["version"])


def get_package_version() -> str:
    """Return the installed package version or the source-checkout version."""

    try:
        return package_version("gtdb-genomes")
    except PackageNotFoundError:
        repository_root = get_repository_root()
        if repository_root is None:
            return "0.0.0+unknown"
        return read_pyproject_version(repository_root)


def get_packaged_build_info_path() -> Path:
    """Return the packaged build-info file path."""

    return get_package_root() / BUILD_INFO_FILENAME


def read_packaged_git_revision() -> str | None:
    """Return the packaged git revision when build metadata is available."""

    build_info_path = get_packaged_build_info_path()
    if not build_info_path.is_file():
        return None
    try:
        payload = json.loads(build_info_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    git_revision = payload.get("git_revision")
    if not isinstance(git_revision, str) or not git_revision.strip():
        return None
    return git_revision.strip()


def read_source_git_revision() -> str:
    """Return the source-checkout git revision when available."""

    repository_root = get_repository_root()
    if repository_root is None or not (repository_root / ".git").exists():
        return UNKNOWN_GIT_REVISION
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return UNKNOWN_GIT_REVISION
    if result.returncode != 0:
        return UNKNOWN_GIT_REVISION
    revision = result.stdout.strip()
    return revision or UNKNOWN_GIT_REVISION


def get_git_revision() -> str:
    """Return the best available git revision for provenance output."""

    packaged_revision = read_packaged_git_revision()
    if packaged_revision is not None:
        return packaged_revision
    return read_source_git_revision()


def write_build_info(
    path: Path,
    *,
    package_version_value: str,
    git_revision: str,
) -> None:
    """Write one generated build-info file for packaged runtimes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "package_version": package_version_value,
                "git_revision": git_revision,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def get_command_version(command: tuple[str, ...]) -> str:
    """Return a stable single-line version string for one external command."""

    try:
        result = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return UNKNOWN_TOOL_VERSION
    output = result.stdout.strip() or result.stderr.strip()
    if not output:
        return UNKNOWN_TOOL_VERSION
    return output.splitlines()[0].strip()


def build_runtime_provenance(
    *,
    release_manifest_sha256: str,
    bacterial_taxonomy_sha256: str | None,
    archaeal_taxonomy_sha256: str | None,
) -> RuntimeProvenance:
    """Build the runtime provenance details for one workflow run."""

    return RuntimeProvenance(
        package_version=get_package_version(),
        git_revision=get_git_revision(),
        datasets_version=get_command_version(("datasets", "version")),
        unzip_version=get_command_version(("unzip", "-v")),
        release_manifest_sha256=release_manifest_sha256,
        bacterial_taxonomy_sha256=bacterial_taxonomy_sha256,
        archaeal_taxonomy_sha256=archaeal_taxonomy_sha256,
    )


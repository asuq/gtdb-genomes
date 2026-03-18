"""NCBI genome download command construction and planning."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess


DEHYDRATE_ACCESSION_THRESHOLD = 1000
DEHYDRATE_SIZE_GB_THRESHOLD = 15.0
SIZE_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*([KMGT]?B)\b")
SIZE_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


@dataclass(slots=True)
class PreviewError(Exception):
    """Raised when the datasets preview command fails or cannot be parsed."""

    message: str

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


@dataclass(slots=True)
class DownloadMethodDecision:
    """Resolved download mode decision for a requested accession set."""

    requested_method: str
    method_used: str
    accession_count: int
    preview_size_bytes: int | None


def validate_include_value(include: str) -> str:
    """Normalise and validate a datasets include value."""

    tokens: list[str] = []
    for raw_token in include.split(","):
        token = raw_token.strip()
        if not token:
            raise ValueError("argument --include: values must not be empty")
        tokens.append(token)
    if "genome" not in tokens:
        raise ValueError("argument --include: value must contain 'genome'")
    return ",".join(tokens)


def build_preview_command(
    accessions: Iterable[str],
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a datasets preview command for genome accessions."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        *dict.fromkeys(accessions),
        "--include",
        validate_include_value(include),
        "--preview",
    ]
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def build_download_command(
    accessions: Iterable[str],
    archive_path: Path,
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    dehydrated: bool = False,
    debug: bool = False,
) -> list[str]:
    """Build a direct or dehydrated datasets genome download command."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        *dict.fromkeys(accessions),
        "--filename",
        str(archive_path),
        "--include",
        validate_include_value(include),
    ]
    if dehydrated:
        command.append("--dehydrated")
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def run_preview_command(
    accessions: Iterable[str],
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> str:
    """Run `datasets --preview` and return its raw stdout."""

    command = build_preview_command(
        accessions,
        include,
        api_key=api_key,
        datasets_bin=datasets_bin,
        debug=debug,
    )
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        error_message = result.stderr.strip() or result.stdout.strip()
        if not error_message:
            error_message = "datasets preview failed"
        raise PreviewError(error_message)
    return result.stdout


def build_rehydrate_command(
    directory: Path,
    max_workers: int,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a datasets rehydrate command for an extracted dehydrated bag."""

    command = [
        datasets_bin,
        "rehydrate",
        "--directory",
        str(directory),
        "--max-workers",
        str(max_workers),
    ]
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def parse_preview_size_bytes(preview_text: str) -> int | None:
    """Extract the largest size value from preview output."""

    matches = SIZE_PATTERN.findall(preview_text)
    if not matches:
        return None
    sizes = [
        int(float(size_value) * SIZE_UNITS[size_unit.upper()])
        for size_value, size_unit in matches
    ]
    return max(sizes)


def select_download_method(
    requested_method: str,
    accession_count: int,
    preview_text: str | None = None,
) -> DownloadMethodDecision:
    """Resolve the effective download method for a request."""

    if requested_method != "auto":
        return DownloadMethodDecision(
            requested_method=requested_method,
            method_used=requested_method,
            accession_count=accession_count,
            preview_size_bytes=None,
        )
    if preview_text is None:
        raise PreviewError("datasets preview output is required in auto mode")
    preview_size_bytes = parse_preview_size_bytes(preview_text)
    if preview_size_bytes is None:
        raise PreviewError("could not parse datasets preview output")
    method_used = "direct"
    if (
        accession_count >= DEHYDRATE_ACCESSION_THRESHOLD
        or preview_size_bytes > int(DEHYDRATE_SIZE_GB_THRESHOLD * (1024**3))
    ):
        method_used = "dehydrate"
    return DownloadMethodDecision(
        requested_method=requested_method,
        method_used=method_used,
        accession_count=accession_count,
        preview_size_bytes=preview_size_bytes,
    )

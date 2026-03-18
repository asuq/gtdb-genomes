"""NCBI genome download command construction and planning."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path


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

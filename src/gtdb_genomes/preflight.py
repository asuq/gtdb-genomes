"""Preflight checks for external tool availability."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(slots=True)
class PreflightError(Exception):
    """Raised when a required external tool is missing."""

    message: str

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


def get_required_tools(
    download_method: str,
    dry_run: bool,
    prefer_genbank: bool,
) -> tuple[str, ...]:
    """Return the external tools required for the requested execution path."""

    if dry_run:
        required_tools: list[str] = []
        if prefer_genbank or download_method == "auto":
            required_tools.append("datasets")
        return tuple(required_tools)
    return ("datasets", "unzip")


def check_required_tools(required_tools: Sequence[str]) -> None:
    """Ensure that the required external tools are available."""

    missing_tools = [
        tool_name
        for tool_name in required_tools
        if shutil.which(tool_name) is None
    ]
    if missing_tools:
        tools = ", ".join(missing_tools)
        raise PreflightError(
            f"Missing required external tools: {tools}",
        )

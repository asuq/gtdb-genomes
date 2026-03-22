"""Preflight checks for external tool availability."""

from __future__ import annotations

import re
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass


VERSION_NUMBER_PATTERN = re.compile(r"(?P<version>\d+(?:\.\d+)+)")


@dataclass(slots=True)
class PreflightError(Exception):
    """Raised when a required external tool is missing."""

    message: str

    def __str__(self) -> str:
        """Return the error message."""
        return self.message


@dataclass(frozen=True, slots=True)
class ToolVersionPolicy:
    """Supported version range for one required external command."""

    display_name: str
    version_command: tuple[str, ...]
    minimum_version: tuple[int, ...]
    maximum_version_exclusive: tuple[int, ...]
    supported_range: str


SUPPORTED_TOOL_VERSIONS = {
    "datasets": ToolVersionPolicy(
        display_name="ncbi-datasets-cli",
        version_command=("datasets", "version"),
        minimum_version=(18, 4, 0),
        maximum_version_exclusive=(18, 22, 0),
        supported_range=">=18.4.0,<18.22.0",
    ),
    "unzip": ToolVersionPolicy(
        display_name="unzip",
        version_command=("unzip", "-v"),
        minimum_version=(6, 0),
        maximum_version_exclusive=(7, 0),
        supported_range=">=6.0,<7.0",
    ),
}


def get_early_required_tools(
    dry_run: bool,
) -> tuple[str, ...]:
    """Return tools that must be checked before dry-run planning exits."""

    if not dry_run:
        return ()
    return ("unzip",)


def get_supported_preflight_tools(
    dry_run: bool,
) -> tuple[str, ...]:
    """Return tools required for supported planning and execution paths."""

    if dry_run:
        return ("datasets",)
    return ("datasets", "unzip")


def pad_version_tuple(
    version: tuple[int, ...],
    length: int,
) -> tuple[int, ...]:
    """Pad one parsed version tuple with trailing zeroes for comparison."""

    if len(version) >= length:
        return version
    return version + (0,) * (length - len(version))


def parse_tool_version(output: str) -> tuple[int, ...] | None:
    """Parse one command version line into an integer tuple."""

    match = VERSION_NUMBER_PATTERN.search(output)
    if match is None:
        return None
    return tuple(int(part) for part in match.group("version").split("."))


def read_tool_version_output(
    policy: ToolVersionPolicy,
) -> str | None:
    """Return the first available version line for one required tool."""

    try:
        result = subprocess.run(
            list(policy.version_command),
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = result.stdout.strip() or result.stderr.strip()
    if not output:
        return None
    return output.splitlines()[0].strip()


def is_supported_tool_version(
    version: tuple[int, ...],
    policy: ToolVersionPolicy,
) -> bool:
    """Return whether one parsed version tuple is inside the supported range."""

    comparison_length = max(
        len(version),
        len(policy.minimum_version),
        len(policy.maximum_version_exclusive),
    )
    parsed_version = pad_version_tuple(version, comparison_length)
    minimum_version = pad_version_tuple(policy.minimum_version, comparison_length)
    maximum_version = pad_version_tuple(
        policy.maximum_version_exclusive,
        comparison_length,
    )
    return minimum_version <= parsed_version < maximum_version


def build_tool_version_error(
    tool_name: str,
    policy: ToolVersionPolicy,
) -> str:
    """Return one preflight error message for an unreadable tool version."""

    return (
        "Could not determine the installed version for required external tool "
        f"{policy.display_name} (`{tool_name}`); supported range is "
        f"{policy.supported_range}."
    )


def build_tool_version_mismatch_error(
    policy: ToolVersionPolicy,
    raw_version_output: str,
) -> str:
    """Return one preflight error message for an unsupported tool version."""

    return (
        f"Unsupported required external tool version for {policy.display_name}: "
        f"{raw_version_output}. Supported range: {policy.supported_range}."
    )


def build_tool_version_parse_error(
    policy: ToolVersionPolicy,
    raw_version_output: str,
) -> str:
    """Return one preflight error message for an unparsable tool version."""

    return (
        "Could not parse the installed version for required external tool "
        f"{policy.display_name}: {raw_version_output}. Supported range: "
        f"{policy.supported_range}."
    )


def check_supported_tool_versions(required_tools: Sequence[str]) -> None:
    """Ensure required external tools are within the supported version window."""

    version_errors: list[str] = []
    for tool_name in required_tools:
        policy = SUPPORTED_TOOL_VERSIONS.get(tool_name)
        if policy is None:
            continue
        raw_version_output = read_tool_version_output(policy)
        if raw_version_output is None:
            version_errors.append(build_tool_version_error(tool_name, policy))
            continue
        parsed_version = parse_tool_version(raw_version_output)
        if parsed_version is None:
            version_errors.append(
                build_tool_version_parse_error(policy, raw_version_output),
            )
            continue
        if not is_supported_tool_version(parsed_version, policy):
            version_errors.append(
                build_tool_version_mismatch_error(policy, raw_version_output),
            )
    if version_errors:
        raise PreflightError(" ".join(version_errors))


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
    check_supported_tool_versions(required_tools)

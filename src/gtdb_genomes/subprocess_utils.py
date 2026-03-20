"""Shared subprocess timeout and error-message helpers."""

from __future__ import annotations

import subprocess


DEFAULT_SUBPROCESS_TIMEOUT_SECONDS = 4 * 60 * 60


def get_stage_display_name(stage: str) -> str:
    """Return one user-facing subprocess stage label."""

    return stage.replace("_", " ")


def build_subprocess_error_message(
    stage: str,
    result: subprocess.CompletedProcess[str],
) -> str:
    """Build a non-empty error message for one failed subprocess result."""

    error_message = result.stderr.strip() or result.stdout.strip()
    if error_message:
        return error_message
    return f"{get_stage_display_name(stage)} command failed"


def build_timeout_error_message(stage: str, timeout_seconds: int) -> str:
    """Build a timeout error message for one subprocess stage."""

    return (
        f"{get_stage_display_name(stage)} command timed out after "
        f"{timeout_seconds} seconds"
    )


def build_spawn_error_message(stage: str, error: OSError) -> str:
    """Build a process-spawn error message for one subprocess stage."""

    return f"{get_stage_display_name(stage)} command could not start: {error}"

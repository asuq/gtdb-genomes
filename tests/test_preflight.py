"""Focused tests for external-tool preflight helpers."""

from __future__ import annotations

import shutil
import subprocess

import pytest

from gtdb_genomes.preflight import (
    PreflightError,
    check_required_tools,
    get_early_required_tools,
    get_supported_preflight_tools,
)


def test_get_early_required_tools_only_requires_unzip_for_dry_runs() -> None:
    """Dry-runs should preflight `unzip` before planning exits."""

    assert get_early_required_tools(dry_run=True) == ("unzip",)
    assert get_early_required_tools(dry_run=False) == ()


def test_get_supported_preflight_tools_preserves_runtime_requirements() -> None:
    """Supported planning should keep datasets-only dry-runs and full real runs."""

    assert get_supported_preflight_tools(dry_run=True) == ("datasets",)
    assert get_supported_preflight_tools(dry_run=False) == (
        "datasets",
        "unzip",
    )


def test_check_required_tools_accepts_supported_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported external-tool versions should pass preflight unchanged."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return supported version output for each required command."""

        del capture_output, text, check, timeout
        if command[0] == "datasets":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="datasets version: 18.4.0\n",
                stderr="",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="UnZip 6.00 of 20 April 2009\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    check_required_tools(("datasets", "unzip"))


def test_check_required_tools_raises_for_missing_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing external tools should raise one combined preflight error."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: None)

    with pytest.raises(
        PreflightError,
        match="Missing required external tools: datasets, unzip",
    ):
        check_required_tools(("datasets", "unzip"))


def test_check_required_tools_raises_for_unsupported_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Out-of-range tool versions should fail preflight with the supported window."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return unsupported datasets and unzip versions."""

        del capture_output, text, check, timeout
        if command[0] == "datasets":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="datasets version: 18.22.0\n",
                stderr="",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="UnZip 7.00 of 20 April 2009\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Supported range: >=18.4.0,<18.22.0",
    ):
        check_required_tools(("datasets", "unzip"))


def test_check_required_tools_rejects_datasets_versions_below_supported_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Datasets versions older than 18.4.0 should fail preflight."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one pre-floor datasets version and a supported unzip version."""

        del capture_output, text, check, timeout
        if command[0] == "datasets":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="datasets version: 18.3.1\n",
                stderr="",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="UnZip 6.00 of 20 April 2009\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Supported range: >=18.4.0,<18.22.0",
    ):
        check_required_tools(("datasets", "unzip"))


def test_check_required_tools_raises_for_unparseable_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unparseable version output should fail preflight conservatively."""

    monkeypatch.setattr(shutil, "which", lambda tool_name: f"/usr/bin/{tool_name}")

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return unparsable version output for the required command."""

        del command, capture_output, text, check, timeout
        return subprocess.CompletedProcess(
            ["datasets", "version"],
            0,
            stdout="datasets version unavailable\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(
        PreflightError,
        match="Could not parse the installed version",
    ):
        check_required_tools(("datasets",))

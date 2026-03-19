"""Tests for the real-data validation bash helpers."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path


COMMON_HELPERS = Path("bin/real-data-test-common.sh").resolve()


def run_bash(script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run one bash snippet for the real-data helper tests."""

    merged_env = os.environ.copy()
    if env is not None:
        merged_env.update(env)
    return subprocess.run(
        ["bash", "-lc", script],
        capture_output=True,
        text=True,
        check=False,
        env=merged_env,
    )


def test_real_data_write_command_file_redacts_ncbi_api_key(
    tmp_path: Path,
) -> None:
    """The command evidence file should redact the NCBI API key."""

    command_file = tmp_path / "command.sh"
    secret = "abc123secret"
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export NCBI_API_KEY={shlex.quote(secret)}\n"
        "real_data_write_command_file "
        f"{shlex.quote(str(command_file))} "
        "gtdb-genomes --release 95 --ncbi-api-key \"$NCBI_API_KEY\" --output /tmp/out\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    command_text = command_file.read_text(encoding="utf-8")
    assert "REDACTED" in command_text
    assert secret not in command_text


def test_real_data_run_command_check_redacts_logs_and_records_versions(
    tmp_path: Path,
) -> None:
    """Runtime evidence should redact secrets and capture tool versions."""

    test_root = tmp_path / "suite"
    secret = "abc123secret"
    python_code = (
        "import os, sys; "
        "print(os.environ['NCBI_API_KEY']); "
        "print(os.environ['NCBI_API_KEY'], file=sys.stderr)"
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export NCBI_API_KEY={shlex.quote(secret)}\n"
        f"real_data_initialise_suite {shlex.quote(str(test_root))}\n"
        "real_data_record_tool_versions "
        f"{shlex.quote(str(test_root))} {shlex.quote(sys.executable)}\n"
        "real_data_run_command_check "
        f"{shlex.quote(str(test_root))} "
        "check1 0 "
        f"{shlex.quote(sys.executable)} -c {shlex.quote(python_code)}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    evidence_root = test_root / "_evidence" / "check1"
    command_text = (evidence_root / "command.sh").read_text(encoding="utf-8")
    stdout_text = (evidence_root / "stdout.log").read_text(encoding="utf-8")
    stderr_text = (evidence_root / "stderr.log").read_text(encoding="utf-8")
    combined_text = (evidence_root / "combined.log").read_text(encoding="utf-8")
    version_text = (test_root / "_evidence" / "tool-versions.txt").read_text(
        encoding="utf-8",
    )

    assert secret not in command_text
    assert secret not in stdout_text
    assert secret not in stderr_text
    assert secret not in combined_text
    assert "[REDACTED]" in stdout_text
    assert "[REDACTED]" in stderr_text
    assert "[REDACTED]" in combined_text
    assert "python_version=" in version_text
    assert "datasets_version=" in version_text

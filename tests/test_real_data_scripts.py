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
        "gtdb-genomes --gtdb-release 95 --ncbi-api-key \"$NCBI_API_KEY\" --outdir /tmp/out\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    command_text = command_file.read_text(encoding="utf-8")
    assert "REDACTED" in command_text
    assert secret not in command_text


def test_real_data_default_suite_root_creates_unique_directories(
    tmp_path: Path,
) -> None:
    """The default suite root helper should return unique per-run directories."""

    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export TMPDIR={shlex.quote(str(tmp_path))}\n"
        "root_one=$(real_data_default_suite_root local)\n"
        "root_two=$(real_data_default_suite_root local)\n"
        "printf '%s\\n%s\\n' \"$root_one\" \"$root_two\"\n"
        "[ \"$root_one\" != \"$root_two\" ]\n"
        "[ -d \"$root_one\" ]\n"
        "[ -d \"$root_two\" ]\n"
        "rm -rf \"$root_one\" \"$root_two\"\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    roots = result.stdout.splitlines()
    assert len(roots) == 2
    assert roots[0] != roots[1]
    assert roots[0].startswith(str(tmp_path))
    assert roots[1].startswith(str(tmp_path))


def test_real_data_detect_python_bin_falls_back_to_python3(
    tmp_path: Path,
) -> None:
    """The shared helper should detect `python3` when `python` is absent."""

    fake_bin_dir = tmp_path / "bin"
    fake_bin_dir.mkdir()
    fake_python3 = fake_bin_dir / "python3"
    fake_python3.write_text("#!/usr/bin/env bash\nprintf 'python3-bin\\n'\n", encoding="utf-8")
    fake_python3.chmod(0o755)
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export PATH={shlex.quote(str(fake_bin_dir))}\n"
        "real_data_detect_python_bin\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout.strip() == str(fake_python3)


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


def test_real_data_run_command_check_removes_raw_temp_directory(
    tmp_path: Path,
) -> None:
    """Command evidence collection should clean up its raw temp workspace."""

    test_root = tmp_path / "suite"
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir()
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export TMPDIR={shlex.quote(str(temp_dir))}\n"
        f"real_data_initialise_suite {shlex.quote(str(test_root))}\n"
        "real_data_run_command_check "
        f"{shlex.quote(str(test_root))} "
        "check1 0 "
        f"{shlex.quote(sys.executable)} -c 'print(\"ok\")'\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert not list(temp_dir.glob("gtdb_real_command.*"))


def test_remote_runner_uses_shared_defaults() -> None:
    """The remote runner should share unique-root and Python-detection helpers."""

    remote_script = Path("bin/run-real-data-tests-remote.sh").read_text(
        encoding="utf-8",
    )

    assert "real_data_default_suite_root remote" in remote_script
    assert "real_data_detect_python_bin" in remote_script
    assert "\"C0-manifest\"" in remote_script
    assert "gtdb-genomes \\" in remote_script
    assert "--gtdb-taxon g__DefinitelyNotReal" in remote_script
    assert "--dry-run" in remote_script
    assert "get_release_manifest_path" not in remote_script

"""Tests for the installed command entrypoints."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path


def test_pyproject_exposes_console_script() -> None:
    """The package metadata should expose the public console script."""

    with Path("pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    assert pyproject["project"]["scripts"]["gtdb-genomes"] == (
        "gtdb_genomes.cli:main"
    )


def test_module_entrypoint_help_runs() -> None:
    """The module entrypoint should expose the documented CLI help."""

    result = subprocess.run(
        [sys.executable, "-m", "gtdb_genomes", "--help"],
        capture_output=True,
        text=True,
        check=False,
        env={"PYTHONPATH": "src"},
    )

    assert result.returncode == 0
    assert "--release" in result.stdout
    assert "gtdb-genomes" in result.stdout


def test_installed_console_script_help_runs() -> None:
    """The environment-installed command should run without the repo wrapper."""

    console_script = Path(sys.executable).with_name("gtdb-genomes")
    result = subprocess.run(
        [str(console_script), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert console_script.is_file()
    assert result.returncode == 0
    assert "--release" in result.stdout

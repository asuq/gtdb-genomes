"""Tests for the repo-local wrapper script."""

from __future__ import annotations

import os
from pathlib import Path


def test_wrapper_exists_and_is_executable() -> None:
    """The repo-local wrapper should exist and be executable."""

    wrapper = Path("bin/gtdb-genomes")
    mode = wrapper.stat().st_mode
    assert wrapper.is_file()
    assert mode & 0o111


def test_wrapper_runs_cli_through_uv() -> None:
    """The wrapper should delegate to uv with the documented module command."""

    wrapper = Path("bin/gtdb-genomes")
    content = wrapper.read_text(encoding="ascii")

    assert "uv run" in content
    assert "--no-dev" in content
    assert "--python 3.12" in content
    assert "python -m gtdb_genomes.cli" in content
    assert '"$@"' in content

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
    assert pyproject["project"]["license"] == "MIT"


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


def test_runtime_docs_mark_uv_as_development_only() -> None:
    """The runtime docs should not present uv as an end-user requirement."""

    readme_text = Path("README.md").read_text(encoding="utf-8")
    bioconda_text = Path("packaging/bioconda/meta.yaml").read_text(
        encoding="utf-8",
    )
    notice_text = Path("NOTICE").read_text(encoding="utf-8")

    assert "development tool only" in readme_text
    assert "uv run gtdb-genomes" in readme_text
    assert "must not depend on uv at runtime" in bioconda_text
    assert "--prefer-genbank" in readme_text
    assert "one row per recorded failed attempt" in readme_text
    assert "one row per accession attempt" not in readme_text
    assert "Fixed TSV columns:" in readme_text
    assert "attempted_accession" in readme_text
    assert "img.shields.io/badge/python-" in readme_text
    assert "img.shields.io/github/v/release/asuq/gtdb-genome" in readme_text
    assert "img.shields.io/badge/license-MIT" in readme_text
    assert "> [!CAUTION]" in readme_text
    assert "PRJNA417962" in readme_text
    assert "unsupported_input" in readme_text
    assert "Real-data validation guide" in readme_text
    assert "The planned workflow is:" not in readme_text
    assert "- ncbi-datasets-cli" in bioconda_text
    assert "get_release_manifest_path" in bioconda_text
    assert ".tsv.gz" in readme_text
    assert "remains plain text by design" in readme_text
    assert "The MIT licence in this repository applies to the code" in notice_text
    assert "GTDB taxonomy data" in notice_text
    assert "license: MIT" in bioconda_text
    assert "--ncbi-api-key" in readme_text
    assert "- `--api-key`" not in readme_text
    assert "expects an NCBI API key" in readme_text
    assert "passes it only to the" in readme_text
    assert "`datasets` command" in readme_text
    assert "ncbi/datasets" in readme_text
    assert "does not download genomes directly from Python code" in readme_text
    assert "GTDB release resolution and GTDB taxonomy loading remain local" in (
        readme_text
    )


def test_real_data_validation_guide_describes_local_requirements() -> None:
    """The real-data guide should document the local runner environment split."""

    guide_text = Path("docs/real-data-validation.md").read_text(
        encoding="utf-8",
    )

    assert "uv run --no-sync gtdb-genomes" in guide_text
    assert "LOCAL_LAUNCHER_MODE=module" in guide_text
    assert "A1`, `A2`, `A3`, `A4`, `A5`, `A7`, `A8`, `A9`: `uv` only" in (
        guide_text
    )
    assert "A6`: `uv` plus `datasets`" in guide_text
    assert "B1` to `B6`: `uv`, `datasets`, and `unzip`" in guide_text
    assert "offline bundled-data dry-runs remain valid without NCBI access" in (
        guide_text
    )
    assert "debug output can print the raw API-key header" in (
        guide_text
    )
    assert "--ncbi-api-key" in guide_text

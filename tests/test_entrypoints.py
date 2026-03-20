"""Tests for the CLI entrypoints and user-facing docs."""

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
    assert "--gtdb-release" in result.stdout
    assert "--gtdb-taxon" in result.stdout
    assert "--outdir" in result.stdout
    assert "--version-fixed" in result.stdout
    assert "gtdb-genomes" in result.stdout


def test_source_checkout_cli_module_help_runs() -> None:
    """The CLI module should run against the checkout under test."""

    result = subprocess.run(
        [sys.executable, "-m", "gtdb_genomes.cli", "--help"],
        capture_output=True,
        text=True,
        check=False,
        env={"PYTHONPATH": "src"},
    )

    assert result.returncode == 0
    assert "--gtdb-release" in result.stdout
    assert "--version-fixed" in result.stdout
    assert "gtdb-genomes" in result.stdout


def test_runtime_docs_match_current_readme_and_usage_details() -> None:
    """The docs should preserve the current README and detailed usage reference."""

    readme_text = Path("README.md").read_text(encoding="utf-8")
    usage_details_text = Path("docs/usage-details.md").read_text(encoding="utf-8")
    bioconda_text = Path("packaging/bioconda/meta.yaml").read_text(
        encoding="utf-8",
    )
    notice_text = Path("NOTICE").read_text(encoding="utf-8")

    assert "Usage details" in readme_text
    assert "docs/usage-details.md" in readme_text
    assert "uv sync --group dev" in readme_text
    assert "--gtdb-release" in readme_text
    assert "--gtdb-taxon" in readme_text
    assert "--outdir" in readme_text
    assert "--release" not in readme_text
    assert "--taxon" not in readme_text
    assert "--output" not in readme_text
    assert "--no-prefer-genbank" not in readme_text
    assert "--prefer-genbank" in readme_text
    assert "--version-fixed" in readme_text
    assert "uv run gtdb-genomes" in readme_text
    assert "development tool only" in readme_text
    assert "Runtime Contract" not in readme_text
    assert "Retry Policy" not in readme_text
    assert "Output Layout" in readme_text
    assert "Summary Files" in readme_text
    assert "NCBI datasets CLI" not in readme_text

    assert "Runtime Contract" in usage_details_text
    assert "Retry Policy" in usage_details_text
    assert "NCBI datasets CLI" in usage_details_text
    assert "Bundled GTDB Taxonomy" in usage_details_text
    assert "Output Layout" in usage_details_text
    assert "Summary Files" in usage_details_text
    assert "OUTPUT/" in usage_details_text
    assert "--gtdb-release" in usage_details_text
    assert "--gtdb-taxon" in usage_details_text
    assert "--outdir" in usage_details_text
    assert "--release" not in usage_details_text
    assert "--taxon" not in usage_details_text
    assert "--output" not in usage_details_text
    assert "--no-prefer-genbank" not in usage_details_text
    assert "--version-fixed" in usage_details_text
    assert "must not depend on uv at runtime" in bioconda_text
    assert "Fixed TSV columns:" in usage_details_text
    assert "attempted_accession" in usage_details_text
    assert "img.shields.io/badge/python-" in readme_text
    assert "img.shields.io/github/v/release/asuq/gtdb-genome" in readme_text
    assert "img.shields.io/badge/license-MIT" in readme_text
    assert "> [!NOTE]" in readme_text
    assert "PRJNA417962" in readme_text
    assert "unsupported_input" in usage_details_text
    assert "Real-data validation guide" in readme_text
    assert "The planned workflow is:" not in readme_text
    assert "Bioconda recipe template" in readme_text
    assert "conda install -c bioconda" not in readme_text
    assert "- ncbi-datasets-cli" in bioconda_text
    assert "get_release_manifest_path" in bioconda_text
    assert ".tsv.gz" in usage_details_text
    assert "remains plain text by design" in usage_details_text
    assert "The MIT licence in this repository applies to the code" in notice_text
    assert "GTDB taxonomy data" in notice_text
    assert "license: MIT" in bioconda_text
    assert "--ncbi-api-key" in readme_text
    assert "- `--api-key`" not in readme_text
    assert "expects an NCBI API key" in usage_details_text
    assert "passes it only to the" in usage_details_text
    assert "`datasets` command" in usage_details_text
    assert "ncbi/datasets" in usage_details_text
    assert "does not download genomes directly from Python code" in usage_details_text
    assert "may differ from the RefSeq version" in readme_text
    assert "may differ from the RefSeq version" in usage_details_text
    assert "suffix variants are separate taxa" in readme_text
    assert "must be quoted in the shell" in readme_text
    assert "--gtdb-taxon \"s__Altiarchaeum hamiconexum\"" in readme_text
    assert "Unquoted shell input such as" in readme_text
    assert "suffix variants are separate taxa" in usage_details_text
    assert "must be quoted in the shell" in usage_details_text
    assert "--gtdb-taxon \"s__Altiarchaeum hamiconexum\"" in usage_details_text
    assert "Unquoted shell input such as" in usage_details_text
    assert "exact token passed to `datasets`" in usage_details_text
    assert "realised versioned accession" in usage_details_text
    assert "GTDB release resolution and GTDB taxonomy loading remain local" in (
        usage_details_text
    )


def test_real_data_validation_guide_describes_local_requirements() -> None:
    """The real-data guide should document the local runner environment split."""

    guide_text = Path("docs/real-data-validation.md").read_text(
        encoding="utf-8",
    )

    assert "uv run gtdb-genomes" in guide_text
    assert "LOCAL_LAUNCHER_MODE=module" in guide_text
    assert "A1`, `A2`, `A3`, `A4`, `A5`, `A7`, `A8`, `A9`: `uv` only" in (
        guide_text
    )
    assert "A6`: `uv` plus `datasets`" in guide_text
    assert "B1` to `B6`: `uv`, `datasets`, and `unzip`" in guide_text
    assert "offline bundled-data dry-runs remain valid without NCBI access" in (
        guide_text
    )
    assert "unique path such as" in guide_text
    assert "remote environment exposes `python3`" in guide_text
    assert "/tmp/gtdb-realtests/remote-YYYYMMDD-XXXXXX" in guide_text
    assert "debug output can print the raw API-key header" in (
        guide_text
    )
    assert "--ncbi-api-key" in guide_text
    assert "uv build" in guide_text
    assert "python -m pip install" in guide_text
    assert "no `uv` in the remote runtime path" in guide_text
    assert "which gtdb-genomes" in guide_text
    assert "remote `C0-manifest`" in guide_text
    assert "REMOTE_TEST_ROOT" in guide_text
    assert "case-results.tsv" in guide_text
    assert "tool-versions.txt" in guide_text

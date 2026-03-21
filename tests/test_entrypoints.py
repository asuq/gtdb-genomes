"""Tests for the CLI entrypoints and user-facing docs."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path


def assert_contains_all(text: str, snippets: tuple[str, ...]) -> None:
    """Assert that every snippet is present in one document."""

    for snippet in snippets:
        assert snippet in text


def assert_not_contains_any(text: str, snippets: tuple[str, ...]) -> None:
    """Assert that none of the snippets are present in one document."""

    for snippet in snippets:
        assert snippet not in text


def test_pyproject_exposes_console_script() -> None:
    """The package metadata should expose the public console script."""

    with Path("pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    assert pyproject["project"]["scripts"]["gtdb-genomes"] == (
        "gtdb_genomes.cli:main"
    )
    assert pyproject["project"]["license"] == "MIT AND CC-BY-SA-4.0"
    assert pyproject["project"]["license-files"] == [
        "LICENSE",
        "NOTICE",
        "licenses/CC-BY-SA-4.0.txt",
    ]


def test_pyproject_build_targets_include_runtime_package_sources() -> None:
    """The build config should ship both code and bundled taxonomy data."""

    with Path("pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    wheel_packages = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"][
        "packages"
    ]
    sdist_include = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"][
        "include"
    ]

    assert wheel_packages == ["src/gtdb_genomes"]
    assert "src/gtdb_genomes/**" in sdist_include
    assert "data/gtdb_taxonomy/**" in sdist_include


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
    """The README should stay concise while usage-details carries the contract."""

    readme_text = Path("README.md").read_text(encoding="utf-8")
    usage_details_text = Path("docs/usage-details.md").read_text(encoding="utf-8")
    assert_contains_all(
        readme_text,
        (
            "docs/usage-details.md",
            "Quick Start",
            "Command Contract",
            "Examples",
            "defaults to `latest`",
            "--prefer-genbank",
            "--threads",
            "serial in the current workflow",
        ),
    )
    assert_not_contains_any(
        readme_text,
        (
            "Runtime Contract",
            "Retry Policy",
            "Bundled GTDB Taxonomy",
            "download_method_requested",
            "--download-method",
            "Pipeline concept",
            "Step-wise development plan",
        ),
    )

    assert_contains_all(
        usage_details_text,
        (
            "Runtime Contract",
            "Retry Policy",
            "Output Layout",
            "Bundled GTDB Taxonomy",
            "NCBI datasets CLI",
            "Direct downloads remain serial in the current workflow.",
            "download_method_requested",
            "attempted_accession",
            "Defaults to `latest`",
            "--threads",
        ),
    )
    assert_not_contains_any(
        usage_details_text,
        (
            "--download-method",
            "--no-prefer-genbank",
        ),
    )

    bioconda_text = Path("packaging/bioconda/meta.yaml").read_text(
        encoding="utf-8",
    )
    notice_text = Path("NOTICE").read_text(encoding="utf-8")
    cc_by_sa_text = Path("licenses/CC-BY-SA-4.0.txt").read_text(
        encoding="utf-8",
    )
    assert_contains_all(
        bioconda_text,
        (
            "must not depend on uv at runtime",
            "--no-build-isolation",
            "- ncbi-datasets-cli",
        ),
    )
    assert_contains_all(
        notice_text,
        (
            "This repository contains two different licence regimes",
            "Genome Taxonomy Database (GTDB)",
            "creativecommons.org/licenses/by-sa/4.0/",
        ),
    )
    assert_contains_all(
        cc_by_sa_text,
        (
            "Attribution-ShareAlike 4.0 International",
            "Creative Commons Attribution-ShareAlike 4.0 International Public License",
        ),
    )


def test_bioconda_recipe_uses_real_upstream_metadata() -> None:
    """The Bioconda recipe should be pre-release-ready apart from source hash."""

    bioconda_text = Path("packaging/bioconda/meta.yaml").read_text(
        encoding="utf-8",
    )

    assert '{% set version = "0.1.0" %}' in bioconda_text
    assert "https://github.com/asuq/gtdb-genome/releases/download/" in (
        bioconda_text
    )
    assert "https://github.com/asuq/gtdb-genome" in bioconda_text
    assert "https://github.com/asuq/gtdb-genome/blob/main/README.md" in (
        bioconda_text
    )
    assert "recipe-maintainers:" in bioconda_text
    assert "- asuq" in bioconda_text
    assert "example.org" not in bioconda_text
    assert "your-org" not in bioconda_text
    assert "your-github-id" not in bioconda_text
    assert '{% set version = "0.0.0" %}' not in bioconda_text


def test_real_data_validation_guide_describes_local_requirements() -> None:
    """The real-data guide should document the local runner environment split."""

    guide_text = Path("docs/real-data-validation.md").read_text(
        encoding="utf-8",
    )

    assert_contains_all(
        guide_text,
        (
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "uv run gtdb-genomes",
            "LOCAL_LAUNCHER_MODE=module",
            "A1` to `A9`: `uv`, `datasets`, and `unzip`",
            "B1` to `B6`: `uv`, `datasets`, and `unzip`",
            "verifies each source file against the",
            "REMOTE_TEST_ROOT",
            "case-results.tsv",
            "tool-versions.txt",
            "Dry-runs now check `unzip` early",
            "--ncbi-api-key",
        ),
    )
    assert_not_contains_any(
        guide_text,
        (
            "--download-method",
            "REAL_DATA_C1_THREADS",
        ),
    )


def test_ci_workflow_runs_expected_validation_suites() -> None:
    """The main CI workflow should run the intended A, B, and C suites."""

    ci_text = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert_contains_all(
        ci_text,
        (
            "validation-a:",
            "validation-b:",
            "validation-c:",
            "uses: mamba-org/setup-micromamba@v2",
            "environment-name: gtdb-genome",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "bin/run-real-data-tests-local.sh A1 A2 A3 A4 A5 A6 A7 A8 A9",
            "bin/run-real-data-tests-local.sh B1 B2 B3 B4 B5 B6",
            "bin/run-real-data-tests-remote.sh C1 C2 C3 C4 C6",
            "uv build",
            "python -m pip install --force-reinstall dist/*.whl",
        ),
    )
    assert_not_contains_any(
        ci_text,
        (
            "bin/run-real-data-tests-remote.sh C5",
            "bin/run-real-data-tests-remote.sh C7",
            "LOCAL_LAUNCHER_MODE: module",
        ),
    )

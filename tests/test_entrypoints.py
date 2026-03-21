"""Tests for the CLI entrypoints and user-facing docs."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

import pytest


def copy_project_for_build_fixture(destination_root: Path) -> Path:
    """Copy the project into a temporary build fixture directory."""

    project_root = Path.cwd()
    fixture_root = destination_root / "project"
    shutil.copytree(
        project_root,
        fixture_root,
        ignore=shutil.ignore_patterns(
            ".git",
            ".venv",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".untracked",
            "build",
            "dist",
        ),
    )
    return fixture_root


def write_taxonomy_payload(
    payload_path: Path,
    taxonomy_text: str,
) -> tuple[str, str]:
    """Write one compressed taxonomy payload and return its integrity data."""

    payload_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(payload_path, "wb") as handle:
        handle.write(taxonomy_text.encode("utf-8"))
    sha256 = hashlib.sha256(payload_path.read_bytes()).hexdigest()
    row_count = str(
        sum(1 for line in taxonomy_text.splitlines() if line.strip()),
    )
    return sha256, row_count


def build_fixture_project(
    project_root: Path,
    output_root: Path,
) -> subprocess.CompletedProcess[str]:
    """Build one copied fixture project into a dedicated output directory."""

    uv_path = shutil.which("uv")
    if uv_path is None:
        pytest.skip("uv is required for packaging regression tests")
    environment = os.environ.copy()
    environment["UV_CACHE_DIR"] = str(project_root / ".uv-cache")
    return subprocess.run(
        [uv_path, "build", "--out-dir", str(output_root)],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )


def assert_build_result_succeeded(result: subprocess.CompletedProcess[str]) -> None:
    """Assert that one fixture build succeeded or skip on local offline limits."""

    if result.returncode == 0:
        return
    if (
        os.environ.get("CI") != "true"
        and "Failed to resolve requirements from `build-system.requires`"
        in result.stderr
    ):
        pytest.skip("uv build cannot resolve hatchling in this local offline shell")
    pytest.fail(
        "uv build failed\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}",
    )


def archive_members_with_fragment(archive_path: Path, fragment: str) -> set[str]:
    """Return archive members that contain one selected path fragment."""

    if archive_path.suffix == ".whl":
        with zipfile.ZipFile(archive_path) as handle:
            return {
                member_name
                for member_name in handle.namelist()
                if fragment in member_name
            }
    with tarfile.open(archive_path, "r:gz") as handle:
        return {
            member_name
            for member_name in handle.getnames()
            if fragment in member_name
        }


def read_wheel_member_text(wheel_path: Path, member_name: str) -> str:
    """Return one wheel member decoded as UTF-8 text."""

    with zipfile.ZipFile(wheel_path) as handle:
        return handle.read(member_name).decode("utf-8")


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
    wheel_force_include = pyproject["tool"]["hatch"]["build"]["targets"][
        "wheel"
    ]["force-include"]
    sdist_include = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"][
        "include"
    ]
    sdist_artifacts = pyproject["tool"]["hatch"]["build"]["targets"]["sdist"][
        "artifacts"
    ]

    assert wheel_packages == ["src/gtdb_genomes"]
    assert wheel_force_include["data/gtdb_taxonomy"] == (
        "gtdb_genomes/data/gtdb_taxonomy"
    )
    assert "src/gtdb_genomes/**" in sdist_include
    assert "data/gtdb_taxonomy/**" in sdist_artifacts
    assert "data/gtdb_taxonomy/**" not in sdist_include


def test_uv_build_includes_generated_taxonomy_payloads_in_sdist_and_wheel(
    tmp_path: Path,
) -> None:
    """A build should ship generated taxonomy payloads in both artifacts."""

    fixture_root = copy_project_for_build_fixture(tmp_path)
    taxonomy_root = fixture_root / "data" / "gtdb_taxonomy" / "999.0"
    bacterial_payload = taxonomy_root / "bac120_taxonomy_r999.tsv.gz"
    archaeal_payload = taxonomy_root / "ar53_taxonomy_r999.tsv.gz"
    bacterial_sha256, bacterial_rows = write_taxonomy_payload(
        bacterial_payload,
        "GB_GCA_999999.1\td__Bacteria;g__Syntheticus\n",
    )
    archaeal_sha256, archaeal_rows = write_taxonomy_payload(
        archaeal_payload,
        "GB_GCA_999998.1\td__Archaea;g__Syntheticus\n",
    )
    (fixture_root / "data" / "gtdb_taxonomy" / "releases.tsv").write_text(
        (
            "resolved_release\taliases\tbacterial_taxonomy\tarchaeal_taxonomy\t"
            "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
            "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest\t"
            "source_root_url\tchecksum_filename\n"
            "999.0\t999,999.0,latest\tbac120_taxonomy_r999.tsv.gz\t"
            "ar53_taxonomy_r999.tsv.gz\t"
            f"{bacterial_sha256}\t{archaeal_sha256}\t"
            f"{bacterial_rows}\t{archaeal_rows}\ttrue\t"
            "https://example.org/999\tMD5SUM.txt\n"
        ),
        encoding="utf-8",
    )

    dist_root = tmp_path / "dist"
    build_result = build_fixture_project(fixture_root, dist_root)
    assert_build_result_succeeded(build_result)

    sdist_path = next(dist_root.glob("*.tar.gz"))
    wheel_path = next(dist_root.glob("*.whl"))

    sdist_members = archive_members_with_fragment(
        sdist_path,
        "data/gtdb_taxonomy/999.0/",
    )
    wheel_members = archive_members_with_fragment(
        wheel_path,
        "gtdb_genomes/data/gtdb_taxonomy/999.0/",
    )

    assert any(
        member_name.endswith("data/gtdb_taxonomy/999.0/bac120_taxonomy_r999.tsv.gz")
        for member_name in sdist_members
    )
    assert any(
        member_name.endswith("data/gtdb_taxonomy/999.0/ar53_taxonomy_r999.tsv.gz")
        for member_name in sdist_members
    )
    assert (
        "gtdb_genomes/data/gtdb_taxonomy/999.0/bac120_taxonomy_r999.tsv.gz"
        in wheel_members
    )
    assert (
        "gtdb_genomes/data/gtdb_taxonomy/999.0/ar53_taxonomy_r999.tsv.gz"
        in wheel_members
    )
    build_info = json.loads(
        read_wheel_member_text(wheel_path, "gtdb_genomes/_build_info.json"),
    )
    assert build_info["package_version"] == "0.1.0"
    assert "git_revision" in build_info


def test_uv_build_rejects_manifest_only_source_fixture(tmp_path: Path) -> None:
    """A source build should fail clearly when the payload is not bootstrapped."""

    fixture_root = copy_project_for_build_fixture(tmp_path)

    dist_root = tmp_path / "dist"
    build_result = build_fixture_project(fixture_root, dist_root)

    assert build_result.returncode != 0
    assert "Bundled GTDB taxonomy payload is not ready for packaging." in (
        build_result.stderr
    )
    assert "bootstrap_taxonomy" in build_result.stderr


def test_clean_runtime_wheel_install_validates_bundled_latest_release(
    tmp_path: Path,
) -> None:
    """A built wheel should validate bundled data without `uv` on `PATH`."""

    fixture_root = copy_project_for_build_fixture(tmp_path)
    taxonomy_root = fixture_root / "data" / "gtdb_taxonomy" / "999.0"
    bacterial_payload = taxonomy_root / "bac120_taxonomy_r999.tsv.gz"
    archaeal_payload = taxonomy_root / "ar53_taxonomy_r999.tsv.gz"
    bacterial_sha256, bacterial_rows = write_taxonomy_payload(
        bacterial_payload,
        "GB_GCA_999999.1\td__Bacteria;g__Syntheticus\n",
    )
    archaeal_sha256, archaeal_rows = write_taxonomy_payload(
        archaeal_payload,
        "GB_GCA_999998.1\td__Archaea;g__Syntheticus\n",
    )
    (fixture_root / "data" / "gtdb_taxonomy" / "releases.tsv").write_text(
        (
            "resolved_release\taliases\tbacterial_taxonomy\tarchaeal_taxonomy\t"
            "bacterial_taxonomy_sha256\tarchaeal_taxonomy_sha256\t"
            "bacterial_taxonomy_rows\tarchaeal_taxonomy_rows\tis_latest\t"
            "source_root_url\tchecksum_filename\n"
            "999.0\t999,999.0,latest\tbac120_taxonomy_r999.tsv.gz\t"
            "ar53_taxonomy_r999.tsv.gz\t"
            f"{bacterial_sha256}\t{archaeal_sha256}\t"
            f"{bacterial_rows}\t{archaeal_rows}\ttrue\t"
            "https://example.org/999\tMD5SUM.txt\n"
        ),
        encoding="utf-8",
    )

    dist_root = tmp_path / "dist"
    build_result = build_fixture_project(fixture_root, dist_root)
    assert_build_result_succeeded(build_result)

    wheel_path = next(dist_root.glob("*.whl"))
    venv_root = tmp_path / "runtime-venv"
    subprocess.run(
        [sys.executable, "-m", "venv", str(venv_root)],
        check=True,
        capture_output=True,
        text=True,
    )
    pip_bin = venv_root / "bin" / "pip"
    python_bin = venv_root / "bin" / "python"
    runtime_env = {
        **os.environ,
        "PATH": f"{venv_root / 'bin'}:/usr/bin:/bin",
    }
    install_result = subprocess.run(
        [
            str(pip_bin),
            "install",
            "--no-deps",
            "--force-reinstall",
            str(wheel_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=runtime_env,
    )
    assert install_result.returncode == 0, install_result.stderr

    runtime_result = subprocess.run(
        [
            str(python_bin),
            "-c",
            (
                "import shutil; "
                "assert shutil.which('uv') is None; "
                "from gtdb_genomes.release_resolver import "
                "resolve_and_validate_release; "
                "resolution = resolve_and_validate_release('latest'); "
                "assert resolution.resolved_release == '999.0'; "
                "assert resolution.bacterial_taxonomy is not None"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=runtime_env,
    )
    assert runtime_result.returncode == 0, runtime_result.stderr


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
    assert "--version-latest" in result.stdout
    assert "--version-fixed" not in result.stdout
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
    assert "--version-latest" in result.stdout
    assert "--version-fixed" not in result.stdout
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
            "Command options",
            "Examples",
            "gtdb release number, defaults to `latest`",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "refresh_taxonomy_manifest",
            "--version-latest",
            "keep the exact selected version",
            "--prefer-genbank",
            "--threads",
            'prints the planned download list without downloading',
            '--gtdb-taxon "p__Pseudomonadota" "c__Alphaproteobacteria"',
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
            "refresh_taxonomy_manifest",
            "--version-latest",
            "exact selected versioned accession",
            "MD5SUM",
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
            "resolve_and_validate_release('latest')",
        ),
    )
    assert_contains_all(
        notice_text,
        (
            "This repository contains two different licence regimes",
            "Genome Taxonomy Database (GTDB)",
            "bootstrap mirror",
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
            "--gtdb-release 226",
            "226 / s__Thermoflexus hugenholtzii",
            "226 / g__Methanobrevibacter",
            "verifies each source file against the",
            "REMOTE_TEST_ROOT",
            "case-results.tsv",
            "tool-versions.txt",
            "Dry-runs preflight `unzip` early",
            "C5",
            "C7",
            "`NCBI_API_KEY` for `C7`",
            "`NCBI_API_KEY` for `C2`, `C3`, and `C5`",
            "packaged-runtime `C` coverage is split into separate build and runtime",
            "no `uv` on `PATH`",
        ),
    )
    assert_not_contains_any(
        guide_text,
        (
            "--download-method",
            "REAL_DATA_C1_THREADS",
            "latest / s__Thermoflexus hugenholtzii",
            "latest / g__Methanobrevibacter",
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
            "validation-c-build:",
            "validation-c-runtime:",
            "uses: mamba-org/setup-micromamba@v2",
            "environment-name: gtdb-genome",
            "- \"3.13\"",
            "- \"3.14\"",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "bin/run-real-data-tests-local.sh A1 A2 A3 A4 A5 A6 A7 A8 A9",
            "bin/run-real-data-tests-local.sh B1 B2 B3 B4 B5 B6",
            "bin/run-real-data-tests-remote.sh C1 C2 C3 C4 C5 C6",
            "uv build",
            "actions/download-artifact@v5",
            "python -m pip install --force-reinstall dist/*.whl",
            "shutil.which('uv') is None",
            "resolve_and_validate_release('latest')",
        ),
    )
    assert_not_contains_any(
        ci_text,
        (
            "bin/run-real-data-tests-remote.sh C7",
            "LOCAL_LAUNCHER_MODE: module",
        ),
    )


def test_release_workflow_enforces_build_then_clean_runtime() -> None:
    """The release workflow should split build and packaged runtime validation."""

    release_text = Path(".github/workflows/release.yml").read_text(
        encoding="utf-8",
    )

    assert_contains_all(
        release_text,
        (
            "build-artifacts:",
            "runtime-validation:",
            "workflow_dispatch:",
            "push:",
            "tags:",
            "uv build",
            "actions/upload-artifact@v6",
            "actions/download-artifact@v5",
            "python -m pip install --force-reinstall dist/*.whl",
            "shutil.which('uv') is None",
            "bin/run-real-data-tests-remote.sh C1 C2 C3 C4 C5 C6",
        ),
    )


def test_live_validation_workflow_bootstraps_before_b1() -> None:
    """The live validation workflow should bootstrap taxonomy before B1."""

    live_text = Path(".github/workflows/live-validation.yml").read_text(
        encoding="utf-8",
    )

    assert_contains_all(
        live_text,
        (
            "validation-live:",
            "uv sync --locked --group dev",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "bin/run-real-data-tests-local.sh B1",
            "LOCAL_LAUNCHER_MODE: module",
        ),
    )

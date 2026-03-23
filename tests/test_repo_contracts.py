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

from gtdb_genomes.preflight import SUPPORTED_TOOL_VERSIONS


def get_venv_scripts_directory(venv_root: Path) -> Path:
    """Return the script directory for one virtual environment."""

    if sys.platform == "win32":
        return venv_root / "Scripts"
    return venv_root / "bin"


def get_venv_command_path(venv_root: Path, command_name: str) -> Path:
    """Return the platform-specific path for one virtualenv command."""

    scripts_directory = get_venv_scripts_directory(venv_root)
    if sys.platform == "win32":
        return scripts_directory / f"{command_name}.exe"
    return scripts_directory / command_name


def build_runtime_path_environment(venv_root: Path) -> dict[str, str]:
    """Return a PATH that exposes the virtualenv but excludes ambient `uv`."""

    path_entries = [str(get_venv_scripts_directory(venv_root))]
    if sys.platform == "win32":
        system_root = os.environ.get("SystemRoot")
        if system_root:
            path_entries.extend([system_root, str(Path(system_root) / "System32")])
    else:
        path_entries.extend(["/usr/bin", "/bin"])
    return {
        **os.environ,
        "PATH": os.pathsep.join(path_entries),
    }


def copy_manifest_only_project_fixture(destination_root: Path) -> Path:
    """Copy the project and strip any bootstrapped bundled taxonomy payloads."""

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
    taxonomy_root = fixture_root / "data" / "gtdb_taxonomy"
    if taxonomy_root.exists():
        for child_path in taxonomy_root.iterdir():
            if child_path.name == "releases.tsv":
                continue
            if child_path.is_dir():
                shutil.rmtree(child_path)
            else:
                child_path.unlink()
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


def markdown_level_two_section(document_text: str, heading: str) -> str:
    """Return the body for one level-two Markdown section."""

    heading_marker = f"## {heading}\n"
    start_index = document_text.index(heading_marker) + len(heading_marker)
    end_index = document_text.find("\n## ", start_index)
    if end_index == -1:
        return document_text[start_index:]
    return document_text[start_index:end_index]


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

    fixture_root = copy_manifest_only_project_fixture(tmp_path)
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
    inspect_result = subprocess.run(
        [
            sys.executable,
            str(fixture_root / "bin" / "inspect_built_artifacts.py"),
            str(dist_root),
        ],
        cwd=fixture_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert inspect_result.returncode == 0, inspect_result.stderr


def test_uv_build_rejects_manifest_only_source_fixture(tmp_path: Path) -> None:
    """A source build should fail clearly when the payload is not bootstrapped."""

    fixture_root = copy_manifest_only_project_fixture(tmp_path)

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

    fixture_root = copy_manifest_only_project_fixture(tmp_path)
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
    pip_bin = get_venv_command_path(venv_root, "pip")
    python_bin = get_venv_command_path(venv_root, "python")
    runtime_env = build_runtime_path_environment(venv_root)
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
    installation_text = markdown_level_two_section(readme_text, "Installation")
    development_text = markdown_level_two_section(
        readme_text,
        "Development And Packaging",
    )
    assert_contains_all(
        readme_text,
        (
            "docs/usage-details.md",
            "Quick Start",
            "Command options",
            "Examples",
            "https://github.com/asuq/gtdb-genomes/actions/workflows/ci.yml/badge.svg",
            (
                "[![Pytest: Linux | macOS | Windows]"
                "(https://img.shields.io/badge/pytest-Linux%20%7C%20macOS%20%7C%20Windows-4c8eda.svg)]"
                "(https://github.com/asuq/gtdb-genomes/actions/workflows/ci.yml)"
            ),
            (
                "https://github.com/asuq/gtdb-genomes/actions/workflows/"
                "live-validation.yml/badge.svg"
            ),
            (
                "[![CITATION.cff]"
                "(https://img.shields.io/badge/CITATION-cff-blue.svg)]"
                "(https://github.com/asuq/gtdb-genomes/blob/main/CITATION.cff)"
            ),
            "gtdb release number, defaults to `latest`",
            "refresh_taxonomy_manifest",
            "--version-latest",
            "keeps the exact selected version",
            "--prefer-genbank",
            "--threads",
            "resolves inputs without creating the final output tree",
            "Operational Notes And Limitations",
            "exact-token and case-sensitive",
            "Automatic planning switches to `dehydrate` at 1,000 or more unique `datasets`",
            "The planner intentionally stays count-only for this project.",
            "Direct downloads remain serial in the current workflow.",
            "consult current NCBI metadata",
            "cannot be combined with an effective NCBI API key",
            "`genome`, `gff3`, and `protein`",
            "`ncbi-datasets-cli >=18.4.0,<18.22.0`",
            "`unzip >=6.0,<7.0`",
            "The CLI checks these versions during preflight",
            "first public Bioconda release is pending a tagged source release",
            "draft template",
            "pytest matrix runs on Linux, macOS, and Windows",
            "Clean packaged-runtime",
            "real-data validation currently run on Linux",
            "uv sync --group dev",
            "draft Bioconda recipe template",
            "packaging/bioconda/meta.yaml.template",
            "quarantined until a tagged release archive and final SHA256 are available",
            "polars >=1.31.0,<2.0.0",
            '--gtdb-taxon "p__Pseudomonadota" "c__Alphaproteobacteria"',
        ),
    )
    assert_contains_all(
        installation_text,
        (
            "pending a tagged source release",
            "draft template",
            "`polars >=1.31.0,<2.0.0`",
            "`ncbi-datasets-cli >=18.4.0,<18.22.0`",
            "`unzip >=6.0,<7.0`",
            "packaged-runtime",
            "real-data validation currently run on Linux",
            "source-checkout workflow in Development And Packaging below",
        ),
    )
    assert_not_contains_any(
        installation_text,
        (
            "mamba create -n gtdb-genomes -c conda-forge -c bioconda",
            "mamba activate gtdb-genomes",
            "gtdb-genomes --help",
            "uv sync --group dev",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "uv run gtdb-genomes --help",
        ),
    )
    assert_contains_all(
        development_text,
        (
            "uv sync --group dev",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "uv run gtdb-genomes --help",
            "packaged wheel and sdist validation in CI on Linux",
            "data/gtdb_taxonomy/releases.tsv",
            "MD5SUM",
            "refresh_taxonomy_manifest",
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
            "tracks the tagged sdist metadata",
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
            "version_latest",
            "package_version",
            "git_revision",
            "datasets_version",
            "unzip_version",
            "release_manifest_sha256",
            "bacterial_taxonomy_sha256",
            "archaeal_taxonomy_sha256",
                    "attempted_accession",
                    "download_request_accession",
                    "Defaults to `latest`",
                    "refresh_taxonomy_manifest",
                    "--version-latest",
                    "current NCBI",
                    "GTDB-release-preserving transform",
                "first uses explicit",
                "paired-assembly metadata from the RefSeq summary record",
                "candidate metadata lookup fails or stays incomplete",
                "1,000 or more",
                "generic `datasets` `> 15 GB` heuristic",
            "planning or runtime failure with no successful genomes",
            "local final-output materialisation failure",
            "MD5SUM",
            "--threads",
            "child process environment",
            "Ambient `NCBI_API_KEY` is the normal workflow path",
            "forbids `--debug` while an effective NCBI API key is active",
        ),
    )
    assert_not_contains_any(usage_details_text, ("--download-method", "--no-prefer-genbank"))
    datasets_policy = SUPPORTED_TOOL_VERSIONS["datasets"]
    unzip_policy = SUPPORTED_TOOL_VERSIONS["unzip"]
    assert f"`{datasets_policy.display_name} {datasets_policy.supported_range}`" in (
        readme_text
    )
    assert f"`{unzip_policy.display_name} {unzip_policy.supported_range}`" in (
        readme_text
    )
    assert f"`{datasets_policy.display_name} {datasets_policy.supported_range}`" in (
        Path("docs/real-data-validation.md").read_text(encoding="utf-8")
    )
    assert f"`{unzip_policy.display_name} {unzip_policy.supported_range}`" in (
        Path("docs/real-data-validation.md").read_text(encoding="utf-8")
    )
    assert "REAL_DATA_DEBUG_SAFE=1" in Path(
        "docs/real-data-validation.md",
    ).read_text(encoding="utf-8")

    bioconda_text = Path("packaging/bioconda/meta.yaml.template").read_text(
        encoding="utf-8",
    )
    bioconda_readme_text = Path("packaging/bioconda/README.md").read_text(
        encoding="utf-8",
    )
    notice_text = Path("NOTICE").read_text(encoding="utf-8")
    cc_by_sa_text = Path("licenses/CC-BY-SA-4.0.txt").read_text(
        encoding="utf-8",
    )
    assert_contains_all(
        bioconda_text,
        (
            "--no-build-isolation",
            "hatchling >=1.27.0,<2.0.0",
            "polars >=1.31.0,<2.0.0",
            "- ncbi-datasets-cli",
            "resolve_and_validate_release('latest')",
            "load_release_taxonomy",
            "g__NoSuchTaxon",
            "'--dry-run'",
        ),
    )
    assert_contains_all(
        bioconda_readme_text,
        (
            "meta.yaml.template",
            "draft recipe template",
            "quarantined",
            "final `sha256`",
            "polars >=1.31.0,<2.0.0",
            "bundled taxonomy loading",
            "offline zero-match dry-run path",
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


def test_bioconda_recipe_template_is_quarantined_until_release_metadata_exists() -> None:
    """The Bioconda draft recipe should stay quarantined until release metadata exists."""

    datasets_policy = SUPPORTED_TOOL_VERSIONS["datasets"]
    unzip_policy = SUPPORTED_TOOL_VERSIONS["unzip"]
    bioconda_template_path = Path("packaging/bioconda/meta.yaml.template")
    bioconda_text = bioconda_template_path.read_text(
        encoding="utf-8",
    )
    bioconda_readme_text = Path("packaging/bioconda/README.md").read_text(
        encoding="utf-8",
    )

    assert not Path("packaging/bioconda/meta.yaml").exists()
    assert bioconda_template_path.is_file()
    assert '{% set version = "0.1.0" %}' in bioconda_text
    assert "https://github.com/asuq/gtdb-genomes/releases/download/" in (
        bioconda_text
    )
    assert (
        "Fill these from the tagged GitHub release after the first public "
        "release"
    ) in bioconda_text
    assert "meta.yaml.template" in bioconda_readme_text
    assert "quarantined" in bioconda_readme_text
    assert "https://github.com/asuq/gtdb-genomes" in bioconda_text
    assert "https://github.com/asuq/gtdb-genomes/blob/main/README.md" in (
        bioconda_text
    )
    assert "hatchling >=1.27.0,<2.0.0" in bioconda_text
    assert "polars >=1.31.0,<2.0.0" in bioconda_text
    assert "g__NoSuchTaxon" in bioconda_text
    assert "--dry-run" in bioconda_text
    assert f"- {unzip_policy.display_name} {unzip_policy.supported_range}" in (
        bioconda_text
    )
    assert f"- {datasets_policy.display_name} {datasets_policy.supported_range}" in (
        bioconda_text
    )
    assert "recipe-maintainers:" in bioconda_text
    assert "- asuq" in bioconda_text
    assert (
        "sha256: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        in bioconda_text
    )
    assert "example.org" not in bioconda_text
    assert "your-org" not in bioconda_text
    assert "your-github-id" not in bioconda_text
    assert '{% set version = "0.0.0" %}' not in bioconda_text
    assert_not_contains_any(
        bioconda_text,
        (
            "https://github.com/asuq/gtdb-genome/releases/download/",
            "https://github.com/asuq/gtdb-genome/blob/main/README.md",
            "https://github.com/asuq/gtdb-genome\n",
            "run_exports:",
            "???",
        ),
    )


def test_citation_file_uses_canonical_release_metadata() -> None:
    """The citation metadata should match the tracked software release identity."""

    citation_text = Path("CITATION.cff").read_text(encoding="utf-8")

    assert_contains_all(
        citation_text,
        (
            "cff-version: 1.2.0",
            'title: "gtdb-genomes"',
            'version: "0.1.0"',
            "date-released: 2026-03-22",
            "repository-code: 'https://github.com/asuq/gtdb-genomes'",
            'family-names: "Shima"',
            'given-names: "Akito"',
            "orcid: 'https://orcid.org/0000-0002-7092-7720'",
            "abstract: CLI to download genomes by GTDB taxon and GTDB release",
            "# identifiers:",
            "#   - type: doi",
            "#     value: xx.yyyy/zenodo.zzzz",
        ),
    )


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
            "`NCBI_API_KEY` for `C2` and `C3`",
            "C5` runs without `NCBI_API_KEY` and uses it opportunistically",
            "leaves `NCBI_API_KEY` ambient",
            "Required environment for `full-large` coverage:",
            "packaged-runtime `C` coverage is split into separate build and runtime",
            "validates both the wheel and `sdist`",
            "no `uv` on `PATH`",
            "`ncbi-datasets-cli >=18.4.0,<18.22.0`",
            "`unzip >=6.0,<7.0`",
            "ncbi-datasets-cli=18.4.0",
            "ncbi-datasets-cli=18.21.0",
            "-c conda-forge -c bioconda",
            "unzip=6.0",
            "load_release_taxonomy()",
        ),
    )
    assert_not_contains_any(
        guide_text,
        (
            "--download-method",
            "REAL_DATA_C1_THREADS",
            "latest / s__Thermoflexus hugenholtzii",
            "latest / g__Methanobrevibacter",
            "`NCBI_API_KEY` for `C5`",
            "passes `NCBI_API_KEY` to the CLI as `--ncbi-api-key`",
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
            "bash bin/install-micromamba-ci.sh",
            "micromamba create -y -n gtdb-genome",
            "python=3.12 uv pip",
            "windows-latest",
            "- \"3.13\"",
            "- \"3.14\"",
            "ncbi-datasets-cli=18.4.0",
            "ncbi-datasets-cli=18.21.0",
            "unzip=6.0",
            "uv run pytest -q",
            "micromamba run -n gtdb-genome",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "bin/run-real-data-tests-local.sh A1 A2 A3 A4 A5 A6 A7 A8 A9",
            "bin/run-real-data-tests-local.sh B1 B2 B3 B4 B5 B6",
            "bin/run-real-data-tests-remote.sh C1 C2 C3 C4 C5 C6",
            "uv build",
            "python bin/inspect_built_artifacts.py dist",
            "actions/download-artifact@v7",
            "micromamba run -n gtdb-genome-runtime",
            "python -m pip install --force-reinstall dist/*.whl",
            "shutil.which('uv') is None",
            "load_release_taxonomy(resolution)",
        ),
    )
    assert_not_contains_any(
        ci_text,
        (
            "bin/run-real-data-tests-remote.sh C7",
            "LOCAL_LAUNCHER_MODE: module",
            "mamba-org/setup-micromamba@v2",
            "actions/download-artifact@v5",
            ".venv/bin/pytest -q",
        ),
    )


def test_ci_micromamba_helper_is_pinned() -> None:
    """The shared micromamba installer should stay pinned and verified."""

    helper_text = Path("bin/install-micromamba-ci.sh").read_text(
        encoding="utf-8",
    )

    assert_contains_all(
        helper_text,
        (
            'MICROMAMBA_VERSION="2.3.3-0"',
            'MICROMAMBA_SHA256="9496f94a8b78c536573c93d946ec9bba74bd9ff79ee55aaa4b546e30db8f511b"',
            "micromamba-linux-64",
            "GITHUB_PATH",
            "GITHUB_ENV",
            "MAMBA_ROOT_PREFIX",
            "sha256sum -c -",
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
            "wheel-runtime-validation:",
            "sdist-runtime-validation:",
            "workflow_dispatch:",
            "push:",
            "tags:",
            "uv build",
            "python bin/inspect_built_artifacts.py dist",
            "actions/upload-artifact@v6",
            "actions/download-artifact@v7",
            "bash bin/install-micromamba-ci.sh",
            "micromamba create -y -n gtdb-genome-release",
            "python=3.12 pip",
            "micromamba run -n gtdb-genome-release",
            "python -m pip install --force-reinstall dist/*.whl",
            "micromamba create -y -n gtdb-genome-release-sdist",
            "python=3.12 pip hatchling polars",
            "micromamba run -n gtdb-genome-release-sdist",
            "python -m pip install --force-reinstall --no-deps",
            "--no-build-isolation dist/*.tar.gz",
            "shutil.which('uv') is None",
            "load_release_taxonomy(resolution)",
            "bin/run-real-data-tests-remote.sh C1 C2 C3 C4 C5 C6",
            "publish:",
            "needs: [wheel-runtime-validation, sdist-runtime-validation]",
            "gh release view",
            "gh release upload",
            "--clobber",
            "gh release create",
            "--verify-tag",
        ),
    )
    assert_not_contains_any(
        release_text,
        (
            "mamba-org/setup-micromamba@v2",
            "actions/download-artifact@v5",
            "softprops/action-gh-release@v2",
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
            "bash bin/install-micromamba-ci.sh",
            "micromamba create -y -n ci-live",
            "python=3.12 uv",
            "micromamba run -n ci-live",
            "uv sync --locked --group dev",
            "uv run python -m gtdb_genomes.bootstrap_taxonomy",
            "bin/run-real-data-tests-local.sh B1",
            "LOCAL_LAUNCHER_MODE: module",
            "ncbi-datasets-cli=18.21.0",
            "unzip=6.0",
        ),
    )
    assert_not_contains_any(
        live_text,
        (
            "mamba-org/setup-micromamba@v2",
        ),
    )

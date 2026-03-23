"""Stubbed end-to-end tests at the CLI boundary."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import CliArgs, main


def test_main_passes_normalised_arguments_into_workflow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The CLI should hand normalised arguments to the workflow runner."""

    captured_args: list[CliArgs] = []

    def fake_run_workflow(args: CliArgs) -> int:
        """Capture the parsed arguments and return a stubbed exit code."""

        captured_args.append(args)
        return 6

    monkeypatch.setattr("gtdb_genomes.workflow.run_workflow", fake_run_workflow)

    exit_code = main(
        [
            "--gtdb-release",
            " latest ",
            "--gtdb-taxon",
            " g__Escherichia ",
            " s__Escherichia coli ",
            "--gtdb-taxon",
            "g__Escherichia",
            "g__Bacillus",
            "--outdir",
            str(tmp_path / "output"),
            "--threads",
            "3",
            "--include",
            " genome , gff3 ",
            "--debug",
            "--dry-run",
        ],
    )

    assert exit_code == 6
    assert captured_args == [
        CliArgs(
            gtdb_release="latest",
            gtdb_taxa=(
                "g__Escherichia",
                "s__Escherichia coli",
                "g__Bacillus",
            ),
            outdir=tmp_path / "output",
            prefer_genbank=False,
            version_latest=False,
            threads=3,
            ncbi_api_key=None,
            include="genome,gff3",
            debug=True,
            keep_temp=False,
            dry_run=True,
        ),
    ]


def test_main_rejects_shell_split_species_input_under_multi_value_taxa(
    tmp_path: Path,
) -> None:
    """Shell-split species input should still fail under the new parser form."""

    with pytest.raises(SystemExit) as error:
        main(
            [
                "--gtdb-taxon",
                "s__Altiarchaeum",
                "hamiconexum",
                "--outdir",
                str(tmp_path / "output"),
            ],
        )

    assert error.value.code == 2


def test_main_defaults_release_to_latest_when_flag_is_omitted(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The CLI should pass the bundled latest alias when release is omitted."""

    captured_args: list[CliArgs] = []

    def fake_run_workflow(args: CliArgs) -> int:
        """Capture the parsed arguments and return a stubbed exit code."""

        captured_args.append(args)
        return 0

    monkeypatch.setattr("gtdb_genomes.workflow.run_workflow", fake_run_workflow)

    exit_code = main(
        [
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path / "output"),
        ],
    )

    assert exit_code == 0
    assert captured_args == [
        CliArgs(
            gtdb_release="latest",
            gtdb_taxa=("g__Escherichia",),
            outdir=tmp_path / "output",
            prefer_genbank=False,
            version_latest=False,
            threads=8,
            ncbi_api_key=None,
            include="genome",
            debug=False,
            keep_temp=False,
            dry_run=False,
        ),
    ]


def test_main_passes_version_latest_into_workflow(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """The CLI should pass the explicit latest-version mode into the workflow."""

    captured_args: list[CliArgs] = []

    def fake_run_workflow(args: CliArgs) -> int:
        """Capture the parsed arguments and return a stubbed exit code."""

        captured_args.append(args)
        return 0

    monkeypatch.setattr("gtdb_genomes.workflow.run_workflow", fake_run_workflow)

    exit_code = main(
        [
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path / "output"),
            "--prefer-genbank",
            "--version-latest",
        ],
    )

    assert exit_code == 0
    assert captured_args == [
        CliArgs(
            gtdb_release="latest",
            gtdb_taxa=("g__Escherichia",),
            outdir=tmp_path / "output",
            prefer_genbank=True,
            version_latest=True,
            threads=8,
            ncbi_api_key=None,
            include="genome",
            debug=False,
            keep_temp=False,
            dry_run=False,
        ),
    ]


def test_main_rejects_debug_with_ncbi_api_key(tmp_path: Path) -> None:
    """The CLI boundary should reject the unsafe debug and API-key mix."""

    with pytest.raises(SystemExit) as error:
        main(
            [
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
                str(tmp_path / "output"),
                "--ncbi-api-key",
                "secret",
                "--debug",
            ],
        )

    assert error.value.code == 2

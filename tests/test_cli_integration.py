"""Stubbed end-to-end tests at the CLI boundary."""

from __future__ import annotations

from pathlib import Path

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

    monkeypatch.setattr("gtdb_genomes.cli.run_workflow", fake_run_workflow)

    exit_code = main(
        [
            "--gtdb-release",
            " latest ",
            "--gtdb-taxon",
            " g__Escherichia ",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path / "output"),
            "--download-method",
            "direct",
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
            gtdb_taxa=("g__Escherichia",),
            outdir=tmp_path / "output",
            prefer_genbank=False,
            version_fixed=False,
            download_method="direct",
            threads=3,
            ncbi_api_key=None,
            include="genome,gff3",
            debug=True,
            keep_temp=False,
            dry_run=True,
        ),
    ]

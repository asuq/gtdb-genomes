"""Tests for the Phase 1 command-line interface."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import CliArgs, build_parser, main, parse_args
from gtdb_genomes.preflight import PreflightError


def test_help_includes_documented_flags() -> None:
    """The parser help should include the documented Phase 1 flags."""

    help_text = build_parser().format_help()
    assert "--release" in help_text
    assert "--taxon" in help_text
    assert "--output" in help_text
    assert "--prefer-gca" in help_text
    assert "--download-method" in help_text
    assert "--threads" in help_text
    assert "--api-key" in help_text
    assert "--include" in help_text
    assert "--debug" in help_text
    assert "--keep-temp" in help_text
    assert "--dry-run" in help_text


def test_parse_args_normalises_and_deduplicates_taxa(tmp_path: Path) -> None:
    """Repeated taxa should be trimmed and deduplicated in order."""

    parser = build_parser()
    args = parse_args(
        parser,
        [
            "--release",
            " latest ",
            "--taxon",
            " g__Escherichia ",
            "--taxon",
            "g__Escherichia",
            "--taxon",
            " s__Escherichia coli ",
            "--output",
            str(tmp_path),
        ],
    )

    assert isinstance(args, CliArgs)
    assert args.release == "latest"
    assert args.taxa == ("g__Escherichia", "s__Escherichia coli")


def test_parse_args_rejects_blank_release(tmp_path: Path) -> None:
    """Blank release values should fail validation."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--release",
                " ",
                "--taxon",
                "g__Escherichia",
                "--output",
                str(tmp_path),
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_blank_taxon(tmp_path: Path) -> None:
    """Blank taxon values should fail validation."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--release",
                "latest",
                "--taxon",
                " ",
                "--output",
                str(tmp_path),
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_non_positive_threads(tmp_path: Path) -> None:
    """Thread counts must be positive integers."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--release",
                "latest",
                "--taxon",
                "g__Escherichia",
                "--output",
                str(tmp_path),
                "--threads",
                "0",
            ],
        )
    assert error.value.code == 2


def test_parse_args_requires_genome_in_include(tmp_path: Path) -> None:
    """Include values without genome should fail validation."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--release",
                "latest",
                "--taxon",
                "g__Escherichia",
                "--output",
                str(tmp_path),
                "--include",
                "gff3",
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_non_empty_output_directory(tmp_path: Path) -> None:
    """Existing non-empty output directories should fail validation."""

    output_dir = tmp_path / "results"
    output_dir.mkdir()
    (output_dir / "sentinel.txt").write_text("x", encoding="ascii")

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--release",
                "latest",
                "--taxon",
                "g__Escherichia",
                "--output",
                str(output_dir),
            ],
        )
    assert error.value.code == 2


def test_main_returns_preflight_error_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Missing external tools should return exit code 5."""

    def raise_preflight_error() -> None:
        """Raise a preflight error for the test."""

        raise PreflightError("Missing required external tools: datasets")

    monkeypatch.setattr("gtdb_genomes.cli.check_required_tools", raise_preflight_error)
    exit_code = main(
        [
            "--release",
            "latest",
            "--taxon",
            "g__Escherichia",
            "--output",
            str(tmp_path),
        ],
    )

    assert exit_code == 5

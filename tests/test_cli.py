"""Tests for the Phase 1 command-line interface."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import CliArgs, build_parser, main, parse_args
from gtdb_genomes.preflight import PreflightError


def test_help_includes_documented_flags() -> None:
    """The parser help should include the documented Phase 1 flags."""

    help_text = build_parser().format_help()
    assert "--gtdb-release" in help_text
    assert "--gtdb-taxon" in help_text
    assert "--outdir" in help_text
    assert "--prefer-genbank" in help_text
    assert "--no-prefer-genbank" not in help_text
    assert "--download-method" in help_text
    assert "--threads" in help_text
    assert "--ncbi-api-key" in help_text
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
            "--gtdb-release",
            " latest ",
            "--gtdb-taxon",
            " g__Escherichia ",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            " s__Escherichia coli ",
            "--outdir",
            str(tmp_path),
        ],
    )

    assert isinstance(args, CliArgs)
    assert args.gtdb_release == "latest"
    assert args.gtdb_taxa == ("g__Escherichia", "s__Escherichia coli")
    assert args.prefer_genbank is False


def test_parse_args_rejects_blank_release(tmp_path: Path) -> None:
    """Blank release values should fail validation."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--gtdb-release",
                " ",
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
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
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                " ",
                "--outdir",
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
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
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
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
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
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
                str(output_dir),
            ],
        )
    assert error.value.code == 2


def test_parse_args_accepts_ncbi_api_key_flag(tmp_path: Path) -> None:
    """The renamed NCBI API key flag should parse into the normalised args."""

    parser = build_parser()
    args = parse_args(
        parser,
        [
            "--gtdb-release",
            "latest",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path),
            "--ncbi-api-key",
            "secret",
        ],
    )

    assert args.ncbi_api_key == "secret"


def test_parse_args_rejects_legacy_api_key_flag(tmp_path: Path) -> None:
    """The removed legacy API key flag should be rejected."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "g__Escherichia",
                "--outdir",
                str(tmp_path),
                "--api-key",
                "secret",
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_removed_legacy_flags(tmp_path: Path) -> None:
    """Removed legacy CLI flags should be rejected."""

    for legacy_flag in ("--release", "--taxon", "--output", "--no-prefer-genbank"):
        parser = build_parser()
        argv = [
            "--gtdb-release",
            "latest",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path),
        ]
        if legacy_flag == "--no-prefer-genbank":
            argv.append(legacy_flag)
        else:
            argv.extend([legacy_flag, "legacy-value"])
        with pytest.raises(SystemExit) as error:
            parse_args(parser, argv)
        assert error.value.code == 2


def test_main_returns_preflight_error_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Missing external tools should return exit code 5."""

    def raise_preflight_error(required_tools: tuple[str, ...]) -> None:
        """Raise a preflight error for the test."""

        assert required_tools == ("datasets", "unzip")
        raise PreflightError("Missing required external tools: datasets")

    monkeypatch.setattr("gtdb_genomes.cli.check_required_tools", raise_preflight_error)
    exit_code = main(
        [
            "--gtdb-release",
            "latest",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path),
        ],
    )

    assert exit_code == 5

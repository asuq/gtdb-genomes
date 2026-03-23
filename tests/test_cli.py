"""Tests for the Phase 1 command-line interface."""

from __future__ import annotations

from pathlib import Path

import pytest

from gtdb_genomes.cli import (
    DEFAULT_THREADS,
    CliArgs,
    build_parser,
    main,
    parse_args,
)
from gtdb_genomes.subprocess_utils import NCBI_API_KEY_ENV_VAR
from gtdb_genomes.preflight import PreflightError


def test_help_includes_documented_flags() -> None:
    """The parser help should include the documented Phase 1 flags."""

    help_text = build_parser().format_help()
    assert "--gtdb-release" in help_text
    assert "--gtdb-taxon" in help_text
    assert "--outdir" in help_text
    assert "--prefer-genbank" in help_text
    assert "--version-latest" in help_text
    assert "--version-fixed" not in help_text
    assert "--no-prefer-genbank" not in help_text
    assert "--download-method" not in help_text
    assert "--threads" in help_text
    assert "--ncbi-api-key" in help_text
    assert "--include" in help_text
    assert "--debug" in help_text
    assert "--keep-temp" in help_text
    assert "--dry-run" in help_text
    assert "Accept one or more values per" in help_text
    assert "use and repeat as needed." in help_text
    assert "Quote species taxa with" in help_text
    assert 'spaces, for example "s__Altiarchaeum hamiconexum".' in help_text
    assert "current NCBI metadata" in help_text
    assert "direct downloads remain serial" in help_text
    assert "Default: latest." in help_text
    assert "8." in help_text
    assert f"Overrides ambient {NCBI_API_KEY_ENV_VAR}." in help_text


def test_parse_args_defaults_release_to_latest(tmp_path: Path) -> None:
    """Omitting the release flag should default to the bundled latest alias."""

    parser = build_parser()
    args = parse_args(
        parser,
        [
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path),
        ],
    )

    assert isinstance(args, CliArgs)
    assert args.gtdb_release == "latest"


def test_parse_args_accepts_multiple_taxa_after_one_flag(tmp_path: Path) -> None:
    """One `--gtdb-taxon` occurrence should accept multiple complete taxa."""

    parser = build_parser()
    args = parse_args(
        parser,
        [
            "--gtdb-release",
            " latest ",
            "--gtdb-taxon",
            " g__Escherichia ",
            " s__Escherichia coli ",
            "--outdir",
            str(tmp_path),
        ],
    )

    assert isinstance(args, CliArgs)
    assert args.gtdb_release == "latest"
    assert args.gtdb_taxa == ("g__Escherichia", "s__Escherichia coli")
    assert args.prefer_genbank is False
    assert args.version_latest is False


def test_parse_args_normalises_and_deduplicates_taxa_across_taxon_groups(
    tmp_path: Path,
) -> None:
    """Multi-value and repeated taxon groups should deduplicate in order."""

    parser = build_parser()
    args = parse_args(
        parser,
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
            str(tmp_path),
        ],
    )

    assert isinstance(args, CliArgs)
    assert args.gtdb_release == "latest"
    assert args.gtdb_taxa == (
        "g__Escherichia",
        "s__Escherichia coli",
        "g__Bacillus",
    )


def test_parse_args_uses_fixed_default_threads(tmp_path: Path) -> None:
    """Thread defaults should stay pinned to the documented fixed value."""

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
        ],
    )

    assert args.threads == DEFAULT_THREADS == 8


def test_parse_args_rejects_shell_split_species_taxon(tmp_path: Path) -> None:
    """Unquoted shell-split species input should fail CLI parsing."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "s__Altiarchaeum",
                "hamiconexum",
                "--outdir",
                str(tmp_path),
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_taxon_without_recognised_rank_prefix(
    tmp_path: Path,
) -> None:
    """Each parsed value should be validated as one complete GTDB taxon token."""

    parser = build_parser()
    with pytest.raises(SystemExit) as error:
        parse_args(
            parser,
            [
                "--gtdb-release",
                "latest",
                "--gtdb-taxon",
                "Escherichia",
                "--outdir",
                str(tmp_path),
            ],
        )
    assert error.value.code == 2


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


def test_parse_args_rejects_uninspectable_output_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Output paths that raise OS errors during inspection should fail cleanly."""

    parser = build_parser()
    output_dir = tmp_path / "uninspectable"
    original_exists = Path.exists
    original_is_dir = Path.is_dir
    original_iterdir = Path.iterdir

    def fake_exists(path: Path) -> bool:
        """Pretend that only the test output directory already exists."""

        if path == output_dir:
            return True
        return original_exists(path)

    def fake_is_dir(path: Path) -> bool:
        """Report the test path as an existing directory."""

        if path == output_dir:
            return True
        return original_is_dir(path)

    def fake_iterdir(path: Path):
        """Raise a deterministic permission error for the test path."""

        if path == output_dir:
            raise PermissionError("permission denied")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "exists", fake_exists)
    monkeypatch.setattr(Path, "is_dir", fake_is_dir)
    monkeypatch.setattr(Path, "iterdir", fake_iterdir)

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

    captured = capsys.readouterr()
    assert error.value.code == 2
    assert "argument --outdir: could not inspect path" in captured.err


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


def test_parse_args_rejects_version_latest_without_prefer_genbank(
    tmp_path: Path,
) -> None:
    """Latest-version mode should require the GenBank preference mode."""

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
                "--version-latest",
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


def test_parse_args_uses_ambient_ncbi_api_key_when_flag_is_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Ambient NCBI API keys should become the effective CLI secret."""

    parser = build_parser()
    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

    args = parse_args(
        parser,
        [
            "--gtdb-release",
            "latest",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(tmp_path),
        ],
    )

    assert args.ncbi_api_key == "ambient-secret"


def test_parse_args_prefers_cli_ncbi_api_key_over_ambient_value(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Explicit API keys should override ambient environment values."""

    parser = build_parser()
    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

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
            "flag-secret",
        ],
    )

    assert args.ncbi_api_key == "flag-secret"


def test_parse_args_rejects_debug_with_ncbi_api_key(tmp_path: Path) -> None:
    """Debug mode should be rejected when an NCBI API key is supplied."""

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
                "--ncbi-api-key",
                "secret",
                "--debug",
            ],
        )

    assert error.value.code == 2


def test_parse_args_rejects_debug_with_ambient_ncbi_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Debug mode should be rejected when an ambient API key is active."""

    parser = build_parser()
    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")
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
                "--debug",
            ],
        )

    assert error.value.code == 2


def test_parse_args_defaults_to_fixed_version_with_prefer_genbank(
    tmp_path: Path,
) -> None:
    """Prefer-GenBank should keep the exact selected version by default."""

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
            "--prefer-genbank",
        ],
    )

    assert args.prefer_genbank is True
    assert args.version_latest is False


def test_parse_args_accepts_version_latest_with_prefer_genbank(
    tmp_path: Path,
) -> None:
    """Latest-version mode should parse when GenBank preference is enabled."""

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
            "--prefer-genbank",
            "--version-latest",
        ],
    )

    assert args.prefer_genbank is True
    assert args.version_latest is True


def test_parse_args_rejects_removed_version_fixed_flag(tmp_path: Path) -> None:
    """The removed fixed-version flag should be rejected."""

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
                "--version-fixed",
            ],
        )
    assert error.value.code == 2


def test_parse_args_rejects_removed_download_method_flag(tmp_path: Path) -> None:
    """The public strategy-selection flag should no longer be accepted."""

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
                "--download-method",
                "direct",
            ],
        )
    assert error.value.code == 2


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

    def raise_preflight_error(args: CliArgs) -> int:
        """Raise a preflight error for the test."""

        assert args.gtdb_release == "latest"
        raise PreflightError("Missing required external tools: datasets")

    monkeypatch.setattr("gtdb_genomes.workflow.run_workflow", raise_preflight_error)
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

"""Tests for datasets download planning and retry handling."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gtdb_genomes.download import (
    DEHYDRATE_ACCESSION_THRESHOLD,
    DEHYDRATE_SIZE_GB_THRESHOLD,
    PreviewError,
    build_batch_dehydrate_command,
    build_download_command,
    build_preview_command,
    build_rehydrate_command,
    download_with_accession_fallback,
    get_direct_download_concurrency,
    get_rehydrate_workers,
    parse_preview_size_bytes,
    run_retryable_command,
    run_preview_command,
    select_download_method,
    validate_include_value,
    write_accession_input_file,
)


def test_validate_include_value_requires_genome() -> None:
    """The include value should stay genome-centric."""

    assert validate_include_value(" genome , gff3 ") == "genome,gff3"

    with pytest.raises(ValueError, match="must contain 'genome'"):
        validate_include_value("protein,gff3")


def test_command_builders_match_datasets_cli_shape() -> None:
    """Command builders should emit the expected datasets argv layout."""

    preview_command = build_preview_command(
        ["GCA_1", "GCA_1", "GCF_2"],
        "genome,gff3",
        api_key="secret",
        debug=True,
    )
    download_command = build_download_command(
        ["GCA_1", "GCF_2"],
        Path("/tmp/out.zip"),
        "genome",
        api_key="secret",
        dehydrated=True,
        debug=True,
    )
    rehydrate_command = build_rehydrate_command(
        Path("/tmp/bag"),
        7,
        api_key="secret",
        debug=True,
    )
    batch_dehydrate_command = build_batch_dehydrate_command(
        Path("/tmp/accessions.txt"),
        Path("/tmp/out.zip"),
        "genome",
        api_key="secret",
        debug=True,
    )

    assert preview_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "GCA_1",
        "GCF_2",
        "--include",
        "genome,gff3",
        "--preview",
        "--api-key",
        "secret",
        "--debug",
    ]
    assert download_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "GCA_1",
        "GCF_2",
        "--filename",
        "/tmp/out.zip",
        "--include",
        "genome",
        "--dehydrated",
        "--api-key",
        "secret",
        "--debug",
    ]
    assert rehydrate_command == [
        "datasets",
        "rehydrate",
        "--directory",
        "/tmp/bag",
        "--max-workers",
        "7",
        "--api-key",
        "secret",
        "--debug",
    ]
    assert batch_dehydrate_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "--inputfile",
        "/tmp/accessions.txt",
        "--filename",
        "/tmp/out.zip",
        "--include",
        "genome",
        "--dehydrated",
        "--api-key",
        "secret",
        "--debug",
    ]


def test_select_download_method_uses_preview_size_and_count_thresholds() -> None:
    """Auto mode should switch to dehydrate on either documented threshold."""

    small_preview = "Package size: 1.0 GB\n"
    large_preview = f"Package size: {DEHYDRATE_SIZE_GB_THRESHOLD + 1.0} GB\n"

    assert select_download_method("auto", 5, small_preview).method_used == "direct"
    assert (
        select_download_method("auto", DEHYDRATE_ACCESSION_THRESHOLD, small_preview)
        .method_used
        == "dehydrate"
    )
    assert select_download_method("auto", 5, large_preview).method_used == "dehydrate"

    with pytest.raises(PreviewError, match="required in auto mode"):
        select_download_method("auto", 5, None)

    with pytest.raises(PreviewError, match="could not parse"):
        select_download_method("auto", 5, "No size here")


def test_parse_preview_size_bytes_uses_largest_size_value() -> None:
    """Preview parsing should prefer the largest observed size token."""

    preview = "Download size: 850 MB\nUncompressed size: 2.5 GB\n"

    assert parse_preview_size_bytes(preview) == int(2.5 * 1024**3)


def test_worker_caps_and_accession_input_file_follow_documented_limits(
    tmp_path: Path,
) -> None:
    """Worker caps and dehydrated accession files should stay deterministic."""

    accession_file = write_accession_input_file(
        tmp_path / "accessions.txt",
        ["GCA_1", "GCA_1", "GCF_2"],
    )

    assert get_direct_download_concurrency(8, 12) == 5
    assert get_rehydrate_workers(64) == 30
    assert accession_file.read_text(encoding="ascii") == "GCA_1\nGCF_2\n"


def test_run_retryable_command_records_retries_before_success() -> None:
    """Retryable commands should keep retry history with fixed delays."""

    attempts = iter([1, 1, 0])
    sleep_calls: list[float] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return a sequence of fake command outcomes."""

        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="temporary failure",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="preferred_download",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is True
    assert sleep_calls == [5, 15]
    assert [failure.final_status for failure in result.failures] == [
        "retry_scheduled",
        "retry_scheduled",
    ]


def test_preferred_gca_download_uses_full_retry_budget_before_fallback() -> None:
    """Fallback should start only after the preferred accession exhausts retries."""

    attempts = iter([1, 1, 1, 1, 0])
    sleep_calls: list[float] = []
    commands: list[list[str]] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return a fake preferred-failure then fallback-success sequence."""

        commands.append(command)
        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="download failed",
        )

    result = download_with_accession_fallback(
        preferred_accession="GCA_000001.1",
        fallback_accession="GCF_000001.1",
        archive_path=Path("/tmp/out.zip"),
        include="genome",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is True
    assert result.used_accession == "GCF_000001.1"
    assert result.used_fallback is True
    assert sleep_calls == [5, 15, 45]
    assert [command[4] for command in commands] == [
        "GCA_000001.1",
        "GCA_000001.1",
        "GCA_000001.1",
        "GCA_000001.1",
        "GCF_000001.1",
    ]
    assert result.failures[-1].final_status == "retry_exhausted"


def test_preview_command_uses_full_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preview should retry network failures three times before raising."""

    attempts = iter([1, 1, 1, 1])

    def fake_run(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        """Return repeated preview failures."""

        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="preview failed",
        )

    with pytest.raises(PreviewError, match="preview failed"):
        run_preview_command(
            ["GCA_1"],
            "genome",
            sleep_func=lambda delay: None,
            runner=fake_run,
        )

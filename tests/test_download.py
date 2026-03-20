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
        Path("/tmp/accessions.txt"),
        "genome,gff3",
        ncbi_api_key="secret",
        debug=True,
    )
    download_command = build_download_command(
        ["GCA_1", "GCF_2"],
        Path("/tmp/out.zip"),
        "genome",
        ncbi_api_key="secret",
        debug=True,
    )
    rehydrate_command = build_rehydrate_command(
        Path("/tmp/bag"),
        7,
        ncbi_api_key="secret",
        debug=True,
    )
    batch_dehydrate_command = build_batch_dehydrate_command(
        Path("/tmp/accessions.txt"),
        Path("/tmp/out.zip"),
        "genome",
        ncbi_api_key="secret",
        debug=True,
    )

    assert preview_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "--inputfile",
        "/tmp/accessions.txt",
        "--include",
        "genome,gff3",
        "--preview",
        "--api-key",
        "secret",
        "--debug",
    ]
    assert "GCA_1" not in preview_command
    assert "GCF_2" not in preview_command
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
    mixed_preview = "Download size: 1.0 GB\nUncompressed size: 16.0 GB\n"

    assert select_download_method("auto", 5, small_preview).method_used == "direct"
    assert select_download_method("auto", 5, mixed_preview).method_used == "direct"
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

    with pytest.raises(PreviewError, match="could not parse"):
        select_download_method(
            "auto",
            5,
            "Estimated size: 850 MB\nUncompressed size: 2.5 GB\n",
        )


def test_parse_preview_size_bytes_prefers_package_or_download_size() -> None:
    """Preview parsing should use the labelled package or download size."""

    preview = "Download size: 850 MB\nUncompressed size: 2.5 GB\n"

    assert parse_preview_size_bytes(preview) == int(850 * 1024**2)


def test_parse_preview_size_bytes_accepts_json_preview_output() -> None:
    """Preview parsing should accept JSON output from datasets preview."""

    preview = (
        '{"resource_updated_on":"2026-03-18T16:17:00Z",'
        '"record_count":1024,'
        '"estimated_file_size_mb":1556,'
        '"included_data_files":{"all_genomic_fasta":{"file_count":1024,'
        '"size_mb":1556.5991}}}\n'
    )

    assert parse_preview_size_bytes(preview) == int(1556 * 1024**2)


def test_parse_preview_size_bytes_sums_json_file_sizes_when_needed() -> None:
    """Preview parsing should sum JSON file sizes without an estimate field."""

    preview = (
        '{"included_data_files":{"genome":{"size_mb":512.0},'
        '"annotation":{"size_mb":128.5}}}\n'
    )

    assert parse_preview_size_bytes(preview) == int(640.5 * 1024**2)


def test_parse_preview_size_bytes_rejects_ambiguous_unlabelled_sizes() -> None:
    """Preview parsing should reject multi-size text without package labels."""

    preview = "Estimated size: 850 MB\nUncompressed size: 2.5 GB\n"

    assert parse_preview_size_bytes(preview) is None


def test_worker_caps_and_accession_input_file_follow_documented_limits(
    tmp_path: Path,
) -> None:
    """Worker caps and accession input files should stay deterministic."""

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
        timeout: int,
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


def test_run_retryable_command_uses_stage_message_for_silent_failures() -> None:
    """Silent subprocess failures should still leave a useful error message."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one failed subprocess result without any output."""

        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="preferred_download",
        sleep_func=lambda delay: None,
        runner=runner,
    )

    assert result.succeeded is False
    assert result.failures[-1].error_message == "preferred download command failed"


def test_run_retryable_command_retries_timeouts_before_success() -> None:
    """Timeouts should consume the retry budget like other transient failures."""

    attempts = iter(["timeout", "success"])
    sleep_calls: list[float] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise one timeout before returning a success."""

        attempt = next(attempts)
        if attempt == "timeout":
            raise subprocess.TimeoutExpired(command, timeout)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    result = run_retryable_command(
        ["datasets", "download"],
        stage="preferred_download",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.succeeded is True
    assert sleep_calls == [5]
    assert result.failures[0].error_type == "timeout"


def test_run_retryable_command_returns_spawn_failure_without_retry() -> None:
    """Spawn failures should fail fast instead of consuming the retry budget."""

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a missing-executable error before a child process starts."""

        raise FileNotFoundError("datasets")

    result = run_retryable_command(
        ["datasets", "download"],
        stage="preferred_download",
        sleep_func=lambda delay: None,
        runner=runner,
    )

    assert result.succeeded is False
    assert len(result.failures) == 1
    assert result.failures[0].error_type == "spawn_error"
    assert result.failures[0].error_message.startswith(
        "preferred download command could not start",
    )


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
        timeout: int,
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
            Path("/tmp/accessions.txt"),
            "genome",
            sleep_func=lambda delay: None,
            runner=fake_run,
        )

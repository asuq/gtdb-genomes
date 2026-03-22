"""Tests for datasets download planning and retry handling."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gtdb_genomes.download import (
    DEHYDRATE_ACCESSION_THRESHOLD,
    PreviewError,
    build_batch_dehydrate_command,
    build_direct_batch_download_command,
    build_preview_command,
    build_rehydrate_command,
    get_rehydrate_workers,
    run_retryable_command,
    run_preview_command,
    select_download_method,
    validate_include_value,
    write_accession_input_file,
)

COMMAND_TEST_ACCESSION_FILE = Path("tmp") / "accessions.txt"
COMMAND_TEST_ARCHIVE_FILE = Path("tmp") / "out.zip"
COMMAND_TEST_BAG_DIRECTORY = Path("tmp") / "bag"


def test_validate_include_value_requires_genome() -> None:
    """The include value should stay genome-centric."""

    assert validate_include_value(" genome , gff3 ") == "genome,gff3"
    assert validate_include_value("genome,gff3,protein") == "genome,gff3,protein"

    with pytest.raises(ValueError, match="must contain 'genome'"):
        validate_include_value("protein,gff3")


def test_validate_include_value_rejects_unknown_tokens() -> None:
    """Unsupported include tokens should fail locally."""

    with pytest.raises(ValueError, match="unsupported include token"):
        validate_include_value("genome,mrna")


def test_command_builders_match_datasets_cli_shape() -> None:
    """Command builders should emit the expected datasets argv layout."""

    preview_command = build_preview_command(
        COMMAND_TEST_ACCESSION_FILE,
        "genome,gff3",
        ncbi_api_key="secret",
        debug=True,
    )
    direct_batch_command = build_direct_batch_download_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
        "genome",
        ncbi_api_key="secret",
        debug=True,
    )
    rehydrate_command = build_rehydrate_command(
        COMMAND_TEST_BAG_DIRECTORY,
        7,
        ncbi_api_key="secret",
        debug=True,
    )
    batch_dehydrate_command = build_batch_dehydrate_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
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
        str(COMMAND_TEST_ACCESSION_FILE),
        "--include",
        "genome,gff3",
        "--preview",
        "--api-key",
        "secret",
        "--debug",
    ]
    assert "GCA_1" not in preview_command
    assert "GCF_2" not in preview_command
    assert direct_batch_command == [
        "datasets",
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(COMMAND_TEST_ACCESSION_FILE),
        "--filename",
        str(COMMAND_TEST_ARCHIVE_FILE),
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
        str(COMMAND_TEST_BAG_DIRECTORY),
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
        str(COMMAND_TEST_ACCESSION_FILE),
        "--filename",
        str(COMMAND_TEST_ARCHIVE_FILE),
        "--include",
        "genome",
        "--dehydrated",
        "--api-key",
        "secret",
        "--debug",
    ]


def test_select_download_method_uses_count_only_threshold() -> None:
    """Auto mode should switch to dehydrate only above the count threshold."""

    assert select_download_method(5).method_used == "direct"
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD).method_used
        == "direct"
    )
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD + 1).method_used
        == "dehydrate"
    )


def test_worker_caps_and_accession_input_file_follow_documented_limits(
    tmp_path: Path,
) -> None:
    """Rehydrate caps and accession input files should stay deterministic."""

    accession_file = write_accession_input_file(
        tmp_path / "accessions.txt",
        ["GCA_1", "GCA_1", "GCF_2"],
    )

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

    del monkeypatch
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

    with pytest.raises(PreviewError, match="preview failed") as error_info:
        run_preview_command(
            COMMAND_TEST_ACCESSION_FILE,
            "genome",
            sleep_func=lambda delay: None,
            runner=fake_run,
        )
    assert [failure.final_status for failure in error_info.value.failures] == [
        "retry_scheduled",
        "retry_scheduled",
        "retry_scheduled",
        "retry_exhausted",
    ]


def test_preview_command_returns_retry_history_after_success() -> None:
    """Preview should preserve earlier retry failures when a later attempt works."""

    attempts = iter([1, 0])
    sleep_calls: list[float] = []
    observed_attempts: list[int] = []

    def runner(
        command: list[str],
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one transient preview failure before a successful response."""

        return_code = next(attempts)
        observed_attempts.append(return_code)
        return subprocess.CompletedProcess(
            command,
            return_code,
            stdout="Package size: 1.0 GB\n" if return_code == 0 else "",
            stderr="" if return_code == 0 else "preview failed",
        )

    result = run_preview_command(
        COMMAND_TEST_ACCESSION_FILE,
        "genome",
        sleep_func=sleep_calls.append,
        runner=runner,
    )

    assert result.preview_text == "Package size: 1.0 GB\n"
    assert sleep_calls == [5]
    assert observed_attempts == [1, 0]
    assert len(result.failures) == 1
    assert result.failures[0].stage == "preview"
    assert result.failures[0].final_status == "retry_scheduled"

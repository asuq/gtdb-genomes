"""Tests for datasets download planning and retry handling."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from gtdb_genomes.download import (
    DEHYDRATE_ACCESSION_THRESHOLD,
    build_batch_dehydrate_command,
    build_direct_batch_download_command,
    build_rehydrate_command,
    get_rehydrate_workers,
    run_retryable_command,
    select_download_method,
    validate_include_value,
    write_accession_input_file,
)
from gtdb_genomes.subprocess_utils import (
    NCBI_API_KEY_ENV_VAR,
    build_datasets_subprocess_environment,
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

    direct_batch_command = build_direct_batch_download_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
        "genome",
        debug=True,
    )
    rehydrate_command = build_rehydrate_command(
        COMMAND_TEST_BAG_DIRECTORY,
        7,
        debug=True,
    )
    batch_dehydrate_command = build_batch_dehydrate_command(
        COMMAND_TEST_ACCESSION_FILE,
        COMMAND_TEST_ARCHIVE_FILE,
        "genome",
        debug=True,
    )

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
        "--debug",
    ]
    assert rehydrate_command == [
        "datasets",
        "rehydrate",
        "--directory",
        str(COMMAND_TEST_BAG_DIRECTORY),
        "--max-workers",
        "7",
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
        "--debug",
    ]


def test_select_download_method_uses_count_only_threshold() -> None:
    """Auto mode should switch to dehydrate at the documented count threshold."""

    assert select_download_method(5).method_used == "direct"
    assert select_download_method(5).accession_count == 5
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD).method_used
        == "dehydrate"
    )
    assert (
        select_download_method(DEHYDRATE_ACCESSION_THRESHOLD + 1).method_used
        == "dehydrate"
    )


def test_build_datasets_subprocess_environment_overrides_child_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Child environments should honour the explicit CLI API key."""

    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

    environment = build_datasets_subprocess_environment("cli-secret")

    assert environment[NCBI_API_KEY_ENV_VAR] == "cli-secret"
    assert os.environ[NCBI_API_KEY_ENV_VAR] == "ambient-secret"


def test_build_datasets_subprocess_environment_clears_ambient_child_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Child environments should not inherit ambient API keys implicitly."""

    monkeypatch.setenv(NCBI_API_KEY_ENV_VAR, "ambient-secret")

    environment = build_datasets_subprocess_environment(None)

    assert NCBI_API_KEY_ENV_VAR not in environment
    assert os.environ[NCBI_API_KEY_ENV_VAR] == "ambient-secret"


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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return a sequence of fake command outcomes."""

        assert env == {NCBI_API_KEY_ENV_VAR: "secret"}
        return subprocess.CompletedProcess(
            command,
            next(attempts),
            stdout="",
            stderr="temporary failure",
        )

    result = run_retryable_command(
        ["datasets", "download"],
        stage="preferred_download",
        environment={NCBI_API_KEY_ENV_VAR: "secret"},
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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Return one failed subprocess result without any output."""

        assert env is None
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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise one timeout before returning a success."""

        assert env is None
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
        env: dict[str, str] | None,
        timeout: int,
    ) -> subprocess.CompletedProcess[str]:
        """Raise a missing-executable error before a child process starts."""

        assert env is None
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

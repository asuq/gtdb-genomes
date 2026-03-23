"""Contract-level edge-case tests for direct and dehydrated execution."""

from __future__ import annotations

import logging
import polars as pl
import pytest

from gtdb_genomes.cli import CliArgs
from gtdb_genomes.download import CommandFailureRecord, RetryableCommandResult
from gtdb_genomes.layout import LayoutError, initialise_run_directories
from gtdb_genomes.subprocess_utils import NCBI_API_KEY_ENV_VAR
from gtdb_genomes.workflow_execution import (
    AccessionExecution,
    AccessionPlan,
    DownloadExecutionResult,
    ResolvedPayloadDirectory,
    execute_batch_dehydrate_plans,
    execute_direct_accession_plans,
)
from tests.workflow_contract_helpers import (
    build_cli_args,
)


def test_release_80_contains_the_real_shared_preferred_accession_pair() -> None:
    """Selection logic should preserve the known GCF/GCA duplicate pair."""

    taxonomy_frame = pl.DataFrame(
        {
            "gtdb_accession": ["GCF_001881595.2", "GCA_001881595.3"],
            "lineage": [
                "d__Bacteria;g__Example;s__Example one",
                "d__Bacteria;g__Example;s__Example one",
            ],
            "ncbi_accession": ["GCF_001881595.2", "GCA_001881595.3"],
            "taxonomy_file": ["fixture.tsv", "fixture.tsv"],
        },
    )
    selected = taxonomy_frame.filter(
        pl.col("ncbi_accession").is_in(
            ["GCF_001881595.2", "GCA_001881595.3"],
        ),
    )

    assert selected.select("ncbi_accession").rows() == [
        ("GCF_001881595.2",),
        ("GCA_001881595.3",),
    ]


def test_direct_mode_downloads_shared_preferred_accession_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Two originals that share one preferred accession should download once."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[Path, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful preferred download for the shared group."""

        del command, final_failure_status, environment, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extracted directory for one direct batch pass."""

        extraction_root.mkdir(parents=True, exist_ok=True)
        extraction_calls.append((archive_path, extraction_root))

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Return one shared resolved payload for the preferred accession."""

        assert extraction_root.name == "direct_batch_1"
        return (
            ResolvedPayloadDirectory(
                final_accession="GCA_001881595.5",
                directory=payload_directory,
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-shared-success")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-shared-success"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [("preferred_download", "GCA_001881595")]
    assert extraction_calls == [
        (
            run_directories.downloads_root / "direct_batch_1.zip",
            run_directories.extracted_root / "direct_batch_1",
        ),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCA_001881595.5"
    assert result.executions["GCF_001881595.2"].download_status == "downloaded"
    assert result.executions["GCF_001881595.2"].download_batch == "direct_batch_1"
    assert result.executions["GCF_001881595.2"].request_accession_used == (
        "GCA_001881595"
    )
    assert result.executions["GCA_001881595.3"].final_accession == "GCA_001881595.5"
    assert result.executions["GCA_001881595.3"].download_status == "downloaded"
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"
    assert result.executions["GCA_001881595.3"].request_accession_used == (
        "GCA_001881595"
    )


def test_direct_mode_passes_api_key_via_child_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Direct downloads should pass the API key through the child environment."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful direct batch command."""

        del final_failure_status, sleep_func, runner
        assert stage == "preferred_download"
        assert attempted_accession == "GCF_000001.1"
        assert "--api-key" not in command
        assert environment is not None
        assert environment[NCBI_API_KEY_ENV_VAR] == "secret"
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extracted directory for one direct batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Return one resolved payload for the requested accession."""

        assert extraction_root.name == "direct_batch_1"
        return (
            ResolvedPayloadDirectory(
                final_accession="GCF_000001.1",
                directory=payload_directory,
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    args = build_cli_args(tmp_path / "out")
    args.ncbi_api_key = "secret"
    run_directories = initialise_run_directories(tmp_path / "direct-api-key-env")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
            ),
        ),
        args,
        run_directories,
        logging.getLogger("test-direct-api-key-env"),
    )

    assert result.executions["GCF_000001.1"].download_status == "downloaded"


def test_direct_mode_retries_unresolved_accessions_in_later_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A later direct batch should retry only the unresolved request accession."""

    payload_one = tmp_path / "payload-one"
    payload_one.mkdir()
    payload_two = tmp_path / "payload-two"
    payload_two.mkdir()
    download_calls: list[tuple[str, str]] = []
    extraction_calls: list[tuple[Path, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful direct batch commands for both passes."""

        del command, final_failure_status, environment, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per direct batch pass."""

        extraction_root.mkdir(parents=True, exist_ok=True)
        extraction_calls.append((archive_path, extraction_root))

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Expose only one payload in the first pass, then the remaining one."""

        if extraction_root.name == "direct_batch_1":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000001.1",
                    directory=payload_one,
                ),
            )
        if extraction_root.name == "direct_batch_2":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000002.1",
                    directory=payload_two,
                ),
            )
        raise AssertionError(f"Unexpected extraction root: {extraction_root}")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    args = build_cli_args(tmp_path / "out")
    args.prefer_genbank = False

    run_directories = initialise_run_directories(tmp_path / "direct-batch-retry")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
            ),
            AccessionPlan(
                original_accession="GCF_000002.1",
                download_request_accession="GCF_000002.1",
                conversion_status="unchanged_original",
            ),
        ),
        args,
        run_directories,
        logging.getLogger("test-direct-batch-retry"),
    )

    assert result.download_concurrency_used == 1
    assert download_calls == [
        ("preferred_download", "GCF_000001.1;GCF_000002.1"),
        ("preferred_download", "GCF_000002.1"),
    ]
    assert extraction_calls == [
        (
            run_directories.downloads_root / "direct_batch_1.zip",
            run_directories.extracted_root / "direct_batch_1",
        ),
        (
            run_directories.downloads_root / "direct_batch_2.zip",
            run_directories.extracted_root / "direct_batch_2",
        ),
    ]
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_1"
    assert result.executions["GCF_000001.1"].failures == ()
    assert result.executions["GCF_000001.1"].request_accession_used == "GCF_000001.1"
    assert result.executions["GCF_000002.1"].download_batch == "direct_batch_2"
    assert result.executions["GCF_000002.1"].request_accession_used == "GCF_000002.1"
    assert [failure.final_status for failure in result.executions["GCF_000002.1"].failures] == [
        "retry_scheduled",
    ]
    assert [failure.attempted_accession for failure in result.executions["GCF_000002.1"].failures] == [
        "GCF_000002.1",
    ]


def test_direct_mode_falls_back_to_original_accession_after_preferred_phase(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unresolved preferred rows should switch into original-accession fallback."""

    download_calls: list[tuple[str, str]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful preferred and fallback batch commands."""

        del command, final_failure_status, environment, sleep_func, runner
        download_calls.append((stage, attempted_accession or ""))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep the preferred batch unresolved, then resolve the fallback batch."""

        if extraction_root.name == "direct_batch_1":
            return ()
        if extraction_root.name == "direct_fallback_batch_1":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_001881595.2",
                    directory=payload_directory,
                ),
            )
        raise AssertionError(f"Unexpected extraction root: {extraction_root}")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-preferred-fallback")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-fallback-layout"),
    )

    assert download_calls == [
        ("preferred_download", "GCA_001881595"),
        ("fallback_download", "GCF_001881595.2"),
    ]
    assert result.executions["GCF_001881595.2"].final_accession == "GCF_001881595.2"
    assert result.executions["GCF_001881595.2"].download_batch == "direct_fallback_batch_1"
    assert result.executions["GCF_001881595.2"].download_status == "downloaded_after_fallback"
    assert result.executions["GCF_001881595.2"].request_accession_used == (
        "GCF_001881595.2"
    )
    assert (
        result.executions["GCF_001881595.2"].conversion_status
        == "paired_to_gca_fallback_original_on_download_failure"
    )
    assert [failure.attempted_accession for failure in result.executions["GCF_001881595.2"].failures] == [
        "GCA_001881595",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_status == "failed"
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"
    assert result.executions["GCA_001881595.3"].request_accession_used == (
        "GCA_001881595"
    )
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
    ]
    assert [failure.final_status for failure in result.executions["GCA_001881595.3"].failures] == [
        "retry_exhausted",
    ]


def test_direct_mode_preserves_shared_retry_failures_after_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful direct batches should keep earlier shared retry failures."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    call_count = 0

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful batch with preserved retry history."""

        del command, final_failure_status, environment, sleep_func, runner
        nonlocal call_count
        call_count += 1
        assert call_count == 1
        assert stage == "preferred_download"
        assert attempted_accession == "GCF_000001.1"
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="temporary datasets failure",
                    final_status="retry_scheduled",
                ),
            ),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extracted directory for the successful batch."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Resolve the requested payload after the retried batch succeeds."""

        assert extraction_root.name == "direct_batch_1"
        return (
            ResolvedPayloadDirectory(
                final_accession="GCF_000001.1",
                directory=payload_directory,
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    args = build_cli_args(tmp_path / "out")
    args.prefer_genbank = False
    run_directories = initialise_run_directories(tmp_path / "direct-retry-success")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
            ),
        ),
        args,
        run_directories,
        logging.getLogger("test-direct-retry-success"),
    )

    assert result.executions["GCF_000001.1"].download_status == "downloaded"
    assert result.executions["GCF_000001.1"].failures == ()
    assert len(result.shared_failures) == 1
    assert result.shared_failures[0].affected_original_accessions == (
        "GCF_000001.1",
    )
    assert [failure.final_status for failure in result.shared_failures[0].failures] == [
        "retry_scheduled",
    ]
    assert [
        failure.attempted_accession
        for failure in result.shared_failures[0].failures
    ] == ["GCF_000001.1"]


def test_direct_mode_preserves_retry_history_when_extraction_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Extraction failures should not erase earlier shared retry history."""

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one successful batch with preserved retry history."""

        del command, final_failure_status, environment, sleep_func, runner
        assert stage == "preferred_download"
        assert attempted_accession == "GCF_000001.1"
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="temporary datasets failure",
                    final_status="retry_scheduled",
                ),
            ),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Raise one layout failure after the retried batch succeeds."""

        del archive_path, extraction_root
        raise LayoutError("broken archive layout")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )

    args = build_cli_args(tmp_path / "out")
    args.prefer_genbank = False
    run_directories = initialise_run_directories(
        tmp_path / "direct-retry-layout-failure",
    )
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
            ),
        ),
        args,
        run_directories,
        logging.getLogger("test-direct-retry-layout-failure"),
    )

    assert result.executions["GCF_000001.1"].download_status == "failed"
    assert len(result.shared_failures) == 2
    assert [
        failure.final_status for failure in result.shared_failures[0].failures
    ] == ["retry_scheduled"]
    assert [
        failure.attempted_accession
        for failure in result.shared_failures[0].failures
    ] == ["GCF_000001.1"]
    assert [
        failure.error_type for failure in result.shared_failures[1].failures
    ] == ["LayoutError"]


def test_direct_fallback_preserves_shared_retry_failures_after_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback batches should keep earlier shared retry failures."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    call_count = 0

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return one unresolved preferred batch and one retried fallback batch."""

        del command, final_failure_status, environment, sleep_func, runner
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            assert stage == "preferred_download"
            assert attempted_accession == "GCA_001881595"
            return RetryableCommandResult(
                succeeded=True,
                stdout="",
                stderr="",
                failures=(),
            )
        assert call_count == 2
        assert stage == "fallback_download"
        assert attempted_accession == "GCF_001881595.2"
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(
                CommandFailureRecord(
                    stage="fallback_download",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="temporary fallback failure",
                    final_status="retry_scheduled",
                ),
            ),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Leave the preferred batch unresolved and resolve the fallback batch."""

        if extraction_root.name == "direct_batch_1":
            return ()
        if extraction_root.name == "direct_fallback_batch_1":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_001881595.2",
                    directory=payload_directory,
                ),
            )
        raise AssertionError(f"Unexpected extraction root: {extraction_root}")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(
        tmp_path / "direct-fallback-retry-success",
    )
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-fallback-retry-success"),
    )

    assert result.executions["GCF_001881595.2"].download_status == (
        "downloaded_after_fallback"
    )
    assert len(result.shared_failures) == 1
    assert result.shared_failures[0].affected_original_accessions == (
        "GCF_001881595.2",
    )
    assert [failure.final_status for failure in result.shared_failures[0].failures] == [
        "retry_scheduled",
    ]
    assert [
        failure.attempted_accession
        for failure in result.shared_failures[0].failures
    ] == ["GCF_001881595.2"]


def test_direct_mode_records_failed_fallback_after_layout_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fallback exhaustion should retain both preferred and fallback history."""

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful direct batch commands for both phases."""

        del command
        del final_failure_status
        del environment
        del sleep_func
        del runner
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create one extraction root per batch pass."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep both preferred and fallback phases unresolved."""

        del extraction_root
        return ()

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_direct.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_payloads.collect_payload_directories",
        fake_collect_payload_directories,
    )

    run_directories = initialise_run_directories(tmp_path / "direct-fallback-failed")
    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
            ),
            AccessionPlan(
                original_accession="GCA_001881595.3",
                download_request_accession="GCA_001881595",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        run_directories,
        logging.getLogger("test-direct-fallback-failed"),
    )

    assert result.executions["GCF_001881595.2"].final_accession is None
    assert result.executions["GCF_001881595.2"].download_batch == "direct_fallback_batch_1"
    assert result.executions["GCF_001881595.2"].request_accession_used == (
        "GCF_001881595.2"
    )
    assert [failure.attempted_accession for failure in result.executions["GCF_001881595.2"].failures] == [
        "GCA_001881595",
        "GCF_001881595.2",
    ]
    assert [failure.final_status for failure in result.executions["GCF_001881595.2"].failures] == [
        "retry_exhausted",
        "retry_exhausted",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_1"
    assert result.executions["GCA_001881595.3"].request_accession_used == (
        "GCA_001881595"
    )
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
    ]


def test_batch_dehydrate_failure_falls_back_to_direct(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed batch dehydrated download should fall back to direct mode."""

    plans = (
        AccessionPlan(
            original_accession="GCF_000001.1",
            download_request_accession="GCA_000001",
            conversion_status="paired_to_gca",
        ),
        AccessionPlan(
            original_accession="GCF_000002.1",
            download_request_accession="GCA_000002",
            conversion_status="paired_to_gca",
        ),
    )
    args = CliArgs(
        gtdb_release="95",
        gtdb_taxa=("g__Escherichia",),
        outdir=tmp_path / "output",
        prefer_genbank=True,
        version_latest=False,
        threads=4,
        ncbi_api_key=None,
        include="genome",
        debug=False,
        keep_temp=False,
        dry_run=False,
    )
    run_directories = initialise_run_directories(tmp_path / "batch-output")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.run_retryable_command",
        lambda *args, **kwargs: RetryableCommandResult(
            succeeded=False,
            stdout="",
            stderr="batch failed",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=4,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="batch failed",
                    final_status="retry_exhausted",
                ),
            ),
        ),
    )

    def fake_execute_direct_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        run_directories,
        logger,
    ) -> DownloadExecutionResult:
        """Return a synthetic direct-download fallback result."""

        return DownloadExecutionResult(
            executions={
                plan.original_accession: AccessionExecution(
                    original_accession=plan.original_accession,
                    final_accession=plan.original_accession,
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch=plan.original_accession,
                    payload_directory=tmp_path,
                    failures=(),
                    request_accession_used=plan.original_accession,
                )
                for plan in plans
            },
            method_used="direct",
            download_concurrency_used=2,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.execute_direct_accession_plans",
        fake_execute_direct_accession_plans,
    )

    result = execute_batch_dehydrate_plans(
        plans,
        args,
        run_directories,
        logging.getLogger("test"),
        (),
    )

    assert result.method_used == "dehydrate_fallback_direct"
    assert result.download_concurrency_used == 2
    assert result.executions["GCF_000001.1"].failures == ()
    assert result.executions["GCF_000001.1"].request_accession_used == "GCF_000001.1"
    assert result.shared_failures[0].failures[0].attempted_accession == (
        "GCA_000001;GCA_000002"
    )


def test_batch_dehydrate_passes_api_key_via_child_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dehydrated download and rehydrate should use the child API-key environment."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    observed_calls: list[tuple[str, dict[str, str] | None]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
    ) -> RetryableCommandResult:
        """Return successful dehydrated download and rehydrate stages."""

        del final_failure_status, attempted_accession, sleep_func, runner
        assert "--api-key" not in command
        observed_calls.append((stage, environment))
        return RetryableCommandResult(
            succeeded=True,
            stdout="",
            stderr="",
            failures=(),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extracted directory for the dehydrated batch."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.locate_batch_payload_directories",
        lambda extraction_root, requested_accessions: {
            requested_accession: ResolvedPayloadDirectory(
                final_accession=f"{requested_accession}.1",
                directory=payload_directory,
            )
            for requested_accession in requested_accessions
        },
    )

    args = build_cli_args(tmp_path / "out")
    args.ncbi_api_key = "secret"
    args.threads = 4
    plans = (
        AccessionPlan(
            original_accession="GCF_000001.1",
            download_request_accession="GCA_000001",
            conversion_status="paired_to_gca",
        ),
    )
    run_directories = initialise_run_directories(tmp_path / "dehydrate-api-key-env")
    result = execute_batch_dehydrate_plans(
        plans,
        args,
        run_directories,
        logging.getLogger("test-dehydrate-api-key-env"),
        ("secret",),
    )

    assert [stage for stage, _ in observed_calls] == ["preferred_download", "rehydrate"]
    assert [environment[NCBI_API_KEY_ENV_VAR] for _, environment in observed_calls] == [
        "secret",
        "secret",
    ]
    assert result.method_used == "dehydrate"

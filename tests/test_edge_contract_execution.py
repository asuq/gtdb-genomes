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
    PartialBatchPayloadResolution,
    ResolvedPayloadDirectory,
    execute_batch_dehydrate_plans,
    execute_accession_plans,
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Return one successful direct batch command."""

        del final_failure_status, sleep_func, runner, stream_runner
        assert stage == "preferred_download"
        assert attempted_accession == "GCF_000001.1"
        assert "--api-key" not in command
        assert environment is not None
        assert environment[NCBI_API_KEY_ENV_VAR] == "secret"
        assert logger is not None
        assert progress_label == "direct_batch_1: preferred_download"
        assert progress_step == 10
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


def test_execute_accession_plans_rejects_unknown_method(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unknown execution methods should fail fast instead of running direct mode."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_direct_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("direct execution should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_batch_dehydrate_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dehydrate execution should not run"),
        ),
    )

    with pytest.raises(ValueError, match="Unsupported download method: invalid"):
        execute_accession_plans(
            (
                AccessionPlan(
                    original_accession="GCF_000001.1",
                    download_request_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                ),
            ),
            build_cli_args(tmp_path / "out"),
            "invalid",
            initialise_run_directories(tmp_path / "invalid-method"),
            logging.getLogger("test-invalid-execution-method"),
            (),
        )


def test_direct_mode_retries_unresolved_accessions_in_later_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A later direct batch should retry only the unresolved request accession."""

    payload_one = tmp_path / "payload-one"
    payload_one.mkdir()
    payload_two = tmp_path / "payload-two"
    payload_two.mkdir()
    download_calls: list[tuple[str, str, str | None]] = []
    extraction_calls: list[tuple[Path, Path]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Return successful preferred and fallback batch commands."""

        del command, final_failure_status, environment, sleep_func, runner
        del logger, progress_step, stream_runner
        download_calls.append((stage, attempted_accession or "", progress_label))
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

        if extraction_root.name in {
            "direct_batch_1",
            "direct_batch_2",
            "direct_batch_3",
            "direct_batch_4",
        }:
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
        (
            "preferred_download",
            "GCA_001881595",
            "direct_batch_1: preferred_download",
        ),
        (
            "preferred_download",
            "GCA_001881595",
            "direct_batch_2: preferred_download",
        ),
        (
            "preferred_download",
            "GCA_001881595",
            "direct_batch_3: preferred_download",
        ),
        (
            "preferred_download",
            "GCA_001881595",
            "direct_batch_4: preferred_download",
        ),
        (
            "fallback_download",
            "GCF_001881595.2",
            "direct_fallback_batch_1: fallback_download",
        ),
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
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_status == "failed"
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_4"
    assert result.executions["GCA_001881595.3"].request_accession_used == (
        "GCA_001881595"
    )
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
    ]
    assert [failure.final_status for failure in result.executions["GCA_001881595.3"].failures] == [
        "retry_scheduled",
        "retry_scheduled",
        "retry_scheduled",
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
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
    assert len(result.shared_failures) == 8
    assert [
        failure.final_status for failure in result.shared_failures[0].failures
    ] == ["retry_scheduled"]
    assert [
        failure.attempted_accession
        for failure in result.shared_failures[0].failures
    ] == ["GCF_000001.1"]
    assert [
        failure.error_type for failure in result.shared_failures[-1].failures
    ] == ["LayoutError"]
    assert [
        failure.final_status for failure in result.shared_failures[-1].failures
    ] == ["retry_exhausted"]


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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Return one unresolved preferred batch and one retried fallback batch."""

        del command, final_failure_status, environment, sleep_func, runner
        nonlocal call_count
        call_count += 1
        if call_count <= 4:
            assert stage == "preferred_download"
            assert attempted_accession == "GCA_001881595"
            return RetryableCommandResult(
                succeeded=True,
                stdout="",
                stderr="",
                failures=(),
            )
        assert call_count == 5
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

        if extraction_root.name in {
            "direct_batch_1",
            "direct_batch_2",
            "direct_batch_3",
            "direct_batch_4",
        }:
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
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
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
    assert result.executions["GCF_001881595.2"].download_batch == "direct_fallback_batch_4"
    assert result.executions["GCF_001881595.2"].request_accession_used == (
        "GCF_001881595.2"
    )
    assert [failure.attempted_accession for failure in result.executions["GCF_001881595.2"].failures] == [
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
        "GCF_001881595.2",
        "GCF_001881595.2",
        "GCF_001881595.2",
        "GCF_001881595.2",
    ]
    assert [failure.final_status for failure in result.executions["GCF_001881595.2"].failures] == [
        "retry_scheduled",
        "retry_scheduled",
        "retry_scheduled",
        "retry_exhausted",
        "retry_scheduled",
        "retry_scheduled",
        "retry_scheduled",
        "retry_exhausted",
    ]
    assert result.executions["GCA_001881595.3"].final_accession is None
    assert result.executions["GCA_001881595.3"].download_batch == "direct_batch_4"
    assert result.executions["GCA_001881595.3"].request_accession_used == (
        "GCA_001881595"
    )
    assert [failure.attempted_accession for failure in result.executions["GCA_001881595.3"].failures] == [
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
        "GCA_001881595",
    ]


def test_direct_mode_waits_for_wave_completion_before_retrying_failed_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failed sibling batches should wait for the whole wave before splitting."""

    payload_one = tmp_path / "payload-one"
    payload_three = tmp_path / "payload-three"
    payload_four = tmp_path / "payload-four"
    payload_one.mkdir()
    payload_three.mkdir()
    payload_four.mkdir()
    download_calls: list[str] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Fail two mixed batches before isolating one bad singleton."""

        del command, final_failure_status, environment, sleep_func, runner
        assert stage == "preferred_download"
        download_calls.append(attempted_accession or "")
        if attempted_accession == (
            "GCF_000001.1;GCF_000002.1;GCF_000003.1;GCF_000004.1"
        ):
            return RetryableCommandResult(
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
            )
        if attempted_accession == "GCF_000001.1;GCF_000002.1":
            return RetryableCommandResult(
                succeeded=False,
                stdout="",
                stderr="left-half failed",
                failures=(
                    CommandFailureRecord(
                        stage="preferred_download",
                        attempt_index=4,
                        max_attempts=4,
                        error_type="subprocess",
                        error_message="left-half failed",
                        final_status="retry_exhausted",
                    ),
                ),
            )
        if attempted_accession == "GCF_000003.1;GCF_000004.1":
            return RetryableCommandResult(
                succeeded=True,
                stdout="",
                stderr="",
                failures=(),
            )
        if attempted_accession == "GCF_000001.1":
            return RetryableCommandResult(
                succeeded=True,
                stdout="",
                stderr="",
                failures=(),
            )
        assert attempted_accession == "GCF_000002.1"
        return RetryableCommandResult(
            succeeded=False,
            stdout="",
            stderr="single failed",
            failures=(
                CommandFailureRecord(
                    stage="preferred_download",
                    attempt_index=4,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="single failed",
                    final_status="retry_exhausted",
                ),
            ),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create extraction roots for successful batches only."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Resolve the right-half batch before the next-wave singleton retry."""

        if extraction_root.name == "direct_batch_3":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000003.1",
                    directory=payload_three,
                ),
                ResolvedPayloadDirectory(
                    final_accession="GCF_000004.1",
                    directory=payload_four,
                ),
            )
        if extraction_root.name == "direct_batch_4":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000001.1",
                    directory=payload_one,
                ),
            )
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

    run_directories = initialise_run_directories(tmp_path / "direct-batch-decompose")
    with caplog.at_level(logging.INFO):
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
                AccessionPlan(
                    original_accession="GCF_000003.1",
                    download_request_accession="GCF_000003.1",
                    conversion_status="unchanged_original",
                ),
                AccessionPlan(
                    original_accession="GCF_000004.1",
                    download_request_accession="GCF_000004.1",
                    conversion_status="unchanged_original",
                ),
            ),
            build_cli_args(tmp_path / "out"),
            run_directories,
            logging.getLogger("test-direct-batch-decompose"),
        )

    assert download_calls == [
        "GCF_000001.1;GCF_000002.1;GCF_000003.1;GCF_000004.1",
        "GCF_000001.1;GCF_000002.1",
        "GCF_000003.1;GCF_000004.1",
        "GCF_000001.1",
        "GCF_000002.1",
        "GCF_000002.1",
    ]
    assert result.executions["GCF_000001.1"].download_status == "downloaded"
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_4"
    assert result.executions["GCF_000002.1"].download_status == "failed"
    assert result.executions["GCF_000002.1"].download_batch == "direct_batch_6"
    assert result.executions["GCF_000003.1"].download_batch == "direct_batch_3"
    assert result.executions["GCF_000004.1"].download_batch == "direct_batch_3"
    assert len(result.shared_failures) == 4
    assert (
        "preferred_download wave 2: starting 2 batch(es) covering 4 request accession(s)"
        in caplog.text
    )
    assert (
        "preferred_download wave 3: starting 2 batch(es) covering 2 request accession(s)"
        in caplog.text
    )


def test_direct_mode_waits_for_wave_completion_before_retrying_partial_batches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Partial direct results should not retry before sibling batches finish."""

    payload_one = tmp_path / "payload-good-layout-1"
    payload_two = tmp_path / "payload-good-layout-2"
    payload_three = tmp_path / "payload-good-layout-3"
    payload_four = tmp_path / "payload-good-layout-4"
    payload_one.mkdir()
    payload_two.mkdir()
    payload_three.mkdir()
    payload_four.mkdir()
    download_calls: list[str] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Succeed all downloads so the wave scheduler decides the retry order."""

        del command, final_failure_status, environment, sleep_func, runner
        assert stage == "preferred_download"
        download_calls.append(attempted_accession or "")
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
        """Create extraction roots for all preferred batches."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Resolve one left-half payload, then the right-half batch, then the retry."""

        if extraction_root.name == "direct_batch_2":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000001.1",
                    directory=payload_one,
                ),
            )
        if extraction_root.name == "direct_batch_3":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000003.1",
                    directory=payload_three,
                ),
                ResolvedPayloadDirectory(
                    final_accession="GCF_000004.1",
                    directory=payload_four,
                ),
            )
        if extraction_root.name == "direct_batch_4":
            return (
                ResolvedPayloadDirectory(
                    final_accession="GCF_000002.1",
                    directory=payload_two,
                ),
            )
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

    run_directories = initialise_run_directories(tmp_path / "direct-layout-decompose")
    with caplog.at_level(logging.INFO):
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
                AccessionPlan(
                    original_accession="GCF_000003.1",
                    download_request_accession="GCF_000003.1",
                    conversion_status="unchanged_original",
                ),
                AccessionPlan(
                    original_accession="GCF_000004.1",
                    download_request_accession="GCF_000004.1",
                    conversion_status="unchanged_original",
                ),
            ),
            build_cli_args(tmp_path / "out"),
            run_directories,
            logging.getLogger("test-direct-layout-decompose"),
        )

    assert result.executions["GCF_000001.1"].download_status == "downloaded"
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_2"
    assert result.executions["GCF_000002.1"].download_status == "downloaded"
    assert result.executions["GCF_000002.1"].download_batch == "direct_batch_4"
    assert [failure.attempted_accession for failure in result.executions["GCF_000002.1"].failures] == [
        "GCF_000002.1",
        "GCF_000002.1",
    ]
    assert result.executions["GCF_000003.1"].download_batch == "direct_batch_3"
    assert result.executions["GCF_000004.1"].download_batch == "direct_batch_3"
    assert download_calls == [
        "GCF_000001.1;GCF_000002.1;GCF_000003.1;GCF_000004.1",
        "GCF_000001.1;GCF_000002.1",
        "GCF_000003.1;GCF_000004.1",
        "GCF_000002.1",
    ]
    assert (
        "preferred_download wave 2: starting 2 batch(es) covering 4 request accession(s)"
        in caplog.text
    )


def test_direct_mode_retries_suppressed_groups_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Suppressed-only direct groups should stop after two total waves."""

    download_calls: list[str] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Succeed both suppressed direct downloads before layout resolution."""

        del command, final_failure_status, environment, sleep_func, runner
        assert stage == "preferred_download"
        download_calls.append(attempted_accession or "")
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
        """Create extraction roots for both suppressed waves."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep the suppressed request unresolved in both waves."""

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

    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCF_000001.1",
                conversion_status="unchanged_original",
                is_suppressed=True,
            ),
        ),
        build_cli_args(tmp_path / "out"),
        initialise_run_directories(tmp_path / "suppressed-direct-budget"),
        logging.getLogger("test-suppressed-direct-budget"),
    )

    assert download_calls == ["GCF_000001.1", "GCF_000001.1"]
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_2"
    assert [failure.max_attempts for failure in result.executions["GCF_000001.1"].failures] == [
        2,
        2,
    ]
    assert [failure.final_status for failure in result.executions["GCF_000001.1"].failures] == [
        "retry_scheduled",
        "retry_exhausted",
    ]


def test_direct_mode_keeps_normal_retry_budget_for_mixed_groups(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed suppressed and normal request groups should use the normal budget."""

    download_calls: list[str] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Succeed all mixed-group downloads before layout resolution."""

        del command, final_failure_status, environment, sleep_func, runner
        assert stage == "preferred_download"
        download_calls.append(attempted_accession or "")
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
        """Create extraction roots for the mixed-group retries."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_collect_payload_directories(
        extraction_root: Path,
    ) -> tuple[ResolvedPayloadDirectory, ...]:
        """Keep the mixed request unresolved in every wave."""

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

    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_000001.1",
                download_request_accession="GCA_000001",
                conversion_status="unchanged_original",
                is_suppressed=True,
            ),
            AccessionPlan(
                original_accession="GCA_000001.3",
                download_request_accession="GCA_000001",
                conversion_status="unchanged_original",
            ),
        ),
        build_cli_args(tmp_path / "out"),
        initialise_run_directories(tmp_path / "mixed-direct-budget"),
        logging.getLogger("test-mixed-direct-budget"),
    )

    assert download_calls == [
        "GCA_000001",
        "GCA_000001",
        "GCA_000001",
        "GCA_000001",
    ]
    assert result.executions["GCF_000001.1"].download_batch == "direct_batch_4"
    assert result.executions["GCA_000001.3"].download_batch == "direct_batch_4"
    assert [failure.max_attempts for failure in result.executions["GCA_000001.3"].failures] == [
        4,
        4,
        4,
        4,
    ]


def test_direct_fallback_retries_suppressed_groups_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Suppressed fallback groups should use the reduced workflow budget."""

    download_calls: list[tuple[str, str]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Succeed preferred and fallback downloads before layout resolution."""

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
        """Create extraction roots for preferred and fallback waves."""

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

    result = execute_direct_accession_plans(
        (
            AccessionPlan(
                original_accession="GCF_001881595.2",
                download_request_accession="GCA_001881595",
                conversion_status="paired_to_gca",
                is_suppressed=True,
            ),
        ),
        build_cli_args(tmp_path / "out"),
        initialise_run_directories(tmp_path / "suppressed-fallback-budget"),
        logging.getLogger("test-suppressed-fallback-budget"),
    )

    assert download_calls == [
        ("preferred_download", "GCA_001881595"),
        ("preferred_download", "GCA_001881595"),
        ("fallback_download", "GCF_001881595.2"),
        ("fallback_download", "GCF_001881595.2"),
    ]
    assert result.executions["GCF_001881595.2"].download_batch == (
        "direct_fallback_batch_2"
    )
    assert [failure.max_attempts for failure in result.executions["GCF_001881595.2"].failures] == [
        2,
        2,
        2,
        2,
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


def test_batch_dehydrate_preserves_partial_success_before_direct_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Resolved dehydrated payloads should be kept when only part of the cohort fails."""

    payload_directory = tmp_path / "dehydrate-payload"
    payload_directory.mkdir()
    fallback_calls: list[tuple[str, ...]] = []
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

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Succeed the batch download but fail rehydrate after extraction."""

        del command, final_failure_status, environment, sleep_func, runner, attempted_accession
        if stage == "preferred_download":
            return RetryableCommandResult(
                succeeded=True,
                stdout="",
                stderr="",
                failures=(),
            )
        assert stage == "rehydrate"
        return RetryableCommandResult(
            succeeded=False,
            stdout="",
            stderr="rehydrate failed",
            failures=(
                CommandFailureRecord(
                    stage="rehydrate",
                    attempt_index=4,
                    max_attempts=4,
                    error_type="subprocess",
                    error_message="rehydrate failed",
                    final_status="retry_exhausted",
                ),
            ),
        )

    def fake_extract_archive(
        archive_path: Path,
        extraction_root: Path,
    ) -> None:
        """Create the extraction root for partial dehydrated resolution."""

        del archive_path
        extraction_root.mkdir(parents=True, exist_ok=True)

    def fake_locate_partial_batch_payload_directories(
        extraction_root: Path,
        requested_accessions: tuple[str, ...],
    ):
        """Resolve only one dehydrated payload and leave one unresolved."""

        assert extraction_root.name == "dehydrated_batch"
        assert requested_accessions == ("GCA_000001", "GCA_000002")
        return type(
            "PartialResolution",
            (),
            {
                "resolved_payloads": {
                    "GCA_000001": ResolvedPayloadDirectory(
                        final_accession="GCA_000001.3",
                        directory=payload_directory,
                    ),
                },
                "unresolved_messages": {
                    "GCA_000002": (
                        "Could not locate extracted payload directory for requested accession GCA_000002"
                    ),
                },
            },
        )()

    def fake_execute_direct_accession_plans(
        plans: tuple[AccessionPlan, ...],
        args: CliArgs,
        run_directories,
        logger,
    ) -> DownloadExecutionResult:
        """Resolve only the unresolved dehydrated plan through direct fallback."""

        del args, run_directories, logger
        fallback_calls.append(tuple(plan.original_accession for plan in plans))
        return DownloadExecutionResult(
            executions={
                "GCF_000002.1": AccessionExecution(
                    original_accession="GCF_000002.1",
                    final_accession="GCF_000002.1",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=tmp_path,
                    failures=(),
                    request_accession_used="GCF_000002.1",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
            shared_failures=(),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.run_retryable_command",
        fake_run_retryable_command,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.extract_archive",
        fake_extract_archive,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.locate_partial_batch_payload_directories",
        fake_locate_partial_batch_payload_directories,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution_dehydrate.execute_direct_accession_plans",
        fake_execute_direct_accession_plans,
    )

    result = execute_batch_dehydrate_plans(
        plans,
        build_cli_args(tmp_path / "out"),
        initialise_run_directories(tmp_path / "dehydrate-partial-fallback"),
        logging.getLogger("test-dehydrate-partial-fallback"),
        (),
    )

    assert fallback_calls == [("GCF_000002.1",)]
    assert result.method_used == "dehydrate_fallback_direct"
    assert result.executions["GCF_000001.1"].download_status == "downloaded"
    assert result.executions["GCF_000001.1"].download_batch == "dehydrated_batch"
    assert result.executions["GCF_000002.1"].download_status == "downloaded_after_fallback"


def test_batch_dehydrate_passes_api_key_via_child_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dehydrated download and rehydrate should use the child API-key environment."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    observed_calls: list[tuple[str, dict[str, str] | None, str | None]] = []

    def fake_run_retryable_command(
        command: list[str],
        stage: str,
        final_failure_status: str = "retry_exhausted",
        attempted_accession: str | None = None,
        environment: dict[str, str] | None = None,
        sleep_func=None,
        runner=None,
        logger=None,
        progress_label: str | None = None,
        progress_step: int = 10,
        stream_runner=None,
    ) -> RetryableCommandResult:
        """Return successful dehydrated download and rehydrate stages."""

        del final_failure_status, attempted_accession, sleep_func, runner
        del logger, progress_step, stream_runner
        assert "--api-key" not in command
        observed_calls.append((stage, environment, progress_label))
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
        "gtdb_genomes.workflow_execution_dehydrate.locate_partial_batch_payload_directories",
        lambda extraction_root, requested_accessions: PartialBatchPayloadResolution(
            resolved_payloads={
                requested_accession: ResolvedPayloadDirectory(
                    final_accession=f"{requested_accession}.1",
                    directory=payload_directory,
                )
                for requested_accession in requested_accessions
            },
            unresolved_messages={},
        ),
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

    assert [stage for stage, _, _ in observed_calls] == [
        "preferred_download",
        "rehydrate",
    ]
    assert [
        environment[NCBI_API_KEY_ENV_VAR]
        for _, environment, _ in observed_calls
    ] == [
        "secret",
        "secret",
    ]
    assert [progress_label for _, _, progress_label in observed_calls] == [
        "dehydrated_batch: preferred_download",
        "dehydrated_batch: rehydrate",
    ]
    assert result.method_used == "dehydrate"

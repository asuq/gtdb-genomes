"""NCBI genome download command construction and planning."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
import subprocess
import time

from gtdb_genomes.subprocess_utils import (
    DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    build_spawn_error_message,
    build_subprocess_error_message,
    build_timeout_error_message,
)


DEHYDRATE_ACCESSION_THRESHOLD = 1000
REHYDRATE_WORKER_CAP = 30
RETRY_DELAYS_SECONDS = (5, 15, 45)
DEFAULT_REQUESTED_DOWNLOAD_METHOD = "auto"
SUPPORTED_INCLUDE_TOKENS = frozenset({"genome", "gff3", "protein"})


@dataclass(slots=True)
class PreviewError(Exception):
    """Raised when the datasets preview command fails or cannot be parsed."""

    message: str
    failures: tuple["CommandFailureRecord", ...] = ()

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


@dataclass(slots=True)
class PreviewCommandResult:
    """Structured preview output plus preserved retry provenance."""

    preview_text: str
    failures: tuple["CommandFailureRecord", ...]


@dataclass(slots=True)
class DownloadMethodDecision:
    """Resolved download mode decision for a requested accession set."""

    requested_method: str
    method_used: str
    accession_count: int
    preview_size_bytes: int | None


@dataclass(slots=True)
class CommandFailureRecord:
    """One failed retryable command attempt."""

    stage: str
    attempt_index: int
    max_attempts: int
    error_type: str
    error_message: str
    final_status: str
    attempted_accession: str | None = None


@dataclass(slots=True)
class RetryableCommandResult:
    """The result of a retryable subprocess command."""

    succeeded: bool
    stdout: str
    stderr: str
    failures: tuple[CommandFailureRecord, ...]


def get_ordered_unique_accessions(
    accessions: Iterable[str],
) -> tuple[str, ...]:
    """Return first-seen unique accessions in deterministic order."""

    return tuple(dict.fromkeys(accessions))


def validate_include_value(include: str) -> str:
    """Normalise and validate a datasets include value."""

    tokens: list[str] = []
    for raw_token in include.split(","):
        token = raw_token.strip()
        if not token:
            raise ValueError("argument --include: values must not be empty")
        if token not in SUPPORTED_INCLUDE_TOKENS:
            raise ValueError(
                "argument --include: unsupported include token "
                f"{token!r}; supported values are genome, gff3, protein",
            )
        tokens.append(token)
    if "genome" not in tokens:
        raise ValueError("argument --include: value must contain 'genome'")
    return ",".join(tokens)


def build_preview_command(
    accession_file: Path,
    include: str,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a datasets preview command for genome accessions from a file."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(accession_file),
        "--include",
        validate_include_value(include),
        "--preview",
    ]
    if ncbi_api_key:
        command.extend(["--api-key", ncbi_api_key])
    if debug:
        command.append("--debug")
    return command


def build_direct_batch_download_command(
    accession_file: Path,
    archive_path: Path,
    include: str,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a non-dehydrated batch datasets genome download command."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(accession_file),
        "--filename",
        str(archive_path),
        "--include",
        validate_include_value(include),
    ]
    if ncbi_api_key:
        command.extend(["--api-key", ncbi_api_key])
    if debug:
        command.append("--debug")
    return command


def build_batch_dehydrate_command(
    accession_file: Path,
    archive_path: Path,
    include: str,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a batch dehydrated datasets download command."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        "--inputfile",
        str(accession_file),
        "--filename",
        str(archive_path),
        "--include",
        validate_include_value(include),
        "--dehydrated",
    ]
    if ncbi_api_key:
        command.extend(["--api-key", ncbi_api_key])
    if debug:
        command.append("--debug")
    return command


def write_accession_input_file(
    path: Path,
    accessions: Iterable[str],
) -> Path:
    """Write a datasets accession input file in deterministic order."""

    ordered_accessions = get_ordered_unique_accessions(accessions)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(f"{accession}\n" for accession in ordered_accessions),
        encoding="ascii",
    )
    return path


def run_preview_command(
    accession_file: Path,
    include: str,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
    sleep_func: Callable[[float], None] = time.sleep,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> PreviewCommandResult:
    """Run `datasets --preview` and return its output plus retry history."""

    command = build_preview_command(
        accession_file,
        include,
        ncbi_api_key=ncbi_api_key,
        datasets_bin=datasets_bin,
        debug=debug,
    )
    result = run_retryable_command(
        command,
        stage="preview",
        sleep_func=sleep_func,
        runner=runner,
    )
    if result.succeeded:
        return PreviewCommandResult(
            preview_text=result.stdout,
            failures=result.failures,
        )
    error_message = result.stderr.strip() or result.stdout.strip()
    if not error_message and result.failures:
        error_message = result.failures[-1].error_message
    if not error_message:
        error_message = "datasets preview failed"
    raise PreviewError(error_message, failures=result.failures)


def build_rehydrate_command(
    directory: Path,
    max_workers: int,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a datasets rehydrate command for an extracted dehydrated bag."""

    command = [
        datasets_bin,
        "rehydrate",
        "--directory",
        str(directory),
        "--max-workers",
        str(max_workers),
    ]
    if ncbi_api_key:
        command.extend(["--api-key", ncbi_api_key])
    if debug:
        command.append("--debug")
    return command


def select_download_method(
    accession_count: int,
) -> DownloadMethodDecision:
    """Resolve the effective download method from the request-token count."""

    method_used = "direct"
    if accession_count > DEHYDRATE_ACCESSION_THRESHOLD:
        method_used = "dehydrate"
    return DownloadMethodDecision(
        requested_method=DEFAULT_REQUESTED_DOWNLOAD_METHOD,
        method_used=method_used,
        accession_count=accession_count,
        preview_size_bytes=None,
    )


def get_rehydrate_workers(threads: int) -> int:
    """Return the allowed datasets rehydrate worker count."""

    return max(1, min(threads, REHYDRATE_WORKER_CAP))


def run_retryable_command(
    command: list[str],
    stage: str,
    final_failure_status: str = "retry_exhausted",
    attempted_accession: str | None = None,
    sleep_func: Callable[[float], None] = time.sleep,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> RetryableCommandResult:
    """Run a retryable subprocess command with the fixed retry schedule."""

    command_runner = subprocess.run if runner is None else runner
    max_attempts = len(RETRY_DELAYS_SECONDS) + 1
    failures: list[CommandFailureRecord] = []
    for attempt_index in range(1, max_attempts + 1):
        stdout = ""
        stderr = ""
        retry_allowed = attempt_index < max_attempts
        try:
            result = command_runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            error_type = "timeout"
            error_message = build_timeout_error_message(
                stage,
                DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
            )
        except OSError as error:
            error_type = "spawn_error"
            error_message = build_spawn_error_message(stage, error)
            retry_allowed = False
        else:
            stdout = result.stdout
            stderr = result.stderr
            if result.returncode == 0:
                return RetryableCommandResult(
                    succeeded=True,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    failures=tuple(failures),
                )
            error_type = "subprocess"
            error_message = build_subprocess_error_message(stage, result)

        if retry_allowed:
            failures.append(
                CommandFailureRecord(
                    stage=stage,
                    attempt_index=attempt_index,
                    max_attempts=max_attempts,
                    error_type=error_type,
                    error_message=error_message,
                    final_status="retry_scheduled",
                    attempted_accession=attempted_accession,
                ),
            )
            sleep_func(RETRY_DELAYS_SECONDS[attempt_index - 1])
            continue
        failures.append(
            CommandFailureRecord(
                stage=stage,
                attempt_index=attempt_index,
                max_attempts=max_attempts,
                error_type=error_type,
                error_message=error_message,
                final_status=final_failure_status,
                attempted_accession=attempted_accession,
            ),
        )
        return RetryableCommandResult(
            succeeded=False,
            stdout=stdout,
            stderr=stderr,
            failures=tuple(failures),
        )
    raise RuntimeError("Internal error: retry loop terminated unexpectedly")

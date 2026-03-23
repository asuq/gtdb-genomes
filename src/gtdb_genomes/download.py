"""NCBI genome download command construction and automatic planning."""

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
class DownloadMethodDecision:
    """Resolved download mode decision for a requested accession set."""

    method_used: str
    accession_count: int


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


def build_direct_batch_download_command(
    accession_file: Path,
    archive_path: Path,
    include: str,
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
    if debug:
        command.append("--debug")
    return command


def build_batch_dehydrate_command(
    accession_file: Path,
    archive_path: Path,
    include: str,
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


def build_rehydrate_command(
    directory: Path,
    max_workers: int,
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
    if debug:
        command.append("--debug")
    return command


def select_download_method(
    accession_count: int,
) -> DownloadMethodDecision:
    """Resolve the effective download method from the request-token count."""

    method_used = "direct"
    if accession_count >= DEHYDRATE_ACCESSION_THRESHOLD:
        method_used = "dehydrate"
    return DownloadMethodDecision(
        method_used=method_used,
        accession_count=accession_count,
    )


def get_rehydrate_workers(threads: int) -> int:
    """Return the allowed datasets rehydrate worker count."""

    return max(1, min(threads, REHYDRATE_WORKER_CAP))


def run_retryable_command(
    command: list[str],
    stage: str,
    final_failure_status: str = "retry_exhausted",
    attempted_accession: str | None = None,
    environment: dict[str, str] | None = None,
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
                env=environment,
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

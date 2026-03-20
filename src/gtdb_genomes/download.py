"""NCBI genome download command construction and planning."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import json
from pathlib import Path
import re
import subprocess
import time

from gtdb_genomes.subprocess_utils import (
    DEFAULT_SUBPROCESS_TIMEOUT_SECONDS,
    build_spawn_error_message,
    build_subprocess_error_message,
    build_timeout_error_message,
)


DEHYDRATE_ACCESSION_THRESHOLD = 1000
DEHYDRATE_SIZE_GB_THRESHOLD = 15.0
DIRECT_DOWNLOAD_CONCURRENCY_CAP = 5
REHYDRATE_WORKER_CAP = 30
RETRY_DELAYS_SECONDS = (5, 15, 45)
SIZE_PATTERN = re.compile(r"(?i)(\d+(?:\.\d+)?)\s*([KMGT]?B)\b")
SIZE_UNITS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}


@dataclass(slots=True)
class PreviewError(Exception):
    """Raised when the datasets preview command fails or cannot be parsed."""

    message: str

    def __str__(self) -> str:
        """Return the human-readable exception message."""

        return self.message


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


def build_download_command(
    accessions: Iterable[str],
    archive_path: Path,
    include: str,
    ncbi_api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a direct datasets genome download command."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        *get_ordered_unique_accessions(accessions),
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
) -> str:
    """Run `datasets --preview` and return its raw stdout."""

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
        return result.stdout
    error_message = result.stderr.strip() or result.stdout.strip()
    if not error_message and result.failures:
        error_message = result.failures[-1].error_message
    if not error_message:
        error_message = "datasets preview failed"
    raise PreviewError(error_message)


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


def parse_preview_size_bytes(preview_text: str) -> int | None:
    """Extract the package or download size from preview output."""

    stripped_preview = preview_text.strip()
    if not stripped_preview:
        return None
    json_sizes_mb: list[float] = []
    for line in stripped_preview.splitlines():
        if not line.strip().startswith("{"):
            continue
        try:
            preview_payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        estimated_size_mb = preview_payload.get("estimated_file_size_mb")
        if isinstance(estimated_size_mb, int | float):
            json_sizes_mb.append(float(estimated_size_mb))
            continue
        included_data_files = preview_payload.get("included_data_files", {})
        if not isinstance(included_data_files, dict):
            continue
        record_total_mb = 0.0
        found_file_size = False
        for file_metadata in included_data_files.values():
            if not isinstance(file_metadata, dict):
                continue
            size_mb = file_metadata.get("size_mb")
            if isinstance(size_mb, int | float):
                record_total_mb += float(size_mb)
                found_file_size = True
        if found_file_size:
            json_sizes_mb.append(record_total_mb)
    if json_sizes_mb:
        return int(max(json_sizes_mb) * SIZE_UNITS["MB"])

    labelled_matches = re.findall(
        r"(?im)^\s*(?:package|download)\s+size\s*:\s*(\d+(?:\.\d+)?)\s*([KMGT]?B)\b",
        preview_text,
    )
    if labelled_matches:
        return max(
            int(float(size_value) * SIZE_UNITS[size_unit.upper()])
            for size_value, size_unit in labelled_matches
        )

    matches = SIZE_PATTERN.findall(preview_text)
    if len(matches) != 1:
        return None
    size_value, size_unit = matches[0]
    return int(float(size_value) * SIZE_UNITS[size_unit.upper()])


def select_download_method(
    requested_method: str,
    accession_count: int,
    preview_text: str | None = None,
) -> DownloadMethodDecision:
    """Resolve the effective download method for a request."""

    if requested_method != "auto":
        return DownloadMethodDecision(
            requested_method=requested_method,
            method_used=requested_method,
            accession_count=accession_count,
            preview_size_bytes=None,
        )
    if preview_text is None:
        raise PreviewError("datasets preview output is required in auto mode")
    preview_size_bytes = parse_preview_size_bytes(preview_text)
    if preview_size_bytes is None:
        raise PreviewError("could not parse datasets preview output")
    method_used = "direct"
    if (
        accession_count >= DEHYDRATE_ACCESSION_THRESHOLD
        or preview_size_bytes > int(DEHYDRATE_SIZE_GB_THRESHOLD * (1024**3))
    ):
        method_used = "dehydrate"
    return DownloadMethodDecision(
        requested_method=requested_method,
        method_used=method_used,
        accession_count=accession_count,
        preview_size_bytes=preview_size_bytes,
    )


def get_direct_download_concurrency(threads: int, accession_count: int) -> int:
    """Return the allowed direct-download job concurrency for a request."""

    if accession_count <= 0:
        return 0
    return min(threads, DIRECT_DOWNLOAD_CONCURRENCY_CAP, accession_count)


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
    raise AssertionError("retry loop terminated unexpectedly")

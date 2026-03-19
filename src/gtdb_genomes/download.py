"""NCBI genome download command construction and planning."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import time


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


@dataclass(slots=True)
class AccessionDownloadResult:
    """The result of downloading one accession with optional fallback."""

    succeeded: bool
    used_accession: str | None
    used_fallback: bool
    failures: tuple[CommandFailureRecord, ...]


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
    accessions: Iterable[str],
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
) -> list[str]:
    """Build a datasets preview command for genome accessions."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        *dict.fromkeys(accessions),
        "--include",
        validate_include_value(include),
        "--preview",
    ]
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def build_download_command(
    accessions: Iterable[str],
    archive_path: Path,
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    dehydrated: bool = False,
    debug: bool = False,
) -> list[str]:
    """Build a direct or dehydrated datasets genome download command."""

    command = [
        datasets_bin,
        "download",
        "genome",
        "accession",
        *dict.fromkeys(accessions),
        "--filename",
        str(archive_path),
        "--include",
        validate_include_value(include),
    ]
    if dehydrated:
        command.append("--dehydrated")
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def build_batch_dehydrate_command(
    accession_file: Path,
    archive_path: Path,
    include: str,
    api_key: str | None = None,
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
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def write_accession_input_file(
    path: Path,
    accessions: Iterable[str],
) -> Path:
    """Write a datasets accession input file in deterministic order."""

    ordered_accessions = tuple(dict.fromkeys(accessions))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(f"{accession}\n" for accession in ordered_accessions),
        encoding="ascii",
    )
    return path


def run_preview_command(
    accessions: Iterable[str],
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    debug: bool = False,
    sleep_func: Callable[[float], None] = time.sleep,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> str:
    """Run `datasets --preview` and return its raw stdout."""

    command = build_preview_command(
        accessions,
        include,
        api_key=api_key,
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
    api_key: str | None = None,
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
    if api_key:
        command.extend(["--api-key", api_key])
    if debug:
        command.append("--debug")
    return command


def parse_preview_size_bytes(preview_text: str) -> int | None:
    """Extract the largest size value from preview output."""

    matches = SIZE_PATTERN.findall(preview_text)
    if not matches:
        return None
    sizes = [
        int(float(size_value) * SIZE_UNITS[size_unit.upper()])
        for size_value, size_unit in matches
    ]
    return max(sizes)


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
        result = command_runner(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return RetryableCommandResult(
                succeeded=True,
                stdout=result.stdout,
                stderr=result.stderr,
                failures=tuple(failures),
            )
        if attempt_index < max_attempts:
            failures.append(
                CommandFailureRecord(
                    stage=stage,
                    attempt_index=attempt_index,
                    max_attempts=max_attempts,
                    error_type="subprocess",
                    error_message=result.stderr.strip() or result.stdout.strip(),
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
                error_type="subprocess",
                error_message=result.stderr.strip() or result.stdout.strip(),
                final_status=final_failure_status,
                attempted_accession=attempted_accession,
            ),
        )
        return RetryableCommandResult(
            succeeded=False,
            stdout=result.stdout,
            stderr=result.stderr,
            failures=tuple(failures),
        )
    raise AssertionError("retry loop terminated unexpectedly")


def download_with_accession_fallback(
    preferred_accession: str,
    fallback_accession: str | None,
    archive_path: Path,
    include: str,
    api_key: str | None = None,
    datasets_bin: str = "datasets",
    dehydrated: bool = False,
    debug: bool = False,
    sleep_func: Callable[[float], None] = time.sleep,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> AccessionDownloadResult:
    """Download one accession and fall back to the original if needed."""

    preferred_result = run_retryable_command(
        build_download_command(
            [preferred_accession],
            archive_path,
            include,
            api_key=api_key,
            datasets_bin=datasets_bin,
            dehydrated=dehydrated,
            debug=debug,
        ),
        stage="preferred_download",
        attempted_accession=preferred_accession,
        sleep_func=sleep_func,
        runner=runner,
    )
    if preferred_result.succeeded:
        return AccessionDownloadResult(
            succeeded=True,
            used_accession=preferred_accession,
            used_fallback=False,
            failures=preferred_result.failures,
        )
    if fallback_accession is None or fallback_accession == preferred_accession:
        return AccessionDownloadResult(
            succeeded=False,
            used_accession=None,
            used_fallback=False,
            failures=preferred_result.failures,
        )
    fallback_result = run_retryable_command(
        build_download_command(
            [fallback_accession],
            archive_path,
            include,
            api_key=api_key,
            datasets_bin=datasets_bin,
            dehydrated=dehydrated,
            debug=debug,
        ),
        stage="fallback_download",
        final_failure_status="fallback_exhausted",
        attempted_accession=fallback_accession,
        sleep_func=sleep_func,
        runner=runner,
    )
    return AccessionDownloadResult(
        succeeded=fallback_result.succeeded,
        used_accession=fallback_accession if fallback_result.succeeded else None,
        used_fallback=fallback_result.succeeded,
        failures=preferred_result.failures + fallback_result.failures,
    )

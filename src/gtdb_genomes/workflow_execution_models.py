"""Shared data models for workflow execution."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from gtdb_genomes.download import CommandFailureRecord


@dataclass(slots=True)
class AccessionPlan:
    """One unique accession to resolve and download for the run."""

    original_accession: str
    download_request_accession: str
    conversion_status: str


@dataclass(slots=True)
class AccessionExecution:
    """The materialised download outcome for one accession plan."""

    original_accession: str
    final_accession: str | None
    conversion_status: str
    download_status: str
    download_batch: str
    payload_directory: Path | None
    failures: tuple[CommandFailureRecord, ...]
    request_accession_used: str = ""


@dataclass(slots=True)
class DownloadExecutionResult:
    """The realised download execution details for one run."""

    executions: dict[str, AccessionExecution]
    method_used: str
    download_concurrency_used: int
    rehydrate_workers_used: int
    shared_failures: tuple["SharedFailureContext", ...] = ()


@dataclass(frozen=True, slots=True)
class ResolvedPayloadDirectory:
    """The extracted payload directory and its realised accession."""

    final_accession: str
    directory: Path


@dataclass(slots=True)
class SharedFailureContext:
    """Shared failure history scoped to one affected accession subset."""

    affected_original_accessions: tuple[str, ...]
    failures: tuple[CommandFailureRecord, ...]


@dataclass(slots=True)
class PartialBatchPayloadResolution:
    """Resolved and unresolved payloads for one extracted batch archive."""

    resolved_payloads: dict[str, ResolvedPayloadDirectory]
    unresolved_messages: dict[str, str]


@dataclass(slots=True)
class DirectBatchPhaseResult:
    """Accumulated results from one direct batch phase."""

    executions: dict[str, AccessionExecution]
    unresolved_groups: tuple[tuple[str, tuple[AccessionPlan, ...]], ...]
    shared_failures: tuple[SharedFailureContext, ...]

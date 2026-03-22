"""Tests for workflow planning behaviour."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.download import CommandFailureRecord
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    MetadataLookupError,
    SummaryLookupResult,
)
from gtdb_genomes.workflow_execution import SharedFailureContext
from gtdb_genomes.workflow_planning import (
    build_suppressed_accession_notes,
    resolve_supported_accession_preferences,
)
from tests.workflow_contract_helpers import build_cli_args


def test_resolve_supported_accession_preferences_skips_metadata_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Metadata lookup should stay out of the default non-GenBank path."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("metadata lookup should not run"),
        ),
    )
    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia"],
            "taxon_slug": ["g__Escherichia"],
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
            "lineage": ["d__Bacteria;p__Proteobacteria;g__Escherichia"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_shared_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output", prefer_genbank=False),
            logging.getLogger("test-planning-skip-metadata"),
            (),
        )
    )

    assert metadata_shared_failures == ()
    assert suppressed_notes == {}
    assert mapped_frame.select(
        "final_accession",
        "conversion_status",
    ).rows(named=True) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "unchanged_original",
        },
    ]


def test_resolve_supported_accession_preferences_falls_back_when_candidate_metadata_is_partial(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Partial paired-GCA metadata should keep the original accession."""

    lookup_calls: list[tuple[str, ...]] = []

    def fake_run_summary_lookup_with_retries(
        accessions,
        accession_file,
        ncbi_api_key=None,
        datasets_bin="datasets",
        sleep_func=None,
        runner=None,
    ) -> SummaryLookupResult:
        """Return one supported lookup and one partial paired-GCA lookup."""

        del accession_file, ncbi_api_key, datasets_bin, sleep_func, runner
        ordered_accessions = tuple(accessions)
        lookup_calls.append(ordered_accessions)
        if len(lookup_calls) == 1:
            return SummaryLookupResult(
                summary_map={
                    "GCF_000001.1": {
                        "GCF_000001.1",
                        "GCA_000001.2",
                        "GCA_000001.3",
                    },
                },
                status_map={
                    "GCF_000001.1": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession=None,
                        paired_assembly_status=None,
                    ),
                },
                failures=(),
            )
        return SummaryLookupResult(
            summary_map={
                "GCA_000001.2": {"GCA_000001.2"},
                "GCA_000001.3": {"GCA_000001.3"},
            },
            status_map={
                "GCA_000001.2": AssemblyStatusInfo(
                    assembly_status="current",
                    suppression_reason=None,
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            },
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="partial paired-GCA metadata",
                    final_status="retry_exhausted",
                    attempted_accession="GCA_000001.2;GCA_000001.3",
                ),
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia"],
            "taxon_slug": ["g__Escherichia"],
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
            "lineage": ["d__Bacteria;p__Proteobacteria;g__Escherichia"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_shared_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output"),
            logging.getLogger("test-planning-partial-candidate-metadata"),
            (),
        )
    )

    assert lookup_calls[0] == ("GCF_000001.1",)
    assert set(lookup_calls[1]) == {"GCA_000001.2", "GCA_000001.3"}
    assert metadata_shared_failures[0].affected_original_accessions == (
        "GCF_000001.1",
    )
    assert (
        metadata_shared_failures[0].failures[0].attempted_accession
        == "GCA_000001.2;GCA_000001.3"
    )
    assert set(
        metadata_shared_failures[0].failures[0].attempted_accession.split(";"),
    ) == {
        "GCA_000001.2",
        "GCA_000001.3",
    }
    assert suppressed_notes == {}
    assert mapped_frame.select("final_accession", "conversion_status").rows(
        named=True,
    ) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "paired_gca_metadata_incomplete_fallback_original",
        },
    ]


def test_resolve_supported_accession_preferences_falls_back_when_candidate_lookup_silently_omits_requested_gca(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Silent candidate omissions should keep the original accession."""

    lookup_calls: list[tuple[str, ...]] = []

    def fake_run_summary_lookup_with_retries(
        accessions,
        accession_file,
        ncbi_api_key=None,
        datasets_bin="datasets",
        sleep_func=None,
        runner=None,
    ) -> SummaryLookupResult:
        """Return one complete first lookup and one silent second omission."""

        del accession_file, ncbi_api_key, datasets_bin, sleep_func, runner
        ordered_accessions = tuple(accessions)
        lookup_calls.append(ordered_accessions)
        if len(lookup_calls) == 1:
            return SummaryLookupResult(
                summary_map={
                    "GCF_000001.1": {
                        "GCF_000001.1",
                        "GCA_000001.2",
                        "GCA_000001.3",
                    },
                },
                status_map={
                    "GCF_000001.1": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession=None,
                        paired_assembly_status=None,
                    ),
                },
                failures=(),
            )
        return SummaryLookupResult(
            summary_map={
                "GCA_000001.2": {"GCA_000001.2"},
            },
            status_map={
                "GCA_000001.2": AssemblyStatusInfo(
                    assembly_status="current",
                    suppression_reason=None,
                    paired_accession=None,
                    paired_assembly_status=None,
                ),
            },
            incomplete_accessions=("GCA_000001.3",),
            failures=(),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia"],
            "taxon_slug": ["g__Escherichia"],
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
            "lineage": ["d__Bacteria;p__Proteobacteria;g__Escherichia"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_shared_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output"),
            logging.getLogger("test-planning-silent-candidate-omission"),
            (),
        )
    )

    assert lookup_calls[0] == ("GCF_000001.1",)
    assert set(lookup_calls[1]) == {"GCA_000001.2", "GCA_000001.3"}
    assert metadata_shared_failures == ()
    assert suppressed_notes == {}
    assert mapped_frame.select("final_accession", "conversion_status").rows(
        named=True,
    ) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "paired_gca_metadata_incomplete_fallback_original",
        },
    ]


def test_resolve_supported_accession_preferences_falls_back_when_candidate_lookup_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Candidate lookup errors should degrade to the original accession."""

    lookup_calls: list[tuple[str, ...]] = []

    def fake_run_summary_lookup_with_retries(
        accessions,
        accession_file,
        ncbi_api_key=None,
        datasets_bin="datasets",
        sleep_func=None,
        runner=None,
    ) -> SummaryLookupResult:
        """Return one successful lookup, then one candidate-lookup error."""

        del accession_file, ncbi_api_key, datasets_bin, sleep_func, runner
        ordered_accessions = tuple(accessions)
        lookup_calls.append(ordered_accessions)
        if len(lookup_calls) == 1:
            return SummaryLookupResult(
                summary_map={
                    "GCF_000001.1": {
                        "GCF_000001.1",
                        "GCA_000001.2",
                    },
                },
                status_map={
                    "GCF_000001.1": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession="GCA_000001.2",
                        paired_assembly_status=None,
                    ),
                },
                failures=(),
            )
        raise MetadataLookupError(
            "candidate lookup failed",
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="candidate lookup failed",
                    final_status="retry_scheduled",
                    attempted_accession="GCA_000001.2",
                ),
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia"],
            "taxon_slug": ["g__Escherichia"],
            "gtdb_accession": ["RS_GCF_000001.1"],
            "ncbi_accession": ["GCF_000001.1"],
            "lineage": ["d__Bacteria;p__Proteobacteria;g__Escherichia"],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_shared_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output"),
            logging.getLogger("test-planning-candidate-metadata-error"),
            (),
        )
    )

    assert lookup_calls == [
        ("GCF_000001.1",),
        ("GCA_000001.2",),
    ]
    assert metadata_shared_failures == (
        SharedFailureContext(
            affected_original_accessions=("GCF_000001.1",),
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="candidate lookup failed",
                    final_status="retry_scheduled",
                    attempted_accession="GCA_000001.2",
                ),
            ),
        ),
    )
    assert suppressed_notes == {}
    assert mapped_frame.select("final_accession", "conversion_status").rows(
        named=True,
    ) == [
        {
            "final_accession": "GCF_000001.1",
            "conversion_status": "paired_gca_metadata_incomplete_fallback_original",
        },
    ]


def test_resolve_supported_accession_preferences_scopes_candidate_lookup_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Candidate lookup failures should scope only to affected originals."""

    lookup_calls: list[tuple[str, ...]] = []

    def fake_run_summary_lookup_with_retries(
        accessions,
        accession_file,
        ncbi_api_key=None,
        datasets_bin="datasets",
        sleep_func=None,
        runner=None,
    ) -> SummaryLookupResult:
        """Return one mixed supported lookup, then one scoped candidate failure."""

        del accession_file, ncbi_api_key, datasets_bin, sleep_func, runner
        ordered_accessions = tuple(accessions)
        lookup_calls.append(ordered_accessions)
        if len(lookup_calls) == 1:
            return SummaryLookupResult(
                summary_map={
                    "GCF_000001.1": {"GCF_000001.1", "GCA_000001.2"},
                    "GCF_000002.1": {"GCF_000002.1"},
                },
                status_map={
                    "GCF_000001.1": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession="GCA_000001.2",
                        paired_assembly_status=None,
                    ),
                    "GCF_000002.1": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession=None,
                        paired_assembly_status=None,
                    ),
                },
                failures=(),
            )
        raise MetadataLookupError(
            "candidate lookup failed",
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="candidate lookup failed",
                    final_status="retry_scheduled",
                    attempted_accession="GCA_000001.2",
                ),
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    supported_selected_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Escherichia", "g__Bacillus"],
            "taxon_slug": ["g__Escherichia", "g__Bacillus"],
            "gtdb_accession": ["RS_GCF_000001.1", "RS_GCF_000002.1"],
            "ncbi_accession": ["GCF_000001.1", "GCF_000002.1"],
            "lineage": [
                "d__Bacteria;p__Proteobacteria;g__Escherichia",
                "d__Bacteria;p__Firmicutes;g__Bacillus",
            ],
            "taxonomy_file": ["bac120_taxonomy_r95.tsv", "bac120_taxonomy_r95.tsv"],
        },
    )

    mapped_frame, metadata_shared_failures, suppressed_notes = (
        resolve_supported_accession_preferences(
            supported_selected_frame,
            build_cli_args(tmp_path / "output"),
            logging.getLogger("test-planning-candidate-metadata-scope"),
            (),
        )
    )

    assert lookup_calls == [
        ("GCF_000001.1", "GCF_000002.1"),
        ("GCA_000001.2",),
    ]
    assert metadata_shared_failures == (
        SharedFailureContext(
            affected_original_accessions=("GCF_000001.1",),
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="candidate lookup failed",
                    final_status="retry_scheduled",
                    attempted_accession="GCA_000001.2",
                ),
            ),
        ),
    )
    assert suppressed_notes == {}
    assert mapped_frame.select(
        "ncbi_accession",
        "final_accession",
        "conversion_status",
    ).rows(named=True) == [
        {
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
            "conversion_status": "paired_gca_metadata_incomplete_fallback_original",
        },
        {
            "ncbi_accession": "GCF_000002.1",
            "final_accession": "GCF_000002.1",
            "conversion_status": "unchanged_original",
        },
    ]


def test_build_suppressed_accession_notes_prefers_selected_accession_status() -> None:
    """Suppression warnings should use the selected accession status when known."""

    mapped_frame = pl.DataFrame(
        {
            "ncbi_accession": ["GCF_000001.1"],
            "final_accession": ["GCA_000001.3"],
            "conversion_status": ["paired_to_gca"],
        },
    )

    notes = build_suppressed_accession_notes(
        mapped_frame,
        {
            "GCF_000001.1": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession="GCA_000001.3",
                paired_assembly_status="current",
            ),
            "GCA_000001.3": AssemblyStatusInfo(
                assembly_status="suppressed",
                suppression_reason="removed by submitter",
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
    )

    assert notes["GCF_000001.1"].selected_accession == "GCA_000001.3"
    assert notes["GCF_000001.1"].suppression_reason == "removed by submitter"

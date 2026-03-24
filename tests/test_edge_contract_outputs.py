"""Contract-level edge-case tests for output manifests and planning."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
import pytest

from gtdb_genomes.cli import main
from gtdb_genomes.download import (
    CommandFailureRecord,
)
from gtdb_genomes.layout import (
    ACCESSION_MAP_COLUMNS,
    TAXON_ACCESSION_COLUMNS,
    initialise_run_directories,
)
from gtdb_genomes.metadata import (
    AssemblyStatusInfo,
    MetadataLookupError,
    SummaryLookupResult,
    SUPPRESSED_ASSEMBLY_NOTE,
)
from gtdb_genomes.provenance import RuntimeProvenance
from gtdb_genomes.workflow_execution import (
    AccessionExecution,
    DownloadExecutionResult,
    SharedFailureContext,
)
from gtdb_genomes.workflow_outputs import (
    build_enriched_output_rows,
    build_failure_rows,
)
from tests.workflow_contract_helpers import (
    build_multi_accession_taxonomy_frame,
    build_mixed_uba_taxonomy_frame,
    build_shared_preferred_taxonomy_frame,
    build_taxonomy_frame,
    build_uba_only_taxonomy_frame,
    install_fake_release_resolution,
    install_capture_logger,
    parse_summary_log,
    parse_tsv,
)


@pytest.fixture(autouse=True)
def fake_release_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep output-contract tests independent of generated checkout data."""

    install_fake_release_resolution(monkeypatch)


def build_shared_preferred_summary_lookup_result() -> SummaryLookupResult:
    """Return metadata that prefers the paired GenBank accession."""

    return SummaryLookupResult(
        summary_map={
            "GCF_001881595.2": {
                "GCF_001881595.2",
                "GCA_001881595.2",
                "GCA_001881595.3",
            },
            "GCA_001881595.2": {"GCA_001881595.2"},
            "GCA_001881595.3": {"GCA_001881595.3"},
        },
        status_map={
            "GCF_001881595.2": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession="GCA_001881595.2",
                paired_assembly_status="current",
            ),
            "GCA_001881595.2": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
            "GCA_001881595.3": AssemblyStatusInfo(
                assembly_status="current",
                suppression_reason=None,
                paired_accession=None,
                paired_assembly_status=None,
            ),
        },
        failures=(),
    )


def build_shared_and_unique_two_taxa_frame() -> pl.DataFrame:
    """Return one taxonomy frame with shared and unique genus rows."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "RS_GCF_000002.1",
            ],
            "lineage": [
                (
                    "d__Bacteria;p__Proteobacteria;g__Escherichia;"
                    "s__Escherichia coli"
                ),
                "d__Bacteria;p__Proteobacteria;g__Escherichia",
            ],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCF_000002.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r95.tsv",
                "bac120_taxonomy_r95.tsv",
            ],
        },
    )


def build_two_shared_taxa_frame() -> pl.DataFrame:
    """Return one taxonomy frame with two accessions shared across two taxa."""

    return pl.DataFrame(
        {
            "gtdb_accession": [
                "RS_GCF_000001.1",
                "RS_GCF_000002.1",
            ],
            "lineage": [
                (
                    "d__Bacteria;p__Proteobacteria;g__Escherichia;"
                    "s__Escherichia coli"
                ),
                (
                    "d__Bacteria;p__Proteobacteria;g__Escherichia;"
                    "s__Escherichia coli"
                ),
            ],
            "ncbi_accession": [
                "GCF_000001.1",
                "GCF_000002.1",
            ],
            "taxonomy_file": [
                "bac120_taxonomy_r95.tsv",
                "bac120_taxonomy_r95.tsv",
            ],
        },
    )


def test_auto_planning_uses_count_based_selection_in_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Count-based dry-run planning should work without preview plumbing."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    output_dir = tmp_path / "dry-run-no-preview"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
            "--dry-run",
        ],
    )

    assert exit_code == 0
    assert not output_dir.exists()


def test_successful_real_run_does_not_record_removed_planning_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful real runs should not serialise removed planning failures."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the supported accession."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "preview-retry-success"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert failure_header[0] == "accession"
    assert failure_rows == []


def test_mixed_uba_real_run_records_failed_unsupported_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed supported and UBA runs should keep successes and audit skipped UBA rows."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_mixed_uba_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the supported accession."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "mixed-uba-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    unsupported_row = next(
        row for row in accession_maps if row["gtdb_accessions"] == "UBA11131"
    )
    assert unsupported_row["final_accession"] == ""
    assert unsupported_row["conversion_status"] == "failed_no_usable_accession"
    assert unsupported_row["download_status"] == "failed"

    taxon_header, taxon_rows = parse_tsv(
        output_dir / "taxa" / "g__Escherichia" / "taxon_accessions.tsv",
    )
    taxon_maps = [dict(zip(taxon_header, row, strict=True)) for row in taxon_rows]
    unsupported_taxon_row = next(
        row for row in taxon_maps if row["gtdb_accession"] == "UBA11131"
    )
    assert unsupported_taxon_row["final_accession"] == ""
    assert unsupported_taxon_row["download_status"] == "failed"
    assert unsupported_taxon_row["duplicate_across_taxa"] == "false"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["accession"] == "UBA11131"
    assert failure["gtdb_accessions"] == "UBA11131"
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["status"] == "unsupported_input"
    assert "PRJNA417962" in failure["reason"]


def test_uba_only_real_run_writes_failed_manifests_and_exits_seven(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """UBA-only real runs should skip downloads but still write audit manifests."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: (_ for _ in ()).throw(
            AssertionError("preflight should not run"),
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_uba_only_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supported download execution should not run"),
        ),
    )

    output_dir = tmp_path / "uba-only-real"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["gtdb_accessions"] == "UBA11131"
    assert accession_map["final_accession"] == ""
    assert accession_map["download_status"] == "failed"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["stage"] == "preflight"
    assert failure["error_type"] == "unsupported_accession"
    assert failure["status"] == "unsupported_input"

    run_summary = parse_summary_log(output_dir / "run_summary.log")
    assert run_summary["download_method_used"] == "auto"
    assert run_summary["download_concurrency_used"] == "0"


def test_mixed_real_run_writes_zero_match_taxon_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Mixed runs should still emit output rows for requested zero-match taxa."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the matched taxon."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "mixed-zero-match-real"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    taxon_summary_header, taxon_summary_rows = parse_tsv(output_dir / "taxon_summary.tsv")
    taxon_summaries = [
        dict(zip(taxon_summary_header, row, strict=True))
        for row in taxon_summary_rows
    ]
    assert [row["requested_taxon"] for row in taxon_summaries] == [
        "g__Escherichia",
        "g__Bacillus",
    ]
    bacillus_summary = next(
        row for row in taxon_summaries if row["requested_taxon"] == "g__Bacillus"
    )
    assert bacillus_summary["unique_gtdb_accessions"] == "0"
    assert bacillus_summary["successful_accessions"] == "0"
    assert bacillus_summary["failed_accessions"] == "0"

    bacillus_manifest = output_dir / "taxa" / "g__Bacillus" / "taxon_accessions.tsv"
    assert bacillus_manifest.exists()
    manifest_header, manifest_rows = parse_tsv(bacillus_manifest)
    assert manifest_header == list(TAXON_ACCESSION_COLUMNS)
    assert manifest_rows == []


def test_real_run_moves_unique_outputs_and_copies_shared_outputs_first(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Real runs should copy shared payloads first and move unique payloads."""

    shared_payload_directory = tmp_path / "shared-payload"
    shared_payload_directory.mkdir()
    (shared_payload_directory / "genome.fna").write_text(
        ">shared\nACGT\n",
        encoding="ascii",
    )
    unique_payload_directory = tmp_path / "unique-payload"
    unique_payload_directory.mkdir()
    (unique_payload_directory / "genome.fna").write_text(
        ">unique\nTGCA\n",
        encoding="ascii",
    )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_and_unique_two_taxa_frame(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one shared and one unique successful execution."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert {plan.original_accession for plan in plans} == {
            "GCF_000001.1",
            "GCF_000002.1",
        }
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=shared_payload_directory,
                    failures=(),
                ),
                "GCF_000002.1": AccessionExecution(
                    original_accession="GCF_000002.1",
                    final_accession="GCF_000002.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=unique_payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-and-unique-real"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    assert not shared_payload_directory.exists()
    assert not unique_payload_directory.exists()
    assert (
        output_dir / "taxa" / "g__Escherichia" / "GCF_000001.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">shared\nACGT\n"
    assert (
        output_dir / "taxa" / "s__Escherichia_coli" / "GCF_000001.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">shared\nACGT\n"
    assert (
        output_dir / "taxa" / "g__Escherichia" / "GCF_000002.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">unique\nTGCA\n"


def test_real_run_keep_tmp_forces_copy_only_output_materialisation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Keeping temporary files should preserve the source payload directories."""

    shared_payload_directory = tmp_path / "shared-payload"
    shared_payload_directory.mkdir()
    (shared_payload_directory / "genome.fna").write_text(
        ">shared\nACGT\n",
        encoding="ascii",
    )
    unique_payload_directory = tmp_path / "unique-payload"
    unique_payload_directory.mkdir()
    (unique_payload_directory / "genome.fna").write_text(
        ">unique\nTGCA\n",
        encoding="ascii",
    )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_and_unique_two_taxa_frame(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one shared and one unique successful execution."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=shared_payload_directory,
                    failures=(),
                ),
                "GCF_000002.1": AccessionExecution(
                    original_accession="GCF_000002.1",
                    final_accession="GCF_000002.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=unique_payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "keep-tmp-copy-only"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
            "--keep-tmp",
        ],
    )

    assert exit_code == 0
    assert shared_payload_directory.exists()
    assert unique_payload_directory.exists()
    assert (
        output_dir / "taxa" / "g__Escherichia" / "GCF_000001.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">shared\nACGT\n"
    assert (
        output_dir / "taxa" / "s__Escherichia_coli" / "GCF_000001.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">shared\nACGT\n"
    assert (
        output_dir / "taxa" / "g__Escherichia" / "GCF_000002.1" / "genome.fna"
    ).read_text(encoding="ascii") == ">unique\nTGCA\n"


def test_real_run_logs_sorted_duplicate_copies_at_debug_level(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Duplicate-copy logs should be sorted and emitted at debug level only."""

    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    log_stream = install_capture_logger(monkeypatch)
    first_payload_directory = tmp_path / "payload-1"
    first_payload_directory.mkdir()
    (first_payload_directory / "genome.fna").write_text(
        ">one\nACGT\n",
        encoding="ascii",
    )
    second_payload_directory = tmp_path / "payload-2"
    second_payload_directory.mkdir()
    (second_payload_directory / "genome.fna").write_text(
        ">two\nTGCA\n",
        encoding="ascii",
    )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_two_shared_taxa_frame(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return two shared successful executions."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=first_payload_directory,
                    failures=(),
                ),
                "GCF_000002.1": AccessionExecution(
                    original_accession="GCF_000002.1",
                    final_accession="GCF_000002.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=second_payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "sorted-duplicate-logs"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
            "--debug",
        ],
    )

    assert exit_code == 0
    log_text = log_stream.getvalue()
    assert "INFO Copied duplicate genome" not in log_text
    first_message = "DEBUG Copied duplicate genome GCF_000001.1 into taxon g__Escherichia"
    second_message = "DEBUG Copied duplicate genome GCF_000002.1 into taxon g__Escherichia"
    assert first_message in log_text
    assert second_message in log_text
    assert log_text.index(first_message) < log_text.index(second_message)


def test_real_run_emits_progress_with_taxon_accession_counts_and_percentage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Real-run output materialisation should emit tqdm progress details."""

    install_capture_logger(monkeypatch)
    shared_payload_directory = tmp_path / "shared-payload"
    shared_payload_directory.mkdir()
    (shared_payload_directory / "genome.fna").write_text(
        ">shared\nACGT\n",
        encoding="ascii",
    )
    unique_payload_directory = tmp_path / "unique-payload"
    unique_payload_directory.mkdir()
    (unique_payload_directory / "genome.fna").write_text(
        ">unique\nTGCA\n",
        encoding="ascii",
    )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_and_unique_two_taxa_frame(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one shared and one unique successful execution."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=shared_payload_directory,
                    failures=(),
                ),
                "GCF_000002.1": AccessionExecution(
                    original_accession="GCF_000002.1",
                    final_accession="GCF_000002.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=unique_payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "progress-output"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    progress_text = capsys.readouterr().err
    assert "taxa 1/2 g__Escherichia" in progress_text
    assert "taxa 2/2 s__Escherichia_coli" in progress_text
    assert "finished=GCF_000001.1 action=copy" in progress_text
    assert "finished=GCF_000001.1 action=move" in progress_text
    assert "finished=GCF_000002.1 action=move" in progress_text
    assert "2/2" in progress_text
    assert "1/1" in progress_text
    assert "%|" in progress_text


def test_real_run_output_copy_failure_returns_exit_code_eight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Duplicate-copy failures should return a stable workflow exit code."""

    log_stream = install_capture_logger(monkeypatch)
    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia;s__Escherichia coli",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_outputs.copy_accession_payload",
        lambda source_directory, destination_directory: (_ for _ in ()).throw(
            PermissionError("disk full"),
        ),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one shared successful execution before copy failure."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "output-copy-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 8
    assert "Real-run output materialisation failed: disk full" in log_stream.getvalue()
    assert not (output_dir / "run_summary.log").exists()


def test_real_run_output_move_failure_returns_exit_code_eight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Move failures should return a stable workflow exit code."""

    log_stream = install_capture_logger(monkeypatch)
    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_outputs.move_accession_payload",
        lambda source_directory, destination_directory: (_ for _ in ()).throw(
            PermissionError("disk full"),
        ),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution before output copying fails."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert [plan.original_accession for plan in plans] == ["GCF_000001.1"]
        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "output-move-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 8
    assert "Real-run output materialisation failed: disk full" in log_stream.getvalue()
    assert not (output_dir / "run_summary.log").exists()


def test_shared_preferred_direct_manifest_collapses_to_realised_accessions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Shared preferred direct success should collapse to one realised accession row."""

    payload_directory = tmp_path / "shared-preferred-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one shared preferred direct-success result."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCA_001881595.3",
                    conversion_status="paired_to_gca",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession="GCA_001881595.3",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-preferred-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = [
        dict(zip(accession_header, row, strict=True))
        for row in accession_rows
    ]
    assert len(accession_maps) == 1
    assert accession_maps[0]["final_accession"] == "GCA_001881595.3"
    assert accession_maps[0]["gtdb_accessions"] == (
        "GB_GCA_001881595.3;RS_GCF_001881595.2"
    )


def test_real_run_records_provenance_and_download_request_accessions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful runs should emit deterministic provenance and request accessions."""

    payload_directory = tmp_path / "shared-preferred-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: build_shared_preferred_summary_lookup_result(),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_outputs.build_runtime_provenance",
        lambda **kwargs: RuntimeProvenance(
            package_version="1.2.3",
            git_revision="deadbeef",
            datasets_version="datasets 2.0",
            unzip_version="UnZip 6.00",
            release_manifest_sha256=kwargs["release_manifest_sha256"],
            bacterial_taxonomy_sha256=kwargs["bacterial_taxonomy_sha256"],
            archaeal_taxonomy_sha256=kwargs["archaeal_taxonomy_sha256"],
        ),
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful direct execution for the paired accessions."""

        del run_directories, logger, secrets
        assert decision_method == "direct"
        assert {plan.original_accession for plan in plans} == {
            "GCF_001881595.2",
            "GCA_001881595.3",
        }
        payload_directory.mkdir(parents=True, exist_ok=True)
        (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")
        request_accession = (
            "GCA_001881595" if args.version_latest else "GCA_001881595.2"
        )
        selected_accession = (
            "GCA_001881595.3" if args.version_latest else "GCA_001881595.2"
        )
        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession=selected_accession,
                    conversion_status="paired_to_gca",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used=request_accession,
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession="GCA_001881595.3",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used=request_accession,
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    def run_case(
        output_dir: Path,
        *,
        version_latest: bool,
    ) -> tuple[dict[str, str], dict[str, dict[str, str]], dict[str, dict[str, str]]]:
        """Run one workflow case and return the parsed output manifests."""

        args = [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--outdir",
            str(output_dir),
            "--prefer-genbank",
        ]
        if version_latest:
            args.insert(-1, "--version-latest")
        exit_code = main(args)
        assert exit_code == 0

        run_summary = parse_summary_log(output_dir / "run_summary.log")
        accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
        taxon_header, taxon_rows = parse_tsv(
            output_dir / "taxa" / "g__Bacillus" / "taxon_accessions.tsv",
        )
        return (
            run_summary,
            {
                row_dict["final_accession"] or row_dict["gtdb_accessions"]: row_dict
                for row_dict in (
                    dict(zip(accession_header, row, strict=True))
                    for row in accession_rows
                )
            },
            {
                row_dict["ncbi_accession"]: row_dict
                for row_dict in (
                    dict(zip(taxon_header, row, strict=True))
                    for row in taxon_rows
                )
            },
        )

    fixed_summary, fixed_accessions, fixed_taxon_rows = run_case(
        tmp_path / "fixed-version",
        version_latest=False,
    )
    fixed_summary_repeat, _, _ = run_case(
        tmp_path / "fixed-version-repeat",
        version_latest=False,
    )
    latest_summary, latest_accessions, latest_taxon_rows = run_case(
        tmp_path / "latest-version",
        version_latest=True,
    )

    assert fixed_summary["run_id"] == fixed_summary_repeat["run_id"]
    assert fixed_summary["run_id"] != latest_summary["run_id"]
    assert (
        fixed_summary["accession_decision_sha256"]
        == fixed_summary_repeat["accession_decision_sha256"]
    )
    assert (
        fixed_summary["accession_decision_sha256"]
        != latest_summary["accession_decision_sha256"]
    )
    assert fixed_summary["package_version"] == "1.2.3"
    assert fixed_summary["git_revision"] == "deadbeef"
    assert fixed_summary["datasets_version"] == "datasets 2.0"
    assert fixed_summary["unzip_version"] == "UnZip 6.00"
    assert fixed_summary["release_manifest_sha256"] == "0" * 64
    assert fixed_summary["bacterial_taxonomy_sha256"] == "1" * 64
    assert fixed_summary["archaeal_taxonomy_sha256"] == ""
    assert fixed_summary["version_latest"] == "false"

    assert fixed_accessions["GCA_001881595.2"]["selected_accessions"] == "GCA_001881595.2"
    assert (
        fixed_accessions["GCA_001881595.2"]["download_request_accessions"]
        == "GCA_001881595.2"
    )
    assert fixed_taxon_rows["GCF_001881595.2"]["selected_accession"] == "GCA_001881595.2"
    assert fixed_taxon_rows["GCF_001881595.2"]["download_request_accession"] == "GCA_001881595.2"

    assert latest_summary["version_latest"] == "true"
    assert latest_accessions["GCA_001881595.3"]["selected_accessions"] == "GCA_001881595.3"
    assert (
        latest_accessions["GCA_001881595.3"]["download_request_accessions"]
        == "GCA_001881595"
    )
    assert latest_taxon_rows["GCF_001881595.2"]["selected_accession"] == "GCA_001881595.3"
    assert latest_taxon_rows["GCF_001881595.2"]["download_request_accession"] == "GCA_001881595"


def test_direct_fallback_manifest_uses_execution_request_accession(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Fixed-version fallback rows should record the execution request token."""

    payload_directory = tmp_path / "fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: build_shared_preferred_summary_lookup_result(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one fallback success and one preferred-group failure."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used="GCF_001881595.2",
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="preferred failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595.3",
                        ),
                    ),
                    request_accession_used="GCA_001881595.3",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    taxon_header, taxon_rows = parse_tsv(
        output_dir / "taxa" / "g__Bacillus" / "taxon_accessions.tsv",
    )
    taxon_maps = {
        row["gtdb_accession"]: row
        for row in (
            dict(zip(taxon_header, values, strict=True))
            for values in taxon_rows
        )
    }
    assert taxon_maps["RS_GCF_001881595.2"]["download_request_accession"] == (
        "GCF_001881595.2"
    )
    assert taxon_maps["RS_GCF_001881595.2"]["final_accession"] == "GCF_001881595.2"
    assert taxon_maps["RS_GCF_001881595.2"]["conversion_status"] == (
        "paired_to_gca_fallback_original_on_download_failure"
    )


def test_latest_fallback_manifest_uses_original_fallback_request_accession(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Latest-mode fallback rows should keep the original executed token."""

    payload_directory = tmp_path / "latest-fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: build_shared_preferred_summary_lookup_result(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one latest-mode fallback success and one failure."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used="GCF_001881595.2",
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="preferred failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595",
                        ),
                    ),
                    request_accession_used="GCA_001881595",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "latest-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--prefer-genbank",
            "--version-latest",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = {
        row["final_accession"] or row["gtdb_accessions"]: row
        for row in (
            dict(zip(accession_header, values, strict=True))
            for values in accession_rows
        )
    }
    assert accession_maps["GCF_001881595.2"]["selected_accessions"] == (
        "GCA_001881595.3"
    )
    assert accession_maps["GCF_001881595.2"]["download_request_accessions"] == (
        "GCF_001881595.2"
    )
    assert accession_maps["GCF_001881595.2"]["final_accession"] == (
        "GCF_001881595.2"
    )
    assert accession_maps["GCF_001881595.2"]["conversion_status"] == (
        "paired_to_gca_fallback_original_on_download_failure"
    )


def test_failed_fallback_manifest_keeps_terminal_fallback_request_accession(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Failed fallback rows should keep the terminal fallback request token."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: build_shared_preferred_summary_lookup_result(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return failed preferred and fallback executions."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="layout",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="LayoutError",
                            error_message="preferred unresolved",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595",
                        ),
                        CommandFailureRecord(
                            stage="layout",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="LayoutError",
                            error_message="fallback unresolved",
                            final_status="retry_exhausted",
                            attempted_accession="GCF_001881595.2",
                        ),
                    ),
                    request_accession_used="GCF_001881595.2",
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="layout",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="LayoutError",
                            error_message="preferred unresolved",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595",
                        ),
                    ),
                    request_accession_used="GCA_001881595",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "failed-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = {
        row["final_accession"] or row["gtdb_accessions"]: row
        for row in (
            dict(zip(accession_header, values, strict=True))
            for values in accession_rows
        )
    }
    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    gcf_failures = [
        dict(zip(failure_header, values, strict=True))
        for values in failure_rows
        if values[failure_header.index("gtdb_accessions")] == "RS_GCF_001881595.2"
    ]
    assert accession_maps["RS_GCF_001881595.2"]["download_request_accessions"] == (
        "GCF_001881595.2"
    )
    assert [row["accession"] for row in gcf_failures] == ["GCF_001881595.2"]
    assert [row["reason"] for row in gcf_failures] == ["fallback unresolved"]


def test_dehydrate_fallback_manifest_uses_direct_execution_request_accession(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Dehydrate fallback rows should use the direct fallback request token."""

    payload_directory = tmp_path / "dehydrate-fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_shared_preferred_taxonomy_frame(
            "d__Bacteria;p__Firmicutes;g__Bacillus",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: build_shared_preferred_summary_lookup_result(),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one dehydrate-to-direct fallback success and one failure."""

        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_to_gca_fallback_original_on_download_failure",
                    download_status="downloaded_after_fallback",
                    download_batch="direct_fallback_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used="GCF_001881595.2",
                ),
                "GCA_001881595.3": AccessionExecution(
                    original_accession="GCA_001881595.3",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="direct_batch_1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="preferred failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCA_001881595.3",
                        ),
                    ),
                    request_accession_used="GCA_001881595.3",
                ),
            },
            method_used="dehydrate_fallback_direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "dehydrate-fallback-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 6
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_maps = {
        row["final_accession"] or row["gtdb_accessions"]: row
        for row in (
            dict(zip(accession_header, values, strict=True))
            for values in accession_rows
        )
    }
    assert accession_maps["GCF_001881595.2"]["download_request_accessions"] == (
        "GCF_001881595.2"
    )


def test_build_enriched_output_rows_uses_execution_request_accession(
    tmp_path: Path,
) -> None:
    """Output enrichment should use execution provenance over planning state."""

    payload_directory = tmp_path / "payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")
    run_directories = initialise_run_directories(tmp_path / "output")
    mapped_frame = pl.DataFrame(
        {
            "requested_taxon": ["g__Bacillus"],
            "taxon_slug": ["g__Bacillus"],
            "taxonomy_file": ["bac120_taxonomy_r80.tsv"],
            "lineage": ["d__Bacteria;p__Firmicutes;g__Bacillus"],
            "gtdb_accession": ["RS_GCF_001881595.2"],
            "ncbi_accession": ["GCF_001881595.2"],
            "final_accession": ["GCA_001881595.3"],
            "accession_type_original": ["RefSeq"],
            "accession_type_final": ["GenBank"],
        },
    )
    execution_result = DownloadExecutionResult(
        executions={
            "GCF_001881595.2": AccessionExecution(
                original_accession="GCF_001881595.2",
                final_accession="GCF_001881595.2",
                conversion_status="paired_to_gca_fallback_original_on_download_failure",
                download_status="downloaded_after_fallback",
                download_batch="direct_fallback_batch_1",
                payload_directory=payload_directory,
                failures=(),
                request_accession_used="GCF_001881595.2",
            ),
        },
        method_used="direct",
        download_concurrency_used=1,
        rehydrate_workers_used=0,
    )

    enriched_rows, per_taxon_rows, duplicate_counts = (
        build_enriched_output_rows(
            "80.0",
            mapped_frame,
            execution_result,
            {},
            run_directories,
            logging.getLogger("test-build-enriched-output-rows"),
        )
    )

    del per_taxon_rows, duplicate_counts
    assert enriched_rows[0]["selected_accession"] == "GCA_001881595.3"
    assert enriched_rows[0]["download_request_accession"] == "GCF_001881595.2"
    assert enriched_rows[0]["final_accession"] == "GCF_001881595.2"


def test_direct_success_manifest_preserves_shared_retry_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Successful direct rows should keep earlier shared retry failures."""

    payload_directory = tmp_path / "retry-success-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one successful execution with shared retry history."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession="GCF_000001.1",
                    conversion_status="unchanged_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used="GCF_000001.1",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
            shared_failures=(
                SharedFailureContext(
                    affected_original_accessions=("GCF_000001.1",),
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=1,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="temporary datasets failure",
                            final_status="retry_scheduled",
                            attempted_accession="GCF_000001.1",
                        ),
                    ),
                ),
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "direct-retry-success-manifest"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["download_status"] == "downloaded"
    assert accession_map["download_request_accessions"] == "GCF_000001.1"

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert failure_header[0] == "accession"
    assert failure_rows == []


def test_candidate_lookup_failure_falls_back_to_original_without_failure_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Candidate metadata lookup fallbacks should not write terminal failure rows."""

    payload_directory = tmp_path / "candidate-lookup-fallback-payload"
    payload_directory.mkdir()
    (payload_directory / "genome.fna").write_text(">seq\nACGT\n", encoding="ascii")
    lookup_calls: list[tuple[str, ...]] = []

    def fake_run_summary_lookup_with_retries(
        accessions,
        accession_file,
        ncbi_api_key=None,
        datasets_bin="datasets",
        sleep_func=None,
        runner=None,
    ) -> SummaryLookupResult:
        """Return one preferred lookup, then fail the paired-GCA lookup."""

        del accession_file, ncbi_api_key, datasets_bin, sleep_func, runner
        ordered_accessions = tuple(accessions)
        lookup_calls.append(ordered_accessions)
        if len(lookup_calls) == 1:
            return SummaryLookupResult(
                summary_map={
                    "GCF_001881595.2": {
                        "GCF_001881595.2",
                        "GCA_001881595.2",
                        "GCA_001881595.3",
                    },
                },
                status_map={
                    "GCF_001881595.2": AssemblyStatusInfo(
                        assembly_status="current",
                        suppression_reason=None,
                        paired_accession="GCA_001881595.2",
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
                    attempted_accession="GCA_001881595.2",
                ),
            ),
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: pl.DataFrame(
            {
                "gtdb_accession": ["RS_GCF_001881595.2"],
                "lineage": ["d__Bacteria;p__Firmicutes;g__Bacillus"],
                "ncbi_accession": ["GCF_001881595.2"],
                "taxonomy_file": ["bac120_taxonomy_r80.tsv"],
            },
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        fake_run_summary_lookup_with_retries,
    )

    def fake_execute_accession_plans(
        plans,
        args,
        decision_method: str,
        run_directories,
        logger,
        secrets,
    ) -> DownloadExecutionResult:
        """Return one successful original-accession direct execution."""

        del args, run_directories, logger, secrets
        assert decision_method == "direct"
        assert len(plans) == 1
        assert plans[0].original_accession == "GCF_001881595.2"
        assert plans[0].download_request_accession == "GCF_001881595.2"
        return DownloadExecutionResult(
            executions={
                "GCF_001881595.2": AccessionExecution(
                    original_accession="GCF_001881595.2",
                    final_accession="GCF_001881595.2",
                    conversion_status="paired_gca_metadata_incomplete_fallback_original",
                    download_status="downloaded",
                    download_batch="direct_batch_1",
                    payload_directory=payload_directory,
                    failures=(),
                    request_accession_used="GCF_001881595.2",
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "candidate-lookup-fallback"
    exit_code = main(
        [
            "--gtdb-release",
            "80",
            "--gtdb-taxon",
            "g__Bacillus",
            "--prefer-genbank",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 0
    assert lookup_calls == [
        ("GCF_001881595.2",),
        ("GCA_001881595.2",),
    ]
    accession_header, accession_rows = parse_tsv(output_dir / "accession_map.tsv")
    accession_map = dict(zip(accession_header, accession_rows[0], strict=True))
    assert accession_map["selected_accessions"] == "GCF_001881595.2"
    assert accession_map["download_request_accessions"] == "GCF_001881595.2"
    assert accession_map["final_accession"] == "GCF_001881595.2"
    assert accession_map["conversion_status"] == (
        "paired_gca_metadata_incomplete_fallback_original"
    )

    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert failure_header[0] == "accession"
    assert failure_rows == []


def test_failure_manifest_collapses_shared_accession_taxa(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """One shared failed accession should yield one root failure row."""

    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.check_required_tools",
        lambda required_tools: None,
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_selection.load_release_taxonomy",
        lambda resolution: build_taxonomy_frame(
            "d__Bacteria;p__Proteobacteria;g__Escherichia;s__Escherichia coli",
        ),
    )
    monkeypatch.setattr(
        "gtdb_genomes.workflow_planning.run_summary_lookup_with_retries",
        lambda *args, **kwargs: SummaryLookupResult(summary_map={}, failures=()),
    )

    def fake_execute_accession_plans(
        *args,
        **kwargs,
    ) -> DownloadExecutionResult:
        """Return one failed execution for the shared accession."""

        return DownloadExecutionResult(
            executions={
                "GCF_000001.1": AccessionExecution(
                    original_accession="GCF_000001.1",
                    final_accession=None,
                    conversion_status="failed_no_usable_accession",
                    download_status="failed",
                    download_batch="GCF_000001.1",
                    payload_directory=None,
                    failures=(
                        CommandFailureRecord(
                            stage="preferred_download",
                            attempt_index=4,
                            max_attempts=4,
                            error_type="subprocess",
                            error_message="download failed",
                            final_status="retry_exhausted",
                            attempted_accession="GCF_000001.1",
                        ),
                    ),
                ),
            },
            method_used="direct",
            download_concurrency_used=1,
            rehydrate_workers_used=0,
        )

    monkeypatch.setattr(
        "gtdb_genomes.workflow_execution.execute_accession_plans",
        fake_execute_accession_plans,
    )

    output_dir = tmp_path / "shared-failure"
    exit_code = main(
        [
            "--gtdb-release",
            "95",
            "--gtdb-taxon",
            "g__Escherichia",
            "--gtdb-taxon",
            "s__Escherichia coli",
            "--outdir",
            str(output_dir),
        ],
    )

    assert exit_code == 7
    failure_header, failure_rows = parse_tsv(output_dir / "download_failures.tsv")
    assert len(failure_rows) == 1
    failure = dict(zip(failure_header, failure_rows[0], strict=True))
    assert failure["requested_taxa"] == "g__Escherichia;s__Escherichia coli"
    assert failure["accession"] == "GCF_000001.1"


def test_failure_manifest_ignores_shared_metadata_attempts_without_terminal_failures() -> None:
    """Shared metadata retries should not appear without failed accessions."""

    enriched_rows = [
        {
            "requested_taxon": "g__Escherichia",
            "taxon_slug": "g__Escherichia",
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
        },
        {
            "requested_taxon": "s__Escherichia coli",
            "taxon_slug": "s__Escherichia_coli",
            "gtdb_accession": "RS_GCF_000002.1",
            "ncbi_accession": "GCF_000002.1",
            "final_accession": "GCF_000002.1",
        },
    ]
    executions = {
        "GCF_000001.1": AccessionExecution(
            original_accession="GCF_000001.1",
            final_accession="GCF_000001.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="GCF_000001.1",
            payload_directory=None,
            failures=(),
        ),
        "GCF_000002.1": AccessionExecution(
            original_accession="GCF_000002.1",
            final_accession="GCF_000002.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="GCF_000002.1",
            payload_directory=None,
            failures=(),
        ),
    }
    metadata_shared_failures = (
        SharedFailureContext(
            affected_original_accessions=("GCF_000001.1", "GCF_000002.1"),
            failures=(
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=1,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="temporary failure",
                    final_status="retry_scheduled",
                ),
                CommandFailureRecord(
                    stage="metadata_lookup",
                    attempt_index=2,
                    max_attempts=4,
                    error_type="metadata_lookup",
                    error_message="temporary failure",
                    final_status="retry_exhausted",
                ),
            ),
        ),
    )

    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        (),
    )

    assert failure_rows == []


def test_failure_manifest_ignores_metadata_candidate_accession_set() -> None:
    """Metadata-only candidate failures should not populate the final failure file."""

    metadata_shared_failures = (
        SharedFailureContext(
            affected_original_accessions=("GCF_000001.1",),
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
        ),
    )
    enriched_rows = [
        {
            "requested_taxon": "g__Escherichia",
            "taxon_slug": "g__Escherichia",
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
        },
    ]

    failure_rows = build_failure_rows(
        enriched_rows,
        {
            "GCF_000001.1": AccessionExecution(
                original_accession="GCF_000001.1",
                final_accession="GCF_000001.1",
                conversion_status="unchanged_original",
                download_status="downloaded",
                download_batch="GCF_000001.1",
                payload_directory=None,
                failures=(),
            ),
        },
        (),
    )

    assert failure_rows == []


def test_failure_manifest_scopes_candidate_metadata_failures_to_affected_rows() -> None:
    """Candidate metadata-only failures should not populate terminal rows."""

    enriched_rows = [
        {
            "requested_taxon": "g__Escherichia",
            "taxon_slug": "g__Escherichia",
            "gtdb_accession": "RS_GCF_000001.1",
            "ncbi_accession": "GCF_000001.1",
            "final_accession": "GCF_000001.1",
        },
        {
            "requested_taxon": "g__Bacillus",
            "taxon_slug": "g__Bacillus",
            "gtdb_accession": "RS_GCF_000002.1",
            "ncbi_accession": "GCF_000002.1",
            "final_accession": "GCF_000002.1",
        },
    ]
    executions = {
        "GCF_000001.1": AccessionExecution(
            original_accession="GCF_000001.1",
            final_accession="GCF_000001.1",
            conversion_status="paired_gca_metadata_incomplete_fallback_original",
            download_status="downloaded",
            download_batch="direct_batch_1",
            payload_directory=None,
            failures=(),
        ),
        "GCF_000002.1": AccessionExecution(
            original_accession="GCF_000002.1",
            final_accession="GCF_000002.1",
            conversion_status="unchanged_original",
            download_status="downloaded",
            download_batch="direct_batch_1",
            payload_directory=None,
            failures=(),
        ),
    }
    metadata_shared_failures = (
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

    failure_rows = build_failure_rows(
        enriched_rows,
        executions,
        (),
    )

    assert failure_rows == []

"""Tests for the real-data validation bash helpers."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path


COMMON_HELPERS = Path("bin/real-data-test-common.sh").resolve()
SERVER_WRAPPER = Path("bin/run-real-data-tests-server.sh").resolve()


def run_bash(script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run one bash snippet for the real-data helper tests."""

    merged_env = os.environ.copy()
    if env is not None:
        merged_env.update(env)
    return subprocess.run(
        ["bash", "-lc", script],
        capture_output=True,
        text=True,
        check=False,
        env=merged_env,
    )


def write_fake_remote_runner(tmp_path: Path) -> Path:
    """Create a fake remote runner that records its environment and argv."""

    capture_file = tmp_path / "capture.txt"
    fake_runner = tmp_path / "fake-remote-runner.sh"
    fake_runner.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'RUN_OPTIONAL_LARGE=%s\\n' \"${RUN_OPTIONAL_LARGE:-}\" > \"$CAPTURE_FILE\"\n"
        "printf 'ARGC=%s\\n' \"$#\" >> \"$CAPTURE_FILE\"\n"
        "printf 'ARGV=%s\\n' \"$*\" >> \"$CAPTURE_FILE\"\n",
        encoding="utf-8",
    )
    fake_runner.chmod(0o755)
    return fake_runner


def extract_bash_function(script_path: Path, function_name: str) -> str:
    """Extract one top-level bash function definition from a script."""

    script_text = script_path.read_text(encoding="utf-8")
    marker = f"{function_name}() {{"
    start_index = script_text.index(marker)
    end_index = script_text.index("\n\n\n", start_index)
    return script_text[start_index:end_index]


def test_real_data_write_command_file_redacts_ncbi_api_key(
    tmp_path: Path,
) -> None:
    """The command evidence file should redact the NCBI API key."""

    command_file = tmp_path / "command.sh"
    secret = "abc123secret"
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export NCBI_API_KEY={shlex.quote(secret)}\n"
        "real_data_write_command_file "
        f"{shlex.quote(str(command_file))} "
        "gtdb-genomes --gtdb-release 95 --ncbi-api-key \"$NCBI_API_KEY\" --outdir /tmp/out\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    command_text = command_file.read_text(encoding="utf-8")
    assert "REDACTED" in command_text
    assert secret not in command_text


def test_real_data_default_suite_root_creates_unique_directories(
    tmp_path: Path,
) -> None:
    """The default suite root helper should return unique per-run directories."""

    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export TMPDIR={shlex.quote(str(tmp_path))}\n"
        "root_one=$(real_data_default_suite_root local)\n"
        "root_two=$(real_data_default_suite_root local)\n"
        "printf '%s\\n%s\\n' \"$root_one\" \"$root_two\"\n"
        "[ \"$root_one\" != \"$root_two\" ]\n"
        "[ -d \"$root_one\" ]\n"
        "[ -d \"$root_two\" ]\n"
        "rm -rf \"$root_one\" \"$root_two\"\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    roots = result.stdout.splitlines()
    assert len(roots) == 2
    assert roots[0] != roots[1]
    assert roots[0].startswith(str(tmp_path))
    assert roots[1].startswith(str(tmp_path))


def test_real_data_detect_python_bin_falls_back_to_python3(
    tmp_path: Path,
) -> None:
    """The shared helper should detect `python3` when `python` is absent."""

    fake_bin_dir = tmp_path / "bin"
    fake_bin_dir.mkdir()
    fake_python3 = fake_bin_dir / "python3"
    fake_python3.write_text("#!/usr/bin/env bash\nprintf 'python3-bin\\n'\n", encoding="utf-8")
    fake_python3.chmod(0o755)
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export PATH={shlex.quote(str(fake_bin_dir))}\n"
        "real_data_detect_python_bin\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout.strip() == str(fake_python3)


def test_real_data_run_command_check_redacts_logs_and_records_versions(
    tmp_path: Path,
) -> None:
    """Runtime evidence should redact secrets and capture tool versions."""

    test_root = tmp_path / "suite"
    secret = "abc123secret"
    python_code = (
        "import os, sys; "
        "print(os.environ['NCBI_API_KEY']); "
        "print(os.environ['NCBI_API_KEY'], file=sys.stderr)"
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export NCBI_API_KEY={shlex.quote(secret)}\n"
        f"real_data_initialise_suite {shlex.quote(str(test_root))}\n"
        "real_data_record_tool_versions "
        f"{shlex.quote(str(test_root))} {shlex.quote(sys.executable)}\n"
        "real_data_run_command_check "
        f"{shlex.quote(str(test_root))} "
        "check1 0 "
        f"{shlex.quote(sys.executable)} -c {shlex.quote(python_code)}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    evidence_root = test_root / "_evidence" / "check1"
    command_text = (evidence_root / "command.sh").read_text(encoding="utf-8")
    stdout_text = (evidence_root / "stdout.log").read_text(encoding="utf-8")
    stderr_text = (evidence_root / "stderr.log").read_text(encoding="utf-8")
    combined_text = (evidence_root / "combined.log").read_text(encoding="utf-8")
    version_text = (test_root / "_evidence" / "tool-versions.txt").read_text(
        encoding="utf-8",
    )

    assert secret not in command_text
    assert secret not in stdout_text
    assert secret not in stderr_text
    assert secret not in combined_text
    assert "[REDACTED]" in stdout_text
    assert "[REDACTED]" in stderr_text
    assert "[REDACTED]" in combined_text
    assert "python_version=" in version_text
    assert "datasets_version=" in version_text


def test_real_data_run_command_check_removes_raw_temp_directory(
    tmp_path: Path,
) -> None:
    """Command evidence collection should clean up its raw temp workspace."""

    test_root = tmp_path / "suite"
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir()
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"export TMPDIR={shlex.quote(str(temp_dir))}\n"
        f"real_data_initialise_suite {shlex.quote(str(test_root))}\n"
        "real_data_run_command_check "
        f"{shlex.quote(str(test_root))} "
        "check1 0 "
        f"{shlex.quote(sys.executable)} -c 'print(\"ok\")'\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert not list(temp_dir.glob("gtdb_real_command.*"))


def test_real_data_prepare_case_command_records_faulthandler_and_safe_debug(
    tmp_path: Path,
) -> None:
    """Safe investigation mode should record faulthandler and debug flags."""

    command_file = tmp_path / "command.sh"
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "export REAL_DATA_PYTHON_FAULTHANDLER=1\n"
        "export REAL_DATA_DEBUG_SAFE=1\n"
        "real_data_prepare_case_command "
        "gtdb-genomes --gtdb-release 226 "
        "--gtdb-taxon 's__Thermoflexus hugenholtzii' "
        "--threads 2 --include genome\n"
        "real_data_write_command_file "
        f"{shlex.quote(str(command_file))} "
        '"${REAL_DATA_PREPARED_COMMAND[@]}" --outdir /tmp/out\n'
    )

    result = run_bash(script)

    assert result.returncode == 0
    command_text = command_file.read_text(encoding="utf-8")
    assert "env PYTHONFAULTHANDLER=1 gtdb-genomes" in command_text
    assert "--debug" in command_text
    assert "--threads 2" in command_text


def test_real_data_prepare_case_command_skips_debug_for_api_key_case(
    tmp_path: Path,
) -> None:
    """Safe debug mode should not add `--debug` to API-key cases."""

    command_file = tmp_path / "command.sh"
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "export REAL_DATA_PYTHON_FAULTHANDLER=1\n"
        "export REAL_DATA_DEBUG_SAFE=1\n"
        "real_data_prepare_case_command "
        "gtdb-genomes --gtdb-release 207 "
        "--gtdb-taxon g__Methanobrevibacter "
        "--threads 4 --include genome,gff3 "
        "--ncbi-api-key secret\n"
        "real_data_write_command_file "
        f"{shlex.quote(str(command_file))} "
        '"${REAL_DATA_PREPARED_COMMAND[@]}" --outdir /tmp/out\n'
    )

    result = run_bash(script)

    assert result.returncode == 0
    command_text = command_file.read_text(encoding="utf-8")
    assert "env PYTHONFAULTHANDLER=1 gtdb-genomes" in command_text
    assert "--debug" not in command_text
    assert "--ncbi-api-key" in command_text


def test_real_data_append_optional_ncbi_api_key_keeps_command_without_key() -> None:
    """Optional API-key helper should leave commands unchanged when unset."""

    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "unset NCBI_API_KEY\n"
        "while IFS= read -r -d '' argument; do\n"
        "  printf '%s\\n' \"$argument\"\n"
        "done < <(real_data_append_optional_ncbi_api_key gtdb-genomes --threads 2)\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout.splitlines() == ["gtdb-genomes", "--threads", "2"]


def test_real_data_append_optional_ncbi_api_key_appends_key_when_set() -> None:
    """Optional API-key helper should append the CLI flag when available."""

    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "export NCBI_API_KEY=secret\n"
        "while IFS= read -r -d '' argument; do\n"
        "  printf '%s\\n' \"$argument\"\n"
        "done < <(real_data_append_optional_ncbi_api_key gtdb-genomes --threads 2)\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "gtdb-genomes",
        "--threads",
        "2",
        "--ncbi-api-key",
        "secret",
    ]


def test_real_data_assert_any_taxon_manifest_row_column_matches_finds_true_row(
    tmp_path: Path,
) -> None:
    """Taxon manifest column matching should work without regex tab hacks."""

    output_root = tmp_path / "output"
    manifest_path = output_root / "taxa" / "g__Thermoflexus" / "taxon_accessions.tsv"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        "requested_taxon\ttaxon_slug\tlineage\tgtdb_accession\tfinal_accession\t"
        "conversion_status\toutput_relpath\tdownload_status\tduplicate_across_taxa\n"
        "g__Thermoflexus\tg__Thermoflexus\tlineage\tRS_GCF_000001.1\t"
        "GCF_000001.1\tsuccess\ttaxa/g__Thermoflexus/GCF_000001.1\t"
        "downloaded\ttrue\n",
        encoding="utf-8",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "real_data_assert_any_taxon_manifest_row_column_matches "
        f"{shlex.quote(str(output_root))} "
        "duplicate_across_taxa '^true$' "
        "'duplicate-across-taxa flag'\n"
    )

    result = run_bash(script)

    assert result.returncode == 0


def test_real_data_assert_any_taxon_manifest_row_column_matches_handles_crlf(
    tmp_path: Path,
) -> None:
    """Taxon manifest matching should strip CRLF from the last TSV column."""

    output_root = tmp_path / "output"
    manifest_path = output_root / "taxa" / "g__Thermoflexus" / "taxon_accessions.tsv"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(
        (
            "requested_taxon\ttaxon_slug\tlineage\tgtdb_accession\tfinal_accession\t"
            "conversion_status\toutput_relpath\tdownload_status\tduplicate_across_taxa\r\n"
            "g__Thermoflexus\tg__Thermoflexus\tlineage\tRS_GCF_000001.1\t"
            "GCF_000001.1\tsuccess\ttaxa/g__Thermoflexus/GCF_000001.1\t"
            "downloaded\ttrue\r\n"
        ).encode("ascii")
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "real_data_assert_any_taxon_manifest_row_column_matches "
        f"{shlex.quote(str(output_root))} "
        "duplicate_across_taxa '^true$' "
        "'duplicate-across-taxa flag'\n"
    )

    result = run_bash(script)

    assert result.returncode == 0


def test_real_data_tsv_value_strips_crlf_from_last_column(tmp_path: Path) -> None:
    """TSV value extraction should normalise CRLF line endings."""

    tsv_path = tmp_path / "run_summary.tsv"
    tsv_path.write_bytes(
        (
            "run_id\texit_code\r\n"
            "run1\t0\r\n"
        ).encode("ascii")
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"real_data_tsv_value {shlex.quote(str(tsv_path))} exit_code\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout == "0\n"


def test_real_data_unique_tsv_values_for_match_strips_crlf_from_last_column(
    tmp_path: Path,
) -> None:
    """TSV matching should treat CRLF-terminated last columns as normal values."""

    tsv_path = tmp_path / "accession_map.tsv"
    tsv_path.write_bytes(
        (
            "ncbi_accession\tdownload_status\r\n"
            "GCF_000001.1\tdownloaded\r\n"
            "GCF_003670205.1\tfailed\r\n"
            "GCF_003670205.1\tfailed\r\n"
        ).encode("ascii")
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "real_data_unique_tsv_values_for_match "
        f"{shlex.quote(str(tsv_path))} "
        "ncbi_accession download_status failed\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert result.stdout == "GCF_003670205.1\n"


def test_real_data_record_output_evidence_copies_debug_log(tmp_path: Path) -> None:
    """Evidence capture should copy `debug.log` when a run writes one."""

    output_root = tmp_path / "output"
    evidence_root = tmp_path / "evidence"
    output_root.mkdir()
    evidence_root.mkdir()
    (output_root / "debug.log").write_text("debug-line\n", encoding="utf-8")
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        "real_data_record_output_evidence "
        f"{shlex.quote(str(output_root))} {shlex.quote(str(evidence_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    assert (evidence_root / "debug.log").read_text(encoding="utf-8") == "debug-line\n"


def test_remote_runner_uses_shared_defaults() -> None:
    """The remote runner should share unique-root and Python-detection helpers."""

    remote_script = Path("bin/run-real-data-tests-remote.sh").read_text(
        encoding="utf-8",
    )

    assert "real_data_default_suite_root remote" in remote_script
    assert "real_data_detect_python_bin" in remote_script
    assert "\"C0-manifest\"" in remote_script
    assert "gtdb-genomes \\" in remote_script
    assert "--gtdb-taxon g__DefinitelyNotReal" in remote_script
    assert "--dry-run" in remote_script
    assert "--threads 2" in remote_script
    assert "--download-method" not in remote_script
    assert "get_release_manifest_path" not in remote_script


def test_local_runner_keeps_only_c7_as_api_key_required_case() -> None:
    """The local and remote runners should not hard-require the key for CI cases."""

    local_script = Path("bin/run-real-data-tests-local.sh").read_text(
        encoding="utf-8",
    )
    remote_script = Path("bin/run-real-data-tests-remote.sh").read_text(
        encoding="utf-8",
    )

    assert "real_data_append_optional_ncbi_api_key" in local_script
    assert "real_data_append_optional_ncbi_api_key" in remote_script
    assert "B2)\n            real_data_require_ncbi_api_key" not in local_script
    assert "B6)\n            real_data_require_ncbi_api_key" not in local_script
    assert "C2)\n            real_data_require_ncbi_api_key" not in remote_script
    assert "C3)\n            real_data_require_ncbi_api_key" not in remote_script
    c5_block = remote_script.split("C5)", 1)[1].split("C6)", 1)[0]
    assert "real_data_append_optional_ncbi_api_key" in c5_block
    assert "real_data_require_ncbi_api_key" not in c5_block
    assert "C7)\n            real_data_require_ncbi_api_key" in remote_script


def test_real_data_run_case_accepts_expected_exit_pattern(tmp_path: Path) -> None:
    """Case execution should accept regex-style expected exit patterns."""

    test_root = tmp_path / "suite"
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"real_data_initialise_suite {shlex.quote(str(test_root))}\n"
        "real_data_run_case "
        f"{shlex.quote(str(test_root))} "
        "C5 '0|6' absent '' '' "
        f"{shlex.quote(sys.executable)} -c 'import sys; sys.exit(6)'\n"
    )

    result = run_bash(script)

    assert result.returncode == 0
    summary_text = (test_root / "_evidence" / "C5" / "summary.txt").read_text(
        encoding="utf-8",
    )
    assert "status=PASS" in summary_text
    assert "expected_exit=0|6" in summary_text
    assert "actual_exit=6" in summary_text


def test_remote_check_dehydrate_suppressed_partial_result_accepts_suppressed_only_failures(
    tmp_path: Path,
) -> None:
    """C5 should pass on exit 6 when all failed rows carry the suppression note."""

    output_root = tmp_path / "c5-output"
    output_root.mkdir()
    (output_root / "run_summary.tsv").write_text(
        "run_id\texit_code\tdownload_method_used\tsuccessful_accessions\tfailed_accessions\n"
        "run1\t6\tdehydrate\t1024\t1\n",
        encoding="utf-8",
    )
    (output_root / "accession_map.tsv").write_text(
        "requested_taxon\tncbi_accession\tdownload_status\n"
        "g__Bacteroides\tGCF_000001.1\tdownloaded\n"
        "g__Bacteroides\tGCF_003670205.1\tfailed\n",
        encoding="utf-8",
    )
    (output_root / "download_failures.tsv").write_text(
        "requested_taxon\tattempted_accession\terror_message_redacted\n"
        "g__Bacteroides\tGCF_003670205.1\tNCBI metadata marked this assembly as suppressed; the genome payload may no longer be downloadable.\n",
        encoding="utf-8",
    )
    function_text = extract_bash_function(
        Path("bin/run-real-data-tests-remote.sh"),
        "remote_check_dehydrate_suppressed_partial_result",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"{function_text}\n"
        "remote_check_dehydrate_suppressed_partial_result "
        f"{shlex.quote(str(output_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0


def test_remote_check_dehydrate_suppressed_partial_result_accepts_collapsed_download_request_failures(
    tmp_path: Path,
) -> None:
    """C5 should accept one shared suppression failure for multiple requests."""

    output_root = tmp_path / "c5-output"
    output_root.mkdir()
    (output_root / "run_summary.tsv").write_text(
        "run_id\texit_code\tdownload_method_used\tsuccessful_accessions\tfailed_accessions\n"
        "run1\t6\tdehydrate\t1024\t2\n",
        encoding="utf-8",
    )
    (output_root / "accession_map.tsv").write_text(
        "requested_taxon\tncbi_accession\tdownload_request_accession\tdownload_status\n"
        "g__Bacteroides\tGCF_000001.1\tGCF_000001.1\tdownloaded\n"
        "g__Bacteroides\tGCF_003670205.1\tGCA_003670205\tfailed\n"
        "g__Bacteroides\tGCF_003670206.1\tGCA_003670206\tfailed\n",
        encoding="utf-8",
    )
    (output_root / "download_failures.tsv").write_text(
        "requested_taxon\tattempted_accession\terror_message_redacted\n"
        "g__Bacteroides\tGCA_003670205;GCA_003670206\t"
        "NCBI metadata marked this assembly as suppressed; the genome payload may no longer be downloadable.\n",
        encoding="utf-8",
    )
    function_text = extract_bash_function(
        Path("bin/run-real-data-tests-remote.sh"),
        "remote_check_dehydrate_suppressed_partial_result",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"{function_text}\n"
        "remote_check_dehydrate_suppressed_partial_result "
        f"{shlex.quote(str(output_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0


def test_remote_check_dehydrate_suppressed_partial_result_accepts_crlf_tsvs(
    tmp_path: Path,
) -> None:
    """C5 should accept CRLF-terminated TSV rows in both checked manifests."""

    output_root = tmp_path / "c5-output"
    output_root.mkdir()
    (output_root / "run_summary.tsv").write_text(
        "run_id\texit_code\tdownload_method_used\tsuccessful_accessions\tfailed_accessions\n"
        "run1\t6\tdehydrate\t1024\t1\n",
        encoding="utf-8",
    )
    (output_root / "accession_map.tsv").write_bytes(
        (
            "requested_taxon\tncbi_accession\tdownload_status\r\n"
            "g__Bacteroides\tGCF_000001.1\tdownloaded\r\n"
            "g__Bacteroides\tGCF_003670205.1\tfailed\r\n"
        ).encode("ascii")
    )
    (output_root / "download_failures.tsv").write_bytes(
        (
            "requested_taxon\tattempted_accession\terror_message_redacted\r\n"
            "g__Bacteroides\tGCF_003670205.1\t"
            "NCBI metadata marked this assembly as suppressed; the genome payload may no longer be downloadable.\r\n"
        ).encode("ascii")
    )
    function_text = extract_bash_function(
        Path("bin/run-real-data-tests-remote.sh"),
        "remote_check_dehydrate_suppressed_partial_result",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"{function_text}\n"
        "remote_check_dehydrate_suppressed_partial_result "
        f"{shlex.quote(str(output_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode == 0


def test_real_data_runner_cases_pin_latest_smoke_to_release_226() -> None:
    """The live validation cases should not depend on the moving `latest` alias."""

    local_script = Path("bin/run-real-data-tests-local.sh").read_text(
        encoding="utf-8",
    )
    remote_script = Path("bin/run-real-data-tests-remote.sh").read_text(
        encoding="utf-8",
    )

    assert "--gtdb-release 226" in local_script
    assert "--gtdb-release 226" in remote_script
    assert "--gtdb-release latest" not in local_script
    assert "--gtdb-release latest" not in remote_script


def test_remote_check_dehydrate_suppressed_partial_result_requires_exact_download_request_token_match(
    tmp_path: Path,
) -> None:
    """Suppression-note matching should reject accession-prefix false positives."""

    output_root = tmp_path / "c5-output"
    output_root.mkdir()
    (output_root / "run_summary.tsv").write_text(
        "run_id\texit_code\tdownload_method_used\tsuccessful_accessions\tfailed_accessions\n"
        "run1\t6\tdehydrate\t1024\t1\n",
        encoding="utf-8",
    )
    (output_root / "accession_map.tsv").write_text(
        "requested_taxon\tncbi_accession\tdownload_request_accession\tdownload_status\n"
        "g__Bacteroides\tGCF_000001.1\tGCF_000001.1\tdownloaded\n"
        "g__Bacteroides\tGCF_003670205.1\tGCA_003670205\tfailed\n",
        encoding="utf-8",
    )
    (output_root / "download_failures.tsv").write_text(
        "requested_taxon\tattempted_accession\terror_message_redacted\n"
        "g__Bacteroides\tGCA_0036702050\tNCBI metadata marked this assembly as suppressed; the genome payload may no longer be downloadable.\n",
        encoding="utf-8",
    )
    function_text = extract_bash_function(
        Path("bin/run-real-data-tests-remote.sh"),
        "remote_check_dehydrate_suppressed_partial_result",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"{function_text}\n"
        "remote_check_dehydrate_suppressed_partial_result "
        f"{shlex.quote(str(output_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode != 0
    assert "lacks suppression note" in result.stderr


def test_remote_check_dehydrate_suppressed_partial_result_rejects_generic_partial_failures(
    tmp_path: Path,
) -> None:
    """C5 should still fail when a partial failure lacks the suppression note."""

    output_root = tmp_path / "c5-output"
    output_root.mkdir()
    (output_root / "run_summary.tsv").write_text(
        "run_id\texit_code\tdownload_method_used\tsuccessful_accessions\tfailed_accessions\n"
        "run1\t6\tdehydrate_fallback_direct\t1024\t1\n",
        encoding="utf-8",
    )
    (output_root / "accession_map.tsv").write_text(
        "requested_taxon\tncbi_accession\tdownload_status\n"
        "g__Bacteroides\tGCF_000001.1\tdownloaded\n"
        "g__Bacteroides\tGCF_003670205.1\tfailed\n",
        encoding="utf-8",
    )
    (output_root / "download_failures.tsv").write_text(
        "requested_taxon\tattempted_accession\terror_message_redacted\n"
        "g__Bacteroides\tGCF_003670205.1\tdownload failed after retries\n",
        encoding="utf-8",
    )
    function_text = extract_bash_function(
        Path("bin/run-real-data-tests-remote.sh"),
        "remote_check_dehydrate_suppressed_partial_result",
    )
    script = (
        f"source {shlex.quote(str(COMMON_HELPERS))}\n"
        f"{function_text}\n"
        "remote_check_dehydrate_suppressed_partial_result "
        f"{shlex.quote(str(output_root))}\n"
    )

    result = run_bash(script)

    assert result.returncode != 0
    assert "lacks suppression note" in result.stderr


def test_server_wrapper_smoke_preset_uses_remote_smoke_cases(
    tmp_path: Path,
) -> None:
    """The server wrapper should default to the smoke preset."""

    capture_file = tmp_path / "capture.txt"
    fake_runner = write_fake_remote_runner(tmp_path)

    result = subprocess.run(
        ["bash", str(SERVER_WRAPPER)],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "REAL_DATA_SERVER_REMOTE_RUNNER": str(fake_runner),
            "CAPTURE_FILE": str(capture_file),
        },
    )

    assert result.returncode == 0
    capture_text = capture_file.read_text(encoding="utf-8")
    assert "RUN_OPTIONAL_LARGE=" in capture_text
    assert "ARGC=3" in capture_text
    assert "ARGV=C1 C4 C6" in capture_text


def test_server_wrapper_full_preset_delegates_without_case_args(
    tmp_path: Path,
) -> None:
    """The `full` preset should delegate to the remote runner default suite."""

    capture_file = tmp_path / "capture.txt"
    fake_runner = write_fake_remote_runner(tmp_path)

    result = subprocess.run(
        ["bash", str(SERVER_WRAPPER), "full"],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "REAL_DATA_SERVER_REMOTE_RUNNER": str(fake_runner),
            "CAPTURE_FILE": str(capture_file),
        },
    )

    assert result.returncode == 0
    capture_text = capture_file.read_text(encoding="utf-8")
    assert "RUN_OPTIONAL_LARGE=" in capture_text
    assert "ARGC=0" in capture_text
    assert "ARGV=" in capture_text


def test_server_wrapper_full_large_sets_large_suite_flag(
    tmp_path: Path,
) -> None:
    """The `full-large` preset should enable the optional large case."""

    capture_file = tmp_path / "capture.txt"
    fake_runner = write_fake_remote_runner(tmp_path)

    result = subprocess.run(
        ["bash", str(SERVER_WRAPPER), "full-large"],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "REAL_DATA_SERVER_REMOTE_RUNNER": str(fake_runner),
            "CAPTURE_FILE": str(capture_file),
        },
    )

    assert result.returncode == 0
    capture_text = capture_file.read_text(encoding="utf-8")
    assert "RUN_OPTIONAL_LARGE=1" in capture_text
    assert "ARGC=0" in capture_text


def test_server_wrapper_passes_explicit_case_ids_through(
    tmp_path: Path,
) -> None:
    """Explicit case IDs should pass straight through to the remote runner."""

    capture_file = tmp_path / "capture.txt"
    fake_runner = write_fake_remote_runner(tmp_path)

    result = subprocess.run(
        ["bash", str(SERVER_WRAPPER), "C1", "C5", "C6"],
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "REAL_DATA_SERVER_REMOTE_RUNNER": str(fake_runner),
            "CAPTURE_FILE": str(capture_file),
        },
    )

    assert result.returncode == 0
    capture_text = capture_file.read_text(encoding="utf-8")
    assert "ARGC=3" in capture_text
    assert "ARGV=C1 C5 C6" in capture_text

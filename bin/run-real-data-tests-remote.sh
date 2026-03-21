#!/usr/bin/env bash

# Run the packaged-runtime real-data validation matrix on a remote machine.

set -u
set -o pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck source=bin/real-data-test-common.sh
. "${SCRIPT_DIR}/real-data-test-common.sh"

REMOTE_TEST_ROOT="${REMOTE_TEST_ROOT:-$(real_data_default_suite_root remote)}"


remote_check_direct_success() {
    local output_root=$1

    real_data_assert_run_summary_matches \
        "${output_root}" \
        "download_method_used" \
        '^direct$' \
        "remote direct success method" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "successful_accessions" \
        '^[1-9][0-9]*$' \
        "remote direct success count" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "failed_accessions" \
        '^0$' \
        "remote direct success zero failures" || return 1
    return 0
}


remote_check_legacy_mixed() {
    local output_root=$1

    real_data_assert_file_contains \
        "${output_root}/download_failures.tsv" \
        'unsupported_input' \
        "remote legacy unsupported_input" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "failed_accessions" \
        '^[1-9][0-9]*$' \
        "remote legacy failure count" || return 1
    return 0
}


remote_check_dehydrate_result() {
    local output_root=$1

    real_data_assert_run_summary_matches \
        "${output_root}" \
        "download_method_used" \
        '^(dehydrate|dehydrate_fallback_direct)$' \
        "remote dehydrate method" || return 1
    return 0
}


remote_check_dehydrate_suppressed_partial_result() {
    local output_root=$1
    local exit_code=""
    local failed_accession_column="download_request_accession"
    local failed_accessions=()
    local accession=""
    local suppression_note="NCBI metadata marked this assembly as suppressed; the genome payload may no longer be downloadable."

    real_data_assert_run_summary_matches \
        "${output_root}" \
        "download_method_used" \
        '^(dehydrate|dehydrate_fallback_direct)$' \
        "remote dehydrate method" || return 1

    exit_code=$(real_data_tsv_value "${output_root}/run_summary.tsv" "exit_code")
    if [ "${exit_code}" = "0" ]; then
        return 0
    fi
    if [ "${exit_code}" != "6" ]; then
        real_data_fail_message \
            "remote dehydrate partial exit: unexpected exit_code ${exit_code}"
        return 1
    fi

    real_data_assert_run_summary_matches \
        "${output_root}" \
        "successful_accessions" \
        '^[1-9][0-9]*$' \
        "remote dehydrate suppressed partial success count" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "failed_accessions" \
        '^[1-9][0-9]*$' \
        "remote dehydrate suppressed partial failure count" || return 1

    if [ ! -f "${output_root}/accession_map.tsv" ]; then
        real_data_fail_message \
            "remote dehydrate suppressed partial: missing accession_map.tsv"
        return 1
    fi
    if [ ! -f "${output_root}/download_failures.tsv" ]; then
        real_data_fail_message \
            "remote dehydrate suppressed partial: missing download_failures.tsv"
        return 1
    fi

    if ! real_data_tsv_has_column \
        "${output_root}/accession_map.tsv" \
        "${failed_accession_column}"; then
        failed_accession_column="ncbi_accession"
    fi
    if ! real_data_tsv_has_column \
        "${output_root}/accession_map.tsv" \
        "${failed_accession_column}"; then
        real_data_fail_message \
            "remote dehydrate suppressed partial: accession_map.tsv is missing download-request and original accession columns"
        return 1
    fi

    while IFS= read -r accession; do
        if [ -n "${accession}" ]; then
            failed_accessions+=("${accession}")
        fi
    done < <(
        real_data_unique_tsv_values_for_match \
            "${output_root}/accession_map.tsv" \
            "${failed_accession_column}" \
            "download_status" \
            "failed"
    )

    if [ "${#failed_accessions[@]}" -eq 0 ]; then
        real_data_fail_message \
            "remote dehydrate suppressed partial: no failed accessions found"
        return 1
    fi

    for accession in "${failed_accessions[@]}"; do
        if ! real_data_tsv_any_row_matches_exact_token_and_substring \
            "${output_root}/download_failures.tsv" \
            "attempted_accession" \
            "${accession}" \
            "error_message_redacted" \
            "${suppression_note}"; then
            real_data_fail_message \
                "remote dehydrate suppressed partial: failed accession ${accession} lacks suppression note"
            return 1
        fi
    done

    return 0
}


run_remote_case() {
    local case_id=$1
    local base_command=()
    local command=()
    local argument=""

    case "${case_id}" in
        C1)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                gtdb-genomes \
                --gtdb-release 226 \
                --gtdb-taxon "s__Thermoflexus hugenholtzii" \
                --threads 2 \
                --include genome
            ;;
        C2)
            base_command=(
                gtdb-genomes
                --gtdb-release 89
                --gtdb-taxon "s__Thermoflexus hugenholtzii"
                --prefer-genbank
                --threads 1
                --include genome
            )
            command=()
            while IFS= read -r -d '' argument; do
                command+=("${argument}")
            done < <(real_data_append_optional_ncbi_api_key "${base_command[@]}")
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                "${command[@]}"
            ;;
        C3)
            base_command=(
                gtdb-genomes
                --gtdb-release 207
                --gtdb-taxon g__Methanobrevibacter
                --prefer-genbank
                --threads 4
                --include genome,gff3
            )
            command=()
            while IFS= read -r -d '' argument; do
                command+=("${argument}")
            done < <(real_data_append_optional_ncbi_api_key "${base_command[@]}")
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                "${command[@]}"
            ;;
        C4)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 6 present 'PRJNA417962' \
                remote_check_legacy_mixed \
                gtdb-genomes \
                --gtdb-release 80 \
                --gtdb-taxon g__Acholeplasma_C \
                --threads 1 \
                --include genome
            ;;
        C5)
            base_command=(
                gtdb-genomes
                --gtdb-release 202
                --gtdb-taxon g__Bacteroides
                --prefer-genbank
                --threads 12
                --include genome
            )
            command=()
            while IFS= read -r -d '' argument; do
                command+=("${argument}")
            done < <(real_data_append_optional_ncbi_api_key "${base_command[@]}")
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" '0|6' present "" \
                remote_check_dehydrate_suppressed_partial_result \
                "${command[@]}"
            ;;
        C6)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                gtdb-genomes \
                --gtdb-release release220/220.0 \
                --gtdb-taxon "s__Thermoflexus hugenholtzii" \
                --dry-run
            ;;
        C7)
            real_data_require_ncbi_api_key
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_dehydrate_result \
                gtdb-genomes \
                --gtdb-release 214 \
                --gtdb-taxon g__Bacteroides \
                --prefer-genbank \
                --threads 12 \
                --include genome \
                --ncbi-api-key "${NCBI_API_KEY}"
            ;;
        *)
            real_data_die "Unknown remote case ID: ${case_id}"
            ;;
    esac
}


main() {
    local selected_cases=("$@")

    real_data_require_command gtdb-genomes
    real_data_require_command datasets
    real_data_require_command unzip
    if ! REAL_DATA_PYTHON_VERSION_BIN=$(real_data_detect_python_bin); then
        real_data_die "Required command not found on PATH: python or python3"
    fi
    real_data_initialise_suite "${REMOTE_TEST_ROOT}"
    real_data_record_tool_versions \
        "${REMOTE_TEST_ROOT}" \
        "${REAL_DATA_PYTHON_VERSION_BIN}"

    real_data_run_command_check \
        "${REMOTE_TEST_ROOT}" \
        "C0-which" \
        0 \
        which gtdb-genomes
    real_data_run_command_check \
        "${REMOTE_TEST_ROOT}" \
        "C0-help" \
        0 \
        gtdb-genomes --help
    real_data_run_command_check \
        "${REMOTE_TEST_ROOT}" \
        "C0-manifest" \
        4 \
        gtdb-genomes \
        --gtdb-release 226 \
        --gtdb-taxon g__DefinitelyNotReal \
        --outdir "${REMOTE_TEST_ROOT}/c0-manifest-output" \
        --dry-run

    if [ "${#selected_cases[@]}" -eq 0 ]; then
        selected_cases=(C1 C2 C3 C4 C5 C6)
        if [ "${RUN_OPTIONAL_LARGE:-0}" = "1" ]; then
            selected_cases+=(C7)
        fi
    fi

    real_data_log "Remote real-data test root: ${REMOTE_TEST_ROOT}"
    for case_id in "${selected_cases[@]}"; do
        real_data_log "Running remote case ${case_id}"
        run_remote_case "${case_id}"
    done

    real_data_log \
        "Case summary: ${REMOTE_TEST_ROOT}/_evidence/case-results.tsv"
    return "${REAL_DATA_OVERALL_STATUS}"
}


main "$@"

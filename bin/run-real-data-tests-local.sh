#!/usr/bin/env bash

# Run the release-variant local real-data validation matrix.

set -u
set -o pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck source=bin/real-data-test-common.sh
. "${SCRIPT_DIR}/real-data-test-common.sh"

LOCAL_TEST_ROOT="${LOCAL_TEST_ROOT:-/tmp/gtdb-realtests/local-$(real_data_today)}"


local_check_direct_success() {
    local output_root=$1

    real_data_assert_run_summary_matches \
        "${output_root}" \
        "download_method_used" \
        '^direct$' \
        "direct success method" || return 1
    real_data_assert_header_only \
        "${output_root}/download_failures.tsv" \
        "direct success failures" || return 1
    return 0
}


local_check_duplicate_success() {
    local output_root=$1

    local_check_direct_success "${output_root}" || return 1
    real_data_assert_any_taxon_manifest_contains \
        "${output_root}" \
        '\ttrue$' \
        "duplicate-across-taxa flag" || return 1
    real_data_assert_any_row_column_matches \
        "${output_root}/taxon_summary.tsv" \
        "duplicate_copies_written" \
        '^[1-9][0-9]*$' \
        "duplicate copy count" || return 1
    return 0
}


local_check_legacy_mixed() {
    local output_root=$1

    real_data_assert_file_contains \
        "${output_root}/download_failures.tsv" \
        'unsupported_input' \
        "legacy mixed unsupported_input" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "successful_accessions" \
        '^[1-9][0-9]*$' \
        "legacy mixed successes" || return 1
    real_data_assert_run_summary_matches \
        "${output_root}" \
        "failed_accessions" \
        '^[1-9][0-9]*$' \
        "legacy mixed failures" || return 1
    return 0
}


local_check_legacy_only() {
    local output_root=$1

    real_data_assert_file_contains \
        "${output_root}/download_failures.tsv" \
        'unsupported_input' \
        "legacy-only unsupported_input" || return 1
    real_data_assert_no_accession_directories \
        "${output_root}" \
        "legacy-only no payloads" || return 1
    return 0
}


local_check_dehydrate_candidate() {
    local output_root=$1
    local evidence_root=$2

    if [ -e "${output_root}" ]; then
        real_data_fail_message "dry-run preview case created output"
        return 1
    fi
    if ! grep -E -q 'datasets download genome accession .*--preview' \
        "${evidence_root}/combined.log"; then
        real_data_fail_message "preview command not observed for auto dry-run"
        return 1
    fi
    return 0
}


run_local_case() {
    local case_id=$1

    case "${case_id}" in
        A1)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent 'PRJNA417962' "" \
                uv run gtdb-genomes \
                --release 80 \
                --taxon g__Acholeplasma_C \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A2)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release 83 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A3)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release 86 \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A4)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release 89 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A5)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release 95 \
                --taxon g__Thermoflexus \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A6)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" \
                local_check_dehydrate_candidate \
                uv run gtdb-genomes \
                --release 202 \
                --taxon g__Bacteroides \
                --download-method auto \
                --no-prefer-genbank \
                --debug \
                --dry-run
            ;;
        A7)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release 207 \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A8)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release release220/220.0 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        A9)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                uv run gtdb-genomes \
                --release latest \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        B1)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 present "" \
                local_check_direct_success \
                uv run gtdb-genomes \
                --release 83 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --threads 1 \
                --include genome
            ;;
        B2)
            real_data_require_api_key
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 present "" \
                local_check_direct_success \
                uv run gtdb-genomes \
                --release 86 \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --prefer-genbank \
                --threads 2 \
                --include genome,gff3 \
                --api-key "${NCBI_API_KEY}"
            ;;
        B3)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 present "" \
                local_check_duplicate_success \
                uv run gtdb-genomes \
                --release 95 \
                --taxon g__Thermoflexus \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --threads 2 \
                --include genome
            ;;
        B4)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 6 present 'PRJNA417962' \
                local_check_legacy_mixed \
                uv run gtdb-genomes \
                --release 80 \
                --taxon g__Acholeplasma_C \
                --download-method direct \
                --no-prefer-genbank \
                --threads 1 \
                --include genome
            ;;
        B5)
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 7 present 'PRJNA417962' \
                local_check_legacy_only \
                uv run gtdb-genomes \
                --release 80 \
                --taxon g__UBA10030 \
                --download-method direct \
                --no-prefer-genbank \
                --threads 1 \
                --include genome
            ;;
        B6)
            real_data_require_api_key
            real_data_run_case \
                "${LOCAL_TEST_ROOT}" "${case_id}" 0 present "" \
                local_check_direct_success \
                uv run gtdb-genomes \
                --release 207 \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --prefer-genbank \
                --threads 4 \
                --include genome,gff3 \
                --api-key "${NCBI_API_KEY}"
            ;;
        *)
            real_data_die "Unknown local case ID: ${case_id}"
            ;;
    esac
}


main() {
    local selected_cases=("$@")

    real_data_require_command uv
    real_data_require_command datasets
    real_data_require_command unzip
    real_data_initialise_suite "${LOCAL_TEST_ROOT}"

    if [ "${#selected_cases[@]}" -eq 0 ]; then
        selected_cases=(
            A1 A2 A3 A4 A5 A6 A7 A8 A9
            B1 B2 B3 B4 B5 B6
        )
    fi

    real_data_log "Local real-data test root: ${LOCAL_TEST_ROOT}"
    for case_id in "${selected_cases[@]}"; do
        real_data_log "Running local case ${case_id}"
        run_local_case "${case_id}"
    done

    real_data_log \
        "Case summary: ${LOCAL_TEST_ROOT}/_evidence/case-results.tsv"
    return "${REAL_DATA_OVERALL_STATUS}"
}


main "$@"

#!/usr/bin/env bash

# Run the packaged-runtime real-data validation matrix on a remote machine.

set -u
set -o pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck source=bin/real-data-test-common.sh
. "${SCRIPT_DIR}/real-data-test-common.sh"

REMOTE_TEST_ROOT="${REMOTE_TEST_ROOT:-/tmp/gtdb-realtests/remote-$(real_data_today)}"


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


run_remote_case() {
    local case_id=$1

    case "${case_id}" in
        C1)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                gtdb-genomes \
                --release latest \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --threads 2 \
                --include genome
            ;;
        C2)
            real_data_require_ncbi_api_key
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                gtdb-genomes \
                --release 89 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --prefer-genbank \
                --threads 1 \
                --include genome \
                --ncbi-api-key "${NCBI_API_KEY}"
            ;;
        C3)
            real_data_require_ncbi_api_key
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_direct_success \
                gtdb-genomes \
                --release 207 \
                --taxon g__Methanobrevibacter \
                --download-method direct \
                --prefer-genbank \
                --threads 4 \
                --include genome,gff3 \
                --ncbi-api-key "${NCBI_API_KEY}"
            ;;
        C4)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 6 present 'PRJNA417962' \
                remote_check_legacy_mixed \
                gtdb-genomes \
                --release 80 \
                --taxon g__Acholeplasma_C \
                --download-method direct \
                --no-prefer-genbank \
                --threads 1 \
                --include genome
            ;;
        C5)
            real_data_require_ncbi_api_key
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_dehydrate_result \
                gtdb-genomes \
                --release 202 \
                --taxon g__Bacteroides \
                --download-method auto \
                --prefer-genbank \
                --threads 12 \
                --include genome \
                --ncbi-api-key "${NCBI_API_KEY}"
            ;;
        C6)
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 absent "" "" \
                gtdb-genomes \
                --release release220/220.0 \
                --taxon "s__Thermoflexus hugenholtzii" \
                --download-method direct \
                --no-prefer-genbank \
                --dry-run
            ;;
        C7)
            real_data_require_ncbi_api_key
            real_data_run_case \
                "${REMOTE_TEST_ROOT}" "${case_id}" 0 present "" \
                remote_check_dehydrate_result \
                gtdb-genomes \
                --release 214 \
                --taxon g__Bacteroides \
                --download-method auto \
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
    real_data_require_command python
    real_data_require_command datasets
    real_data_require_command unzip
    real_data_initialise_suite "${REMOTE_TEST_ROOT}"

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
        0 \
        python -c \
        "from gtdb_genomes.release_resolver import get_release_manifest_path; path = get_release_manifest_path(); assert path.is_file(), path"

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

#!/usr/bin/env bash

# Common helpers for release-variant real-data validation.

set -u
set -o pipefail

REAL_DATA_OVERALL_STATUS=0
REAL_DATA_CASE_RESULTS_FILE=""


real_data_today() {
    date "+%Y%m%d"
}


real_data_log() {
    printf '%s\n' "$*"
}


real_data_fail_message() {
    printf 'FAIL: %s\n' "$*" >&2
}


real_data_die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}


real_data_require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        real_data_die "Required command not found on PATH: $1"
    fi
}


real_data_require_api_key() {
    if [ -z "${NCBI_API_KEY:-}" ]; then
        real_data_die "NCBI_API_KEY is required for this case"
    fi
}


real_data_initialise_suite() {
    local test_root=$1
    local evidence_root="${test_root}/_evidence"

    mkdir -p "${evidence_root}"
    REAL_DATA_CASE_RESULTS_FILE="${evidence_root}/case-results.tsv"
    if [ ! -f "${REAL_DATA_CASE_RESULTS_FILE}" ]; then
        printf 'case_id\tstatus\texpected_exit\tactual_exit\toutput_root\n' \
            > "${REAL_DATA_CASE_RESULTS_FILE}"
    fi
}


real_data_write_command_file() {
    local command_file=$1

    shift
    : > "${command_file}"
    printf '%q ' "$@" >> "${command_file}"
    printf '\n' >> "${command_file}"
}


real_data_copy_if_present() {
    local source_path=$1
    local destination_path=$2

    if [ -f "${source_path}" ]; then
        cp "${source_path}" "${destination_path}"
    fi
}


real_data_record_output_evidence() {
    local output_root=$1
    local evidence_root=$2

    if [ ! -d "${output_root}" ]; then
        return 0
    fi

    real_data_copy_if_present \
        "${output_root}/run_summary.tsv" \
        "${evidence_root}/run_summary.tsv"
    real_data_copy_if_present \
        "${output_root}/taxon_summary.tsv" \
        "${evidence_root}/taxon_summary.tsv"
    real_data_copy_if_present \
        "${output_root}/accession_map.tsv" \
        "${evidence_root}/accession_map.tsv"
    real_data_copy_if_present \
        "${output_root}/download_failures.tsv" \
        "${evidence_root}/download_failures.tsv"

    du -sh "${output_root}" > "${evidence_root}/output-size.txt" 2>/dev/null || true
    if [ -d "${output_root}/taxa" ]; then
        find "${output_root}/taxa" -maxdepth 2 -type d | sort \
            > "${evidence_root}/taxa-find.txt"
    fi
}


real_data_tsv_value() {
    local tsv_path=$1
    local column_name=$2

    awk -F '\t' -v column_name="${column_name}" '
        NR == 1 {
            for (index = 1; index <= NF; index += 1) {
                if ($index == column_name) {
                    column_index = index
                }
            }
        }
        NR == 2 && column_index > 0 {
            print $column_index
            exit 0
        }
    ' "${tsv_path}"
}


real_data_assert_file_contains() {
    local file_path=$1
    local pattern=$2
    local description=$3

    if [ ! -f "${file_path}" ]; then
        real_data_fail_message "${description}: missing file ${file_path}"
        return 1
    fi
    if ! grep -E -q "${pattern}" "${file_path}"; then
        real_data_fail_message "${description}: pattern not found"
        return 1
    fi
    return 0
}


real_data_assert_header_only() {
    local file_path=$1
    local description=$2
    local line_count=0

    if [ ! -f "${file_path}" ]; then
        real_data_fail_message "${description}: missing file ${file_path}"
        return 1
    fi
    line_count=$(wc -l < "${file_path}")
    if [ "${line_count}" -ne 1 ]; then
        real_data_fail_message "${description}: expected header-only TSV"
        return 1
    fi
    return 0
}


real_data_assert_run_summary_matches() {
    local output_root=$1
    local column_name=$2
    local pattern=$3
    local description=$4
    local value=""

    if [ ! -f "${output_root}/run_summary.tsv" ]; then
        real_data_fail_message "${description}: missing run_summary.tsv"
        return 1
    fi
    value=$(real_data_tsv_value "${output_root}/run_summary.tsv" "${column_name}")
    if ! printf '%s\n' "${value}" | grep -E -q "${pattern}"; then
        real_data_fail_message \
            "${description}: value '${value}' does not match ${pattern}"
        return 1
    fi
    return 0
}


real_data_assert_any_row_column_matches() {
    local tsv_path=$1
    local column_name=$2
    local pattern=$3
    local description=$4

    if [ ! -f "${tsv_path}" ]; then
        real_data_fail_message "${description}: missing file ${tsv_path}"
        return 1
    fi
    if ! awk -F '\t' -v column_name="${column_name}" -v pattern="${pattern}" '
        NR == 1 {
            for (index = 1; index <= NF; index += 1) {
                if ($index == column_name) {
                    column_index = index
                }
            }
        }
        NR > 1 && column_index > 0 && $column_index ~ pattern {
            found = 1
        }
        END {
            exit(found ? 0 : 1)
        }
    ' "${tsv_path}"; then
        real_data_fail_message "${description}: no matching row found"
        return 1
    fi
    return 0
}


real_data_assert_any_taxon_manifest_contains() {
    local output_root=$1
    local pattern=$2
    local description=$3
    local manifest_path=""

    if [ ! -d "${output_root}/taxa" ]; then
        real_data_fail_message "${description}: missing taxa directory"
        return 1
    fi
    while IFS= read -r manifest_path; do
        if grep -E -q "${pattern}" "${manifest_path}"; then
            return 0
        fi
    done <<EOF
$(find "${output_root}/taxa" -name "taxon_accessions.tsv" -type f | sort)
EOF
    real_data_fail_message "${description}: pattern not found"
    return 1
}


real_data_assert_no_accession_directories() {
    local output_root=$1
    local description=$2

    if [ ! -d "${output_root}/taxa" ]; then
        return 0
    fi
    if find "${output_root}/taxa" -mindepth 2 -maxdepth 2 -type d | \
        grep -q '.'; then
        real_data_fail_message "${description}: accession directories exist"
        return 1
    fi
    return 0
}


real_data_run_command_check() {
    local test_root=$1
    local check_id=$2
    local expected_exit=$3
    local evidence_root="${test_root}/_evidence/${check_id}"
    local stdout_file="${evidence_root}/stdout.log"
    local stderr_file="${evidence_root}/stderr.log"
    local combined_file="${evidence_root}/combined.log"
    local summary_file="${evidence_root}/summary.txt"
    local command_file="${evidence_root}/command.sh"
    local actual_exit=0
    local status="PASS"
    local start_epoch=0
    local end_epoch=0

    shift 3
    mkdir -p "${evidence_root}"
    real_data_write_command_file "${command_file}" "$@"

    start_epoch=$(date +%s)
    "$@" > "${stdout_file}" 2> "${stderr_file}"
    actual_exit=$?
    end_epoch=$(date +%s)
    cat "${stdout_file}" "${stderr_file}" > "${combined_file}"

    if [ "${actual_exit}" -ne "${expected_exit}" ]; then
        status="FAIL"
        REAL_DATA_OVERALL_STATUS=1
    fi

    {
        printf 'check_id=%s\n' "${check_id}"
        printf 'status=%s\n' "${status}"
        printf 'expected_exit=%s\n' "${expected_exit}"
        printf 'actual_exit=%s\n' "${actual_exit}"
        printf 'elapsed_seconds=%s\n' "$((end_epoch - start_epoch))"
    } > "${summary_file}"
}


real_data_run_case() {
    local test_root=$1
    local case_id=$2
    local expected_exit=$3
    local expect_output=$4
    local warning_pattern=$5
    local post_check_function=$6
    local output_root="${test_root}/${case_id}"
    local evidence_root="${test_root}/_evidence/${case_id}"
    local stdout_file="${evidence_root}/stdout.log"
    local stderr_file="${evidence_root}/stderr.log"
    local combined_file="${evidence_root}/combined.log"
    local summary_file="${evidence_root}/summary.txt"
    local command_file="${evidence_root}/command.sh"
    local actual_exit=0
    local status="PASS"
    local start_epoch=0
    local end_epoch=0

    shift 6

    if [ -e "${output_root}" ]; then
        real_data_fail_message \
            "${case_id}: output path already exists: ${output_root}"
        REAL_DATA_OVERALL_STATUS=1
        return 1
    fi

    mkdir -p "${evidence_root}"
    real_data_write_command_file "${command_file}" "$@" --output "${output_root}"

    start_epoch=$(date +%s)
    "$@" --output "${output_root}" > "${stdout_file}" 2> "${stderr_file}"
    actual_exit=$?
    end_epoch=$(date +%s)
    cat "${stdout_file}" "${stderr_file}" > "${combined_file}"

    if [ "${actual_exit}" -ne "${expected_exit}" ]; then
        status="FAIL"
        real_data_fail_message \
            "${case_id}: expected exit ${expected_exit}, got ${actual_exit}"
    fi

    if [ "${expect_output}" = "absent" ] && [ -e "${output_root}" ]; then
        status="FAIL"
        real_data_fail_message "${case_id}: output directory should be absent"
    fi
    if [ "${expect_output}" = "present" ] && [ ! -d "${output_root}" ]; then
        status="FAIL"
        real_data_fail_message "${case_id}: output directory is missing"
    fi

    if [ -n "${warning_pattern}" ] && \
        ! grep -E -q "${warning_pattern}" "${combined_file}"; then
        status="FAIL"
        real_data_fail_message "${case_id}: expected warning not found"
    fi

    real_data_record_output_evidence "${output_root}" "${evidence_root}"

    if [ -n "${post_check_function}" ]; then
        if ! "${post_check_function}" "${output_root}" "${evidence_root}"; then
            status="FAIL"
        fi
    fi

    {
        printf 'case_id=%s\n' "${case_id}"
        printf 'status=%s\n' "${status}"
        printf 'expected_exit=%s\n' "${expected_exit}"
        printf 'actual_exit=%s\n' "${actual_exit}"
        printf 'elapsed_seconds=%s\n' "$((end_epoch - start_epoch))"
        printf 'output_root=%s\n' "${output_root}"
    } > "${summary_file}"

    printf '%s\t%s\t%s\t%s\t%s\n' \
        "${case_id}" \
        "${status}" \
        "${expected_exit}" \
        "${actual_exit}" \
        "${output_root}" >> "${REAL_DATA_CASE_RESULTS_FILE}"

    if [ "${status}" != "PASS" ]; then
        REAL_DATA_OVERALL_STATUS=1
    fi

    return 0
}

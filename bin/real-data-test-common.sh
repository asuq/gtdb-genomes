#!/usr/bin/env bash

# Common helpers for release-variant real-data validation.

set -u
set -o pipefail

REAL_DATA_OVERALL_STATUS=0
REAL_DATA_CASE_RESULTS_FILE=""
REAL_DATA_PYTHON_VERSION_BIN="${REAL_DATA_PYTHON_VERSION_BIN:-}"
REAL_DATA_PREPARED_COMMAND=()


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


real_data_require_ncbi_api_key() {
    if [ -z "${NCBI_API_KEY:-}" ]; then
        real_data_die "NCBI_API_KEY is required for this case"
    fi
}


real_data_append_optional_ncbi_api_key() {
    local command=("$@")

    if [ -n "${NCBI_API_KEY:-}" ]; then
        command+=(--ncbi-api-key "${NCBI_API_KEY}")
    fi

    printf '%s\0' "${command[@]}"
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


real_data_default_suite_root() {
    local suite_name=$1
    local suite_root_base="${TMPDIR:-/tmp}/gtdb-realtests"

    mkdir -p "${suite_root_base}"
    mktemp -d "${suite_root_base}/${suite_name}-$(real_data_today)-XXXXXX"
}


real_data_detect_python_bin() {
    if command -v python >/dev/null 2>&1; then
        command -v python
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        command -v python3
        return 0
    fi
    return 1
}


real_data_redact_value() {
    local value=$1

    if [ -n "${NCBI_API_KEY:-}" ]; then
        printf '%s' "${value//${NCBI_API_KEY}/[REDACTED]}"
        return 0
    fi
    printf '%s' "${value}"
}


real_data_cleanup_temp_dir() {
    local temp_dir=$1

    if [ -n "${temp_dir}" ] && [ -d "${temp_dir}" ]; then
        rm -rf "${temp_dir}"
    fi
}


real_data_write_command_file() {
    local command_file=$1
    local redact_next=0
    local argument=""

    shift
    : > "${command_file}"
    for argument in "$@"; do
        if [ "${redact_next}" -eq 1 ]; then
            printf '%q ' "[REDACTED]" >> "${command_file}"
            redact_next=0
            continue
        fi
        if [[ "${argument}" == --ncbi-api-key=* ]]; then
            printf '%q ' "--ncbi-api-key=[REDACTED]" >> "${command_file}"
            continue
        fi
        printf '%q ' "${argument}" >> "${command_file}"
        if [ "${argument}" = "--ncbi-api-key" ]; then
            redact_next=1
        fi
    done
    printf '\n' >> "${command_file}"
}


real_data_redact_file() {
    local source_path=$1
    local destination_path=$2
    local line=""

    : > "${destination_path}"
    while IFS= read -r line || [ -n "${line}" ]; do
        real_data_redact_value "${line}" >> "${destination_path}"
        printf '\n' >> "${destination_path}"
    done < "${source_path}"
}


real_data_record_tool_versions() {
    local test_root=$1
    local python_bin=${2:-}
    local evidence_root="${test_root}/_evidence"
    local version_file="${evidence_root}/tool-versions.txt"
    local detected_python=""

    mkdir -p "${evidence_root}"
    if [ -n "${python_bin}" ] && [ -x "${python_bin}" ]; then
        detected_python="${python_bin}"
    elif detected_python=$(real_data_detect_python_bin); then
        :
    fi

    {
        if [ -n "${detected_python}" ]; then
            printf 'python_bin=%s\n' "${detected_python}"
            printf 'python_version=%s\n' "$("${detected_python}" --version 2>&1)"
        else
            printf 'python_bin=unavailable\n'
            printf 'python_version=unavailable\n'
        fi

        if command -v datasets >/dev/null 2>&1; then
            printf 'datasets_bin=%s\n' "$(command -v datasets)"
            printf 'datasets_version=%s\n' "$(datasets version 2>&1)"
        else
            printf 'datasets_bin=unavailable\n'
            printf 'datasets_version=unavailable\n'
        fi
    } > "${version_file}"
}


real_data_copy_if_present() {
    local source_path=$1
    local destination_path=$2

    if [ -f "${source_path}" ]; then
        cp "${source_path}" "${destination_path}"
    fi
}


real_data_command_uses_ncbi_api_key() {
    local argument=""

    for argument in "$@"; do
        if [ "${argument}" = "--ncbi-api-key" ] || \
            [[ "${argument}" == --ncbi-api-key=* ]]; then
            return 0
        fi
    done
    return 1
}


real_data_prepare_case_command() {
    REAL_DATA_PREPARED_COMMAND=("$@")

    if [ "${REAL_DATA_DEBUG_SAFE:-0}" = "1" ] && \
        ! real_data_command_uses_ncbi_api_key "${REAL_DATA_PREPARED_COMMAND[@]}"; then
        REAL_DATA_PREPARED_COMMAND+=(--debug)
    fi
    if [ "${REAL_DATA_PYTHON_FAULTHANDLER:-0}" = "1" ]; then
        REAL_DATA_PREPARED_COMMAND=(
            env
            PYTHONFAULTHANDLER=1
            "${REAL_DATA_PREPARED_COMMAND[@]}"
        )
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
    real_data_copy_if_present \
        "${output_root}/debug.log" \
        "${evidence_root}/debug.log"

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
            for (field_index = 1; field_index <= NF; field_index += 1) {
                header_value = $field_index
                sub(/\r$/, "", header_value)
                if (header_value == column_name) {
                    column_index = field_index
                }
            }
        }
        NR == 2 && column_index > 0 {
            value = $column_index
            sub(/\r$/, "", value)
            print value
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


real_data_assert_case_exit_matches() {
    local actual_exit=$1
    local expected_exit_pattern=$2
    local description=$3

    if ! printf '%s\n' "${actual_exit}" | grep -E -q "^(${expected_exit_pattern})$"; then
        real_data_fail_message \
            "${description}: value '${actual_exit}' does not match ${expected_exit_pattern}"
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
            for (field_index = 1; field_index <= NF; field_index += 1) {
                header_value = $field_index
                sub(/\r$/, "", header_value)
                if (header_value == column_name) {
                    column_index = field_index
                }
            }
        }
        NR > 1 && column_index > 0 {
            value = $column_index
            sub(/\r$/, "", value)
            if (value ~ pattern) {
                found = 1
            }
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


real_data_assert_any_taxon_manifest_row_column_matches() {
    local output_root=$1
    local column_name=$2
    local pattern=$3
    local description=$4
    local manifest_path=""

    if [ ! -d "${output_root}/taxa" ]; then
        real_data_fail_message "${description}: missing taxa directory"
        return 1
    fi
    while IFS= read -r manifest_path; do
        if [ -n "${manifest_path}" ] && \
            real_data_assert_any_row_column_matches \
                "${manifest_path}" \
                "${column_name}" \
                "${pattern}" \
                "${description}" >/dev/null 2>&1; then
            return 0
        fi
    done <<EOF
$(find "${output_root}/taxa" -name "taxon_accessions.tsv" -type f | sort)
EOF
    real_data_fail_message "${description}: no matching row found"
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
    local temp_dir=""
    local raw_stdout_file=""
    local raw_stderr_file=""
    local temp_dir_escaped=""

    shift 3
    mkdir -p "${evidence_root}"
    real_data_write_command_file "${command_file}" "$@"
    temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/gtdb_real_command.XXXXXX")
    raw_stdout_file="${temp_dir}/stdout.log"
    raw_stderr_file="${temp_dir}/stderr.log"
    printf -v temp_dir_escaped '%q' "${temp_dir}"
    trap "real_data_cleanup_temp_dir ${temp_dir_escaped}" EXIT INT TERM HUP

    start_epoch=$(date +%s)
    "$@" > "${raw_stdout_file}" 2> "${raw_stderr_file}"
    actual_exit=$?
    end_epoch=$(date +%s)
    real_data_redact_file "${raw_stdout_file}" "${stdout_file}"
    real_data_redact_file "${raw_stderr_file}" "${stderr_file}"
    cat "${stdout_file}" "${stderr_file}" > "${combined_file}"
    trap - EXIT INT TERM HUP
    real_data_cleanup_temp_dir "${temp_dir}"

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
    local expected_exit_pattern=$3
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
    local temp_dir=""
    local raw_stdout_file=""
    local raw_stderr_file=""
    local temp_dir_escaped=""
    local command=()

    shift 6

    if [ -e "${output_root}" ]; then
        real_data_fail_message \
            "${case_id}: output path already exists: ${output_root}"
        REAL_DATA_OVERALL_STATUS=1
        return 1
    fi

    mkdir -p "${evidence_root}"
    real_data_prepare_case_command "$@"
    command=("${REAL_DATA_PREPARED_COMMAND[@]}")
    real_data_write_command_file \
        "${command_file}" \
        "${command[@]}" \
        --outdir "${output_root}"
    temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/gtdb_real_command.XXXXXX")
    raw_stdout_file="${temp_dir}/stdout.log"
    raw_stderr_file="${temp_dir}/stderr.log"
    printf -v temp_dir_escaped '%q' "${temp_dir}"
    trap "real_data_cleanup_temp_dir ${temp_dir_escaped}" EXIT INT TERM HUP

    start_epoch=$(date +%s)
    "${command[@]}" --outdir "${output_root}" > "${raw_stdout_file}" 2> "${raw_stderr_file}"
    actual_exit=$?
    end_epoch=$(date +%s)
    real_data_redact_file "${raw_stdout_file}" "${stdout_file}"
    real_data_redact_file "${raw_stderr_file}" "${stderr_file}"
    cat "${stdout_file}" "${stderr_file}" > "${combined_file}"
    trap - EXIT INT TERM HUP
    real_data_cleanup_temp_dir "${temp_dir}"

    if ! real_data_assert_case_exit_matches \
        "${actual_exit}" \
        "${expected_exit_pattern}" \
        "${case_id} expected exit"; then
        status="FAIL"
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
        printf 'expected_exit=%s\n' "${expected_exit_pattern}"
        printf 'actual_exit=%s\n' "${actual_exit}"
        printf 'elapsed_seconds=%s\n' "$((end_epoch - start_epoch))"
        printf 'output_root=%s\n' "${output_root}"
    } > "${summary_file}"

    printf '%s\t%s\t%s\t%s\t%s\n' \
        "${case_id}" \
        "${status}" \
        "${expected_exit_pattern}" \
        "${actual_exit}" \
        "${output_root}" >> "${REAL_DATA_CASE_RESULTS_FILE}"

    if [ "${status}" != "PASS" ]; then
        REAL_DATA_OVERALL_STATUS=1
    fi

    return 0
}

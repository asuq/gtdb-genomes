#!/usr/bin/env bash

# Install a pinned micromamba binary for GitHub Actions CI jobs.

set -eu
set -o pipefail

MICROMAMBA_VERSION="2.3.3-0"
MICROMAMBA_SHA256="9496f94a8b78c536573c93d946ec9bba74bd9ff79ee55aaa4b546e30db8f511b"
MICROMAMBA_URL="https://github.com/mamba-org/micromamba-releases/releases/download/${MICROMAMBA_VERSION}/micromamba-linux-64"


require_env_var() {
    local variable_name=$1

    if [ -z "${!variable_name:-}" ]; then
        printf 'ERROR: required environment variable is not set: %s\n' \
            "${variable_name}" >&2
        exit 1
    fi
}


main() {
    local install_root=""
    local micromamba_bin=""

    require_env_var "RUNNER_TEMP"
    require_env_var "GITHUB_PATH"
    require_env_var "GITHUB_ENV"

    install_root="${RUNNER_TEMP}/micromamba-bin"
    micromamba_bin="${install_root}/micromamba"

    mkdir -p "${install_root}"
    curl -fsSL "${MICROMAMBA_URL}" -o "${micromamba_bin}"
    printf '%s  %s\n' "${MICROMAMBA_SHA256}" "${micromamba_bin}" | sha256sum -c -
    chmod +x "${micromamba_bin}"

    printf '%s\n' "${install_root}" >> "${GITHUB_PATH}"
    printf 'MAMBA_ROOT_PREFIX=%s\n' "${RUNNER_TEMP}/micromamba-root" \
        >> "${GITHUB_ENV}"
}


main "$@"

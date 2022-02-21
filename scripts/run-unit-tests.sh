#!/bin/bash

set -eo pipefail

run_tests() {
    local -a race_flags=()
    if [ "$(uname -m)" == "x86_64" ]; then
        export GORACE=atexit_sleep_ms=0 # reduce overhead of race
        race_flags+=("-race")
    fi

    time {
        go test "${flags[@]}" "${race_flags[@]}" -failfast -tags "$GO_TAGS" "$@" -short -timeout=20m -count=1
    }
}

main() {
    make binary

    # default behavior is to run all tests
    local -a package_spec=("${TEST_PKGS:-github.com/hyperledger-labs/orion-sdk-go/...}")

    # expand the package specs into arrays of packages
    local -a packages
    while IFS= read -r pkg; do packages+=("$pkg"); done < <(go list "${package_spec[@]}")

    if [ "${#packages[@]}" -eq 0 ]; then
        echo "Nothing to test!!!"
    else
        run_tests "${packages[@]}"
    fi
}

main

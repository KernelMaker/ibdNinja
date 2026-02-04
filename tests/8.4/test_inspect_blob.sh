#!/bin/bash
#
# test_inspect_blob.sh - Test the --inspect-blob (-I) feature with JSON fixtures
#
# Tests LOB version chain traversal and binary JSON decoding by:
# 1. Running inspect-blob on each JSON field
# 2. Going through all LOB versions for each field
# 3. Comparing decoded JSON output against expected golden files
#
# Usage: ./test_inspect_blob.sh [options]
#   -v, --verbose     Show diff output on failures
#   -u, --update      Update golden files instead of testing
#   -h, --help        Show this help message
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
EXPECTED_DIR="$SCRIPT_DIR/expected"
IBDNINJA="$PROJECT_DIR/ibdNinja"

VERBOSE=0
UPDATE_MODE=0

# Test counters
PASS=0
FAIL=0
SKIP=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -v, --verbose     Show diff output on failures"
    echo "  -u, --update      Update golden files instead of testing"
    echo "  -h, --help        Show this help message"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose) VERBOSE=1; shift ;;
        -u|--update) UPDATE_MODE=1; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); }
log_skip() { echo -e "${YELLOW}[SKIP]${NC} $1"; SKIP=$((SKIP + 1)); }
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }

if [ ! -x "$IBDNINJA" ]; then
    echo -e "${RED}Error: ibdNinja binary not found at $IBDNINJA${NC}"
    echo "Please run 'make' first."
    exit 1
fi

FIXTURE="$FIXTURES_DIR/json_partial.ibd"
if [ ! -f "$FIXTURE" ]; then
    echo -e "${YELLOW}Skipping inspect-blob tests: json_partial.ibd fixture not found${NC}"
    echo "Run 'make test-fixtures-8.0' to generate it."
    exit 0
fi

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

mkdir -p "$EXPECTED_DIR"

normalize_output() {
    local file="$1"
    sed -i.bak "s|$FIXTURES_DIR|<FIXTURES>|g" "$file"
    rm -f "${file}.bak"
}

# Extract just the JSON value line(s) from inspect-blob output.
# Looks for lines after "[JSON value" marker and captures the JSON text.
extract_json_value() {
    local file="$1"
    # The JSON value follows a line like "[JSON value (...)]:" or "[JSON value vN (...)]:"
    # The next line is the actual JSON text
    # Note: || true prevents exit code 1 when grep finds no matches (e.g., purge decline test)
    grep -A1 '^\[JSON value' "$file" | grep -v '^\[JSON value' | grep -v '^--$' || true
}

# Run an inspect-blob test case with a specified fixture file.
# Args: fixture_path test_name page_no rec_no stdin_input
run_inspect_test_fixture() {
    local fixture_path="$1"
    local test_name="$2"
    local page_no="$3"
    local rec_no="$4"
    local stdin_input="$5"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"
    local json_file="$TMPDIR/${test_name}_json.txt"

    # Run ibdNinja with scripted stdin
    printf '%s' "$stdin_input" | "$IBDNINJA" --file "$fixture_path" -I "${page_no},${rec_no}" > "$output_file" 2>&1 || true

    # Extract JSON value from output
    extract_json_value "$output_file" > "$json_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$json_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file, run with -u to create)"
        return
    fi

    if diff -q "$expected_file" "$json_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            echo "--- Expected (first 200 chars) ---"
            head -c 200 "$expected_file"
            echo ""
            echo "--- Actual (first 200 chars) ---"
            head -c 200 "$json_file"
            echo ""
            echo "--- Diff ---"
            diff "$expected_file" "$json_file" | head -30 || true
            echo ""
        fi
    fi
}

# Run an inspect-blob test case using the default FIXTURE.
# Args: test_name page_no rec_no stdin_input
run_inspect_test() {
    run_inspect_test_fixture "$FIXTURE" "$@"
}

echo ""
echo "=========================================="
echo "  Inspect-Blob JSON Tests"
echo "=========================================="
echo ""

if [ $UPDATE_MODE -eq 1 ]; then
    log_info "Running in UPDATE mode - golden files will be updated"
    echo ""
fi

PAGE=4
REC=1

# ===================================================================
# doc1 tests (field 1 of 2 external fields)
# doc1 has 3 LOB versions: v1 (payload=AAA), v2 (payload=XXX), v3 (payload=MMM)
# ===================================================================

echo -e "${BLUE}Testing: json_partial doc1${NC}"

# doc1 current version (v3): select field 1, action 1, exit
run_inspect_test "json_partial_inspect_doc1_current" $PAGE $REC "$(printf '1\n1\n0\n')"

# doc1 version 1: select field 1, action 2 (specific version), enter 1, exit
run_inspect_test "json_partial_inspect_doc1_v1" $PAGE $REC "$(printf '1\n2\n1\n0\n')"

# doc1 version 2: select field 1, action 2 (specific version), enter 2, exit
run_inspect_test "json_partial_inspect_doc1_v2" $PAGE $REC "$(printf '1\n2\n2\n0\n')"

# doc1 version 3: select field 1, action 2 (specific version), enter 3, exit
run_inspect_test "json_partial_inspect_doc1_v3" $PAGE $REC "$(printf '1\n2\n3\n0\n')"

echo ""

# ===================================================================
# doc2 tests (field 2 of 2 external fields)
# doc2 has 2 LOB versions: v1 (large_text=BBB), v2 (large_text=YYY)
# ===================================================================

echo -e "${BLUE}Testing: json_partial doc2${NC}"

# doc2 current version (v2): select field 2, action 1, exit
run_inspect_test "json_partial_inspect_doc2_current" $PAGE $REC "$(printf '2\n1\n0\n')"

# doc2 version 1: select field 2, action 2 (specific version), enter 1, exit
run_inspect_test "json_partial_inspect_doc2_v1" $PAGE $REC "$(printf '2\n2\n1\n0\n')"

# doc2 version 2: select field 2, action 2 (specific version), enter 2, exit
run_inspect_test "json_partial_inspect_doc2_v2" $PAGE $REC "$(printf '2\n2\n2\n0\n')"

# ===================================================================
# json_partial_large tests (multi-entry LOB with version chains on different entries)
# doc has 4 versions: v1 (initial), v2 (chunk_a=XXX), v3 (chunk_c=ZZZ), v4 (chunk_a=WWW)
# ===================================================================

LARGE_FIXTURE="$FIXTURES_DIR/json_partial_large.ibd"
if [ -f "$LARGE_FIXTURE" ]; then
    echo ""
    echo -e "${BLUE}Testing: json_partial_large${NC}"

    LARGE_PAGE=4
    LARGE_REC=1

    # current version (v4): action 1, exit
    run_inspect_test_fixture "$LARGE_FIXTURE" "json_partial_large_inspect_current" $LARGE_PAGE $LARGE_REC "$(printf '1\n0\n')"

    # version 1: action 2, enter 1, exit
    run_inspect_test_fixture "$LARGE_FIXTURE" "json_partial_large_inspect_v1" $LARGE_PAGE $LARGE_REC "$(printf '2\n1\n0\n')"

    # version 2: action 2, enter 2, exit
    run_inspect_test_fixture "$LARGE_FIXTURE" "json_partial_large_inspect_v2" $LARGE_PAGE $LARGE_REC "$(printf '2\n2\n0\n')"

    # version 3: action 2, enter 3, exit
    run_inspect_test_fixture "$LARGE_FIXTURE" "json_partial_large_inspect_v3" $LARGE_PAGE $LARGE_REC "$(printf '2\n3\n0\n')"

    # version 4: action 2, enter 4, exit
    run_inspect_test_fixture "$LARGE_FIXTURE" "json_partial_large_inspect_v4" $LARGE_PAGE $LARGE_REC "$(printf '2\n4\n0\n')"
else
    echo ""
    log_skip "json_partial_large tests (fixture not found)"
fi

# ===================================================================
# json_partial_purged tests (purged version detection and fallback)
# Only current version should be visible; older versions have been purged.
# ===================================================================

PURGED_FIXTURE="$FIXTURES_DIR/json_partial_purged.ibd"
if [ -f "$PURGED_FIXTURE" ]; then
    echo ""
    echo -e "${BLUE}Testing: json_partial_purged${NC}"

    PURGED_PAGE=4
    PURGED_REC=1

    # current version: action 1, exit
    run_inspect_test_fixture "$PURGED_FIXTURE" "json_partial_purged_inspect_current" $PURGED_PAGE $PURGED_REC "$(printf '1\n0\n')"

    # version 1 with fallback accept: action 2, enter 1 (purged), accept 'y', exit
    run_inspect_test_fixture "$PURGED_FIXTURE" "json_partial_purged_inspect_v1_fallback" $PURGED_PAGE $PURGED_REC "$(printf '2\n1\ny\n0\n')"

    # version 1 decline fallback: action 2, enter 1 (purged), decline 'n', exit
    run_inspect_test_fixture "$PURGED_FIXTURE" "json_partial_purged_inspect_v1_decline" $PURGED_PAGE $PURGED_REC "$(printf '2\n1\nn\n0\n')"

    # current version via action 2: action 2, enter 4 (current), exit
    run_inspect_test_fixture "$PURGED_FIXTURE" "json_partial_purged_inspect_v4" $PURGED_PAGE $PURGED_REC "$(printf '2\n4\n0\n')"
else
    echo ""
    log_skip "json_partial_purged tests (fixture not found)"
fi

echo ""

# ===================================================================
# Summary
# ===================================================================

echo "=========================================="
echo "  Inspect-Blob Test Summary"
echo "=========================================="
echo ""
echo -e "  ${GREEN}Passed:${NC}  $PASS"
echo -e "  ${RED}Failed:${NC}  $FAIL"
echo -e "  ${YELLOW}Skipped:${NC} $SKIP"
echo ""

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    echo "Run with -v for verbose output, or -u to update golden files."
    exit 1
elif [ $PASS -eq 0 ] && [ $SKIP -gt 0 ]; then
    echo -e "${YELLOW}No tests were run (all skipped).${NC}"
    echo "Run with -u to generate initial golden files."
    exit 0
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi

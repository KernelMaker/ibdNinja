#!/bin/bash
#
# run_tests.sh - Run ibdNinja golden file tests
#
# Usage: ./run_tests.sh [options]
#   -v, --verbose     Show diff output on failures
#   -u, --update      Update golden files instead of testing
#   -f, --filter      Only run tests matching pattern
#   -h, --help        Show this help message
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
EXPECTED_DIR="$SCRIPT_DIR/expected"
IBDNINJA="$PROJECT_DIR/ibdNinja"

# Test configuration
VERBOSE=0
UPDATE_MODE=0
FILTER=""

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
    echo "  -f, --filter PAT  Only run tests matching pattern"
    echo "  -h, --help        Show this help message"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -u|--update)
            UPDATE_MODE=1
            shift
            ;;
        -f|--filter)
            FILTER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    FAIL=$((FAIL + 1))
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $1"
    SKIP=$((SKIP + 1))
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check if ibdNinja binary exists
if [ ! -x "$IBDNINJA" ]; then
    echo -e "${RED}Error: ibdNinja binary not found at $IBDNINJA${NC}"
    echo "Please run 'make' first to build the project."
    exit 1
fi

# Check if fixtures exist
if [ ! -d "$FIXTURES_DIR" ] || [ -z "$(ls -A "$FIXTURES_DIR"/*.ibd 2>/dev/null)" ]; then
    echo -e "${YELLOW}Warning: No fixtures found in $FIXTURES_DIR${NC}"
    echo "Run './generate_fixtures.sh' first to generate test fixtures."
    exit 1
fi

# Create expected directory if it doesn't exist
mkdir -p "$EXPECTED_DIR"

# Temporary directory for test outputs
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Normalize output by replacing absolute paths with placeholder
# This ensures tests work across different machines/environments
normalize_output() {
    local file="$1"
    sed -i.bak "s|$FIXTURES_DIR|<FIXTURES>|g" "$file"
    rm -f "${file}.bak"
}

#
# Test functions - each function tests one aspect of ibdNinja
#

# Test: --list-tables
test_list_tables() {
    local fixture="$1"
    local name=$(basename "$fixture" .ibd)
    local test_name="${name}_list_tables"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"

    # Apply filter if specified
    if [ -n "$FILTER" ] && [[ ! "$test_name" == *"$FILTER"* ]]; then
        return
    fi

    # Run ibdNinja
    "$IBDNINJA" --file "$fixture" --list-tables > "$output_file" 2>&1 || true
    normalize_output "$output_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$output_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file)"
        return
    fi

    if diff -q "$expected_file" "$output_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            echo "--- Expected ---"
            head -20 "$expected_file"
            echo "--- Actual ---"
            head -20 "$output_file"
            echo "--- Diff ---"
            diff "$expected_file" "$output_file" | head -30 || true
            echo ""
        fi
    fi
}

# Test: --parse-page (page 4 - typically first data page)
test_parse_page() {
    local fixture="$1"
    local page_no="${2:-4}"
    local name=$(basename "$fixture" .ibd)
    local test_name="${name}_parse_page_${page_no}"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"

    if [ -n "$FILTER" ] && [[ ! "$test_name" == *"$FILTER"* ]]; then
        return
    fi

    "$IBDNINJA" --file "$fixture" --parse-page "$page_no" --no-print-record > "$output_file" 2>&1 || true
    normalize_output "$output_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$output_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file)"
        return
    fi

    if diff -q "$expected_file" "$output_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            diff "$expected_file" "$output_file" | head -50 || true
            echo ""
        fi
    fi
}

# Test: --parse-page with --print-record
test_parse_page_with_records() {
    local fixture="$1"
    local page_no="${2:-4}"
    local name=$(basename "$fixture" .ibd)
    local test_name="${name}_parse_page_${page_no}_records"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"

    if [ -n "$FILTER" ] && [[ ! "$test_name" == *"$FILTER"* ]]; then
        return
    fi

    "$IBDNINJA" --file "$fixture" --parse-page "$page_no" > "$output_file" 2>&1 || true
    normalize_output "$output_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$output_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file)"
        return
    fi

    if diff -q "$expected_file" "$output_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            diff "$expected_file" "$output_file" | head -50 || true
            echo ""
        fi
    fi
}

# Test: --list-leftmost-pages (requires index ID, extracted from list-tables output)
test_list_leftmost_pages() {
    local fixture="$1"
    local name=$(basename "$fixture" .ibd)

    # First, get the primary index ID from --list-tables output
    local list_output=$("$IBDNINJA" --file "$fixture" --list-tables 2>&1)

    # Extract first index ID (typically the clustered index)
    # Use POSIX-compatible grep+sed instead of grep -P (not available on macOS)
    local index_id=$(echo "$list_output" | grep -o 'Index\] id: [0-9]*' | head -1 | sed 's/Index\] id: //')

    if [ -z "$index_id" ]; then
        return  # No index found, skip
    fi

    local test_name="${name}_leftmost_pages_${index_id}"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"

    if [ -n "$FILTER" ] && [[ ! "$test_name" == *"$FILTER"* ]]; then
        return
    fi

    "$IBDNINJA" --file "$fixture" --list-leftmost-pages "$index_id" > "$output_file" 2>&1 || true
    normalize_output "$output_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$output_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file)"
        return
    fi

    if diff -q "$expected_file" "$output_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            diff "$expected_file" "$output_file" | head -30 || true
            echo ""
        fi
    fi
}

# Test: --analyze-index
test_parse_index() {
    local fixture="$1"
    local name=$(basename "$fixture" .ibd)

    # Get the primary index ID
    local list_output=$("$IBDNINJA" --file "$fixture" --list-tables 2>&1)
    # Use POSIX-compatible grep+sed instead of grep -P (not available on macOS)
    local index_id=$(echo "$list_output" | grep -o 'Index\] id: [0-9]*' | head -1 | sed 's/Index\] id: //')

    if [ -z "$index_id" ]; then
        return
    fi

    local test_name="${name}_parse_index_${index_id}"
    local expected_file="$EXPECTED_DIR/${test_name}.txt"
    local output_file="$TMPDIR/${test_name}.txt"

    if [ -n "$FILTER" ] && [[ ! "$test_name" == *"$FILTER"* ]]; then
        return
    fi

    "$IBDNINJA" --file "$fixture" --analyze-index "$index_id" > "$output_file" 2>&1 || true
    normalize_output "$output_file"

    if [ $UPDATE_MODE -eq 1 ]; then
        cp "$output_file" "$expected_file"
        log_info "Updated: $test_name"
        return
    fi

    if [ ! -f "$expected_file" ]; then
        log_skip "$test_name (no expected file)"
        return
    fi

    if diff -q "$expected_file" "$output_file" > /dev/null 2>&1; then
        log_pass "$test_name"
    else
        log_fail "$test_name"
        if [ $VERBOSE -eq 1 ]; then
            diff "$expected_file" "$output_file" | head -50 || true
            echo ""
        fi
    fi
}

#
# Main test execution
#

echo ""
echo "=========================================="
echo "  ibdNinja Golden File Tests"
echo "=========================================="
echo ""

if [ $UPDATE_MODE -eq 1 ]; then
    log_info "Running in UPDATE mode - golden files will be updated"
    echo ""
fi

# Run tests for each fixture
for fixture in "$FIXTURES_DIR"/*.ibd; do
    if [ ! -f "$fixture" ]; then
        continue
    fi

    name=$(basename "$fixture" .ibd)
    echo -e "${BLUE}Testing: ${name}${NC}"

    # Run all test types for this fixture
    test_list_tables "$fixture"
    test_parse_page "$fixture" 4
    test_parse_page_with_records "$fixture" 4
    test_list_leftmost_pages "$fixture"
    test_parse_index "$fixture"

    echo ""
done

#
# Summary
#

echo "=========================================="
echo "  Test Summary"
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

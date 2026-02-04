#!/bin/bash
#
# run_tests.sh - Run ibdNinja golden file tests for all MySQL versions
#
# Usage: ./run_tests.sh [options]
#   Options are passed through to version-specific test runners.
#   -v, --verbose     Show diff output on failures
#   -u, --update      Update golden files instead of testing
#   -f, --filter      Only run tests matching pattern
#   -h, --help        Show this help message
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

EXIT_CODE=0

# Run MySQL 8.0 tests
if [ -d "$SCRIPT_DIR/8.0" ]; then
    echo ""
    echo "============================================"
    echo "  Running MySQL 8.0 tests"
    echo "============================================"
    "$SCRIPT_DIR/8.0/run_tests.sh" "$@" || EXIT_CODE=$?
fi

# Run MySQL 8.4 tests (skip gracefully if fixtures don't exist)
if [ -d "$SCRIPT_DIR/8.4" ]; then
    FIXTURES_84="$SCRIPT_DIR/8.4/fixtures"
    if [ -d "$FIXTURES_84" ] && ls "$FIXTURES_84"/*.ibd >/dev/null 2>&1; then
        echo ""
        echo "============================================"
        echo "  Running MySQL 8.4 tests"
        echo "============================================"
        "$SCRIPT_DIR/8.4/run_tests.sh" "$@" || EXIT_CODE=$?
    else
        echo ""
        echo "============================================"
        echo "  Skipping MySQL 8.4 tests (no fixtures)"
        echo "  Run 'make test-fixtures-8.4' to generate"
        echo "============================================"
    fi
fi

# Run MySQL 9.0 tests (skip gracefully if fixtures don't exist)
if [ -d "$SCRIPT_DIR/9.0" ]; then
    FIXTURES_90="$SCRIPT_DIR/9.0/fixtures"
    if [ -d "$FIXTURES_90" ] && ls "$FIXTURES_90"/*.ibd >/dev/null 2>&1; then
        echo ""
        echo "============================================"
        echo "  Running MySQL 9.0 tests"
        echo "============================================"
        "$SCRIPT_DIR/9.0/run_tests.sh" "$@" || EXIT_CODE=$?
    else
        echo ""
        echo "============================================"
        echo "  Skipping MySQL 9.0 tests (no fixtures)"
        echo "  Run 'make test-fixtures-9.0' to generate"
        echo "============================================"
    fi
fi

exit $EXIT_CODE

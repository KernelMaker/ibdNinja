#!/bin/bash
#
# test_corruption.sh - Error-path tests with deliberately corrupted fixtures
#
# ibdNinja's purpose is to read possibly-damaged files, so a corrupted file
# must produce a clean error message -- never a SIGABRT/SIGSEGV crash and
# never an endless loop. Each test copies a known-good 8.0 fixture, corrupts
# specific on-disk bytes, runs ibdNinja under a timeout, and asserts:
#   1. the process was not killed by a signal (exit code < 128)
#   2. the process did not hang (timeout exit code 124)
#   3. where a specific diagnostic is expected, it was printed
#
# Usage: ./test_corruption.sh [-v|--verbose]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
IBDNINJA="$PROJECT_DIR/ibdNinja"
FIXTURES_DIR="$PROJECT_DIR/tests/8.0/fixtures"
BASE_FIXTURE="$FIXTURES_DIR/simple_table.ibd"

PAGE_SIZE=16384
RUN_TIMEOUT=30

VERBOSE=0
[ "$1" = "-v" ] || [ "$1" = "--verbose" ] && VERBOSE=1

PASS=0
FAIL=0

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; PASS=$((PASS + 1)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); }

if [ ! -x "$IBDNINJA" ]; then
    echo -e "${RED}Error: ibdNinja binary not found at $IBDNINJA${NC}"
    exit 1
fi

if [ ! -f "$BASE_FIXTURE" ]; then
    echo -e "${YELLOW}Skipping corruption tests: simple_table.ibd fixture not found${NC}"
    exit 0
fi

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# patch_bytes <file> <offset> <hex-bytes like "ff00ab">
patch_bytes() {
    python3 - "$1" "$2" "$3" <<'PYEOF'
import sys
path, offset, hexbytes = sys.argv[1], int(sys.argv[2]), bytes.fromhex(sys.argv[3])
with open(path, "r+b") as f:
    f.seek(offset)
    f.write(hexbytes)
PYEOF
}

# run_case <test_name> <expected_pattern|-> <ibdNinja args...>
# expected_pattern: an (extended) regex that must appear in combined output;
#                   "-" means only check for no-crash/no-hang.
run_case() {
    local test_name="$1"
    local pattern="$2"
    shift 2
    local out_file="$TMPDIR/${test_name}.out"

    timeout "$RUN_TIMEOUT" "$IBDNINJA" "$@" > "$out_file" 2>&1 < /dev/null
    local rc=$?

    if [ $rc -eq 124 ]; then
        log_fail "$test_name (hung: killed by ${RUN_TIMEOUT}s timeout)"
        return
    fi
    if [ $rc -ge 128 ]; then
        log_fail "$test_name (crashed: killed by signal $((rc - 128)))"
        [ $VERBOSE -eq 1 ] && tail -20 "$out_file"
        return
    fi
    if [ "$pattern" != "-" ] && ! grep -qE "$pattern" "$out_file"; then
        log_fail "$test_name (expected diagnostic /$pattern/ not found)"
        [ $VERBOSE -eq 1 ] && tail -20 "$out_file"
        return
    fi
    log_pass "$test_name"
}

echo ""
echo "=========================================="
echo "  Corruption / Error-Path Tests"
echo "=========================================="
echo ""
echo -e "${BLUE}Base fixture: simple_table.ibd (MySQL 8.0)${NC}"

# --- Test 1: truncated file (only 3 pages, SDI root unreadable) ------------
F="$TMPDIR/truncated.ibd"
head -c $((3 * PAGE_SIZE)) "$BASE_FIXTURE" > "$F"
run_case "truncated_file" "ERROR" --file "$F" --list-tables

# --- Test 2: bad page number ------------------------------------------------
run_case "bad_page_number" "too large" --file "$BASE_FIXTURE" --parse-page 9999

# --- Test 3: FIL_PAGE_NEXT self-cycle on the leaf page ----------------------
# Point page 4's next-page pointer (FIL_PAGE_NEXT at page offset 12) back at
# page 4; --analyze-index must detect the cycle instead of looping forever.
F="$TMPDIR/page_cycle.ibd"
cp "$BASE_FIXTURE" "$F"
patch_bytes "$F" $((4 * PAGE_SIZE + 12)) "00000004"
INDEX_ID=$("$IBDNINJA" --file "$BASE_FIXTURE" --list-tables 2>/dev/null | \
    grep -o 'Index\] id: [0-9]*' | head -1 | sed 's/Index\] id: //')
if [ -n "$INDEX_ID" ]; then
    run_case "page_next_cycle" "cycle" --file "$F" --analyze-index "$INDEX_ID"
else
    log_fail "page_next_cycle (could not determine index id)"
fi

# --- Test 4: infimum next-record offset out of bounds -----------------------
# The 2-byte next-record pointer of the infimum lives at PAGE_NEW_INFIMUM-2
# = 97 within the page. 0xff00 sends the first user record far past the page.
F="$TMPDIR/bad_infimum_next.ibd"
cp "$BASE_FIXTURE" "$F"
patch_bytes "$F" $((4 * PAGE_SIZE + 97)) "ff00"
run_case "infimum_next_out_of_bounds" "ERROR" --file "$F" --parse-page 4

# --- Test 5: garbage in the record area -------------------------------------
# Stomp 64 bytes of record data on page 4 (past infimum/supremum, before the
# page directory). Parsing must report/clamp, not crash or loop.
F="$TMPDIR/garbage_records.ibd"
cp "$BASE_FIXTURE" "$F"
patch_bytes "$F" $((4 * PAGE_SIZE + 150)) "$(printf 'ff%.0s' $(seq 1 64))"
run_case "garbage_record_area" "-" --file "$F" --parse-page 4

# --- Test 6: corrupted SDI page ----------------------------------------------
# Stomp 16 bytes inside the SDI record area on page 3. Loading must either
# skip the table or fail with an error -- never abort.
F="$TMPDIR/corrupt_sdi.ibd"
cp "$BASE_FIXTURE" "$F"
patch_bytes "$F" $((3 * PAGE_SIZE + 300)) "ffffffffffffffffffffffffffffffff"
run_case "corrupt_sdi_page" "-" --file "$F" --list-tables

echo ""
echo "=========================================="
echo "  Corruption Test Summary"
echo "=========================================="
echo ""
echo -e "  ${GREEN}Passed:${NC}  $PASS"
echo -e "  ${RED}Failed:${NC}  $FAIL"
echo ""

if [ $FAIL -gt 0 ]; then
    echo -e "${RED}Some corruption tests failed!${NC}"
    exit 1
fi
echo -e "${GREEN}All corruption tests passed!${NC}"
exit 0

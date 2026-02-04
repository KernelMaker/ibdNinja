#!/bin/bash
#
# generate_json_partial_fixture.sh - Generate json_partial.ibd with LOB version chains
#
# This script generates the json_partial fixture with intact LOB version chain
# entries. It uses a background transaction to hold a read view, preventing
# InnoDB's purge thread from cleaning up old LOB version entries.
#
# The approach:
# 1. Start MySQL, run the INSERT (from 13_json_partial_update.sql)
# 2. Open a background session with START TRANSACTION WITH CONSISTENT SNAPSHOT
#    (this creates a read view that prevents purge from removing old versions)
# 3. Run the partial UPDATEs in another session
# 4. FLUSH TABLES ... FOR EXPORT to flush dirty pages to disk
# 5. Copy the .ibd file (version chains are intact because purge is blocked)
# 6. Clean up
#
# Usage: ./generate_json_partial_fixture.sh [mysql_version]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-8.0.40}"
CONTAINER_NAME="ibdninja_json_partial_$$"
MYSQL_ROOT_PASSWORD="test_password"
MYSQL_DATABASE="ibdninja_test"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    # Kill background hold session if still running
    kill "$HOLD_PID" 2>/dev/null || true
    wait "$HOLD_PID" 2>/dev/null || true
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

if ! command -v docker &> /dev/null; then
    log_error "Docker is required but not installed."
    exit 1
fi

mkdir -p "$FIXTURES_DIR"

# Start MySQL container
log_info "Starting MySQL $MYSQL_VERSION container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
    -e MYSQL_DATABASE="$MYSQL_DATABASE" \
    mysql:"$MYSQL_VERSION" \
    --innodb-file-per-table=1 \
    --innodb-flush-log-at-trx-commit=1 \
    --innodb-flush-method=O_DIRECT

log_info "Waiting for MySQL to be ready..."
for i in {1..60}; do
    if docker exec "$CONTAINER_NAME" mysqladmin ping -h localhost \
        -u root -p"$MYSQL_ROOT_PASSWORD" --silent 2>/dev/null; then
        log_info "MySQL is ready!"
        break
    fi
    if [ $i -eq 60 ]; then
        log_error "MySQL failed to start within 60 seconds"
        exit 1
    fi
    sleep 1
done
sleep 3

# Helper to run SQL
run_sql() {
    docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
        "$MYSQL_DATABASE" "$@"
}

# Step 1: Run the INSERT (creates table and inserts initial row)
log_info "Running 13_json_partial_update.sql (CREATE TABLE + INSERT)..."
run_sql < "$SQL_DIR/13_json_partial_update.sql"

# Step 2: Start a background session that holds a read view.
# This prevents the purge thread from removing old LOB version entries
# created by the subsequent UPDATEs.
log_info "Starting background session to hold read view (prevents purge)..."
docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    "$MYSQL_DATABASE" <<'EOF' &
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT 'Read view established' AS status;
SELECT SLEEP(300);
EOF
HOLD_PID=$!
sleep 2

# Step 3: Run the partial UPDATEs
log_info "Running partial UPDATEs to create LOB version chains..."
run_sql <<'EOF'
-- Partial update 1: change doc1.payload (10000 bytes > 100 byte threshold)
-- Creates version chain: v1 -> v2
UPDATE json_partial SET doc1 = JSON_SET(doc1, '$.payload', REPEAT('X', 10000)) WHERE id = 1;

-- Partial update 2: change doc1.payload again
-- Creates version chain: v2 -> v3
UPDATE json_partial SET doc1 = JSON_SET(doc1, '$.payload', REPEAT('M', 10000)) WHERE id = 1;

-- Partial update 3: change doc2.large_text
-- Creates version chain for doc2: v1 -> v2
UPDATE json_partial SET doc2 = JSON_SET(doc2, '$.large_text', REPEAT('Y', 10000)) WHERE id = 1;

SELECT 'Updates complete' AS status;
EOF

# Step 4: Flush the table to disk
log_info "Flushing table to disk (FLUSH TABLES ... FOR EXPORT)..."
run_sql -e "FLUSH TABLES json_partial FOR EXPORT;"

# Step 5: Get datadir and copy the .ibd file
DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    -N -e "SELECT @@datadir;" | tr -d '[:space:]')

log_info "Copying json_partial.ibd..."
docker cp "$CONTAINER_NAME:${DATADIR}${MYSQL_DATABASE}/json_partial.ibd" \
    "$FIXTURES_DIR/json_partial.ibd"

# Step 6: Unlock and clean up
log_info "Unlocking tables..."
run_sql -e "UNLOCK TABLES;"

# Kill the background hold session
kill "$HOLD_PID" 2>/dev/null || true
wait "$HOLD_PID" 2>/dev/null || true

# Verify the fixture
if [ -f "$FIXTURES_DIR/json_partial.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/json_partial.ibd" 2>/dev/null || \
           stat -f%z "$FIXTURES_DIR/json_partial.ibd" 2>/dev/null)
    log_info "Generated json_partial.ibd: ${size} bytes"
else
    log_error "Failed to generate json_partial.ibd"
    exit 1
fi

log_info "Fixture generation complete!"
log_info "Run 'make test-inspect-blob-update' to generate golden files."

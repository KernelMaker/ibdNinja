#!/bin/bash
#
# generate_json_partial_large_fixture.sh - Generate json_partial_large.ibd with multi-entry LOB version chains
#
# This script generates a fixture with a large JSON document (~48KB+) that spans
# 3-4 LOB index entries. Partial updates target different chunks to create version
# chains on different LOB index entries.
#
# The approach:
# 1. Start MySQL, run the INSERT (from 14_json_partial_large.sql)
# 2. Open a background session with START TRANSACTION WITH CONSISTENT SNAPSHOT
#    (this creates a read view that prevents purge from removing old versions)
# 3. Run 3 partial UPDATEs targeting different chunks
# 4. FLUSH TABLES ... FOR EXPORT to flush dirty pages to disk
# 5. Copy the .ibd file (version chains are intact because purge is blocked)
# 6. Clean up
#
# Usage: ./generate_json_partial_large_fixture.sh [mysql_version]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-8.0.40}"
CONTAINER_NAME="ibdninja_json_partial_large_$$"
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
log_info "Running 14_json_partial_large.sql (CREATE TABLE + INSERT)..."
run_sql < "$SQL_DIR/14_json_partial_large.sql"

# Step 2: Start a background session that holds a read view.
log_info "Starting background session to hold read view (prevents purge)..."
docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    "$MYSQL_DATABASE" <<'EOF' &
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT 'Read view established' AS status;
SELECT SLEEP(300);
EOF
HOLD_PID=$!
sleep 2

# Step 3: Run the partial UPDATEs targeting different chunks
log_info "Running partial UPDATEs to create LOB version chains on different entries..."
run_sql <<'EOF'
-- Partial update 1: change chunk_a (12000 bytes > 100 byte threshold)
-- Creates version chain on LOB index entry #0: v1 -> v2
UPDATE json_partial_large SET doc = JSON_SET(doc, '$.chunk_a', REPEAT('X', 12000)) WHERE id = 1;

-- Partial update 2: change chunk_c (different LOB index entry)
-- Creates version chain on LOB index entry #1 or #2: v2 -> v3
UPDATE json_partial_large SET doc = JSON_SET(doc, '$.chunk_c', REPEAT('Z', 12000)) WHERE id = 1;

-- Partial update 3: change chunk_a again (same entry as update 1)
-- Extends version chain on LOB index entry #0: v3 -> v4
UPDATE json_partial_large SET doc = JSON_SET(doc, '$.chunk_a', REPEAT('W', 12000)) WHERE id = 1;

SELECT 'Updates complete' AS status;
EOF

# Step 4: Flush the table to disk
log_info "Flushing table to disk (FLUSH TABLES ... FOR EXPORT)..."
run_sql -e "FLUSH TABLES json_partial_large FOR EXPORT;"

# Step 5: Get datadir and copy the .ibd file
DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    -N -e "SELECT @@datadir;" | tr -d '[:space:]')

log_info "Copying json_partial_large.ibd..."
docker cp "$CONTAINER_NAME:${DATADIR}${MYSQL_DATABASE}/json_partial_large.ibd" \
    "$FIXTURES_DIR/json_partial_large.ibd"

# Step 6: Unlock and clean up
log_info "Unlocking tables..."
run_sql -e "UNLOCK TABLES;"

kill "$HOLD_PID" 2>/dev/null || true
wait "$HOLD_PID" 2>/dev/null || true

# Verify the fixture
if [ -f "$FIXTURES_DIR/json_partial_large.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/json_partial_large.ibd" 2>/dev/null || \
           stat -f%z "$FIXTURES_DIR/json_partial_large.ibd" 2>/dev/null)
    log_info "Generated json_partial_large.ibd: ${size} bytes"
else
    log_error "Failed to generate json_partial_large.ibd"
    exit 1
fi

log_info "Fixture generation complete!"
log_info "Run 'make test-inspect-blob-update' to generate golden files."

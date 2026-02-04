#!/bin/bash
#
# generate_json_partial_purged_fixture.sh - Generate json_partial_purged.ibd with purged version chains
#
# This script generates a fixture where old LOB version chain entries have been
# purged by InnoDB. Unlike other fixture scripts, NO background transaction is
# held, allowing the purge thread to clean up old versions.
#
# The approach:
# 1. Start MySQL, run the INSERT (from 15_json_partial_purged.sql)
# 2. Run 3 partial UPDATEs (no read view held - purge is allowed)
# 3. Force purge to run and wait for it to complete
# 4. FLUSH TABLES ... FOR EXPORT to flush dirty pages to disk
# 5. Copy the .ibd file (old versions have been purged)
# 6. Clean up
#
# Expected result: Only the current version (v4) is visible. Versions 1-3
# have been purged. The LOB free list contains recycled entries.
#
# Usage: ./generate_json_partial_purged_fixture.sh [mysql_version]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-9.6.0}"
CONTAINER_NAME="ibdninja_json_partial_purged_$$"
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
log_info "Running 15_json_partial_purged.sql (CREATE TABLE + INSERT)..."
run_sql < "$SQL_DIR/15_json_partial_purged.sql"

# Step 2: Run the partial UPDATEs (NO background transaction - purge is allowed)
log_info "Running partial UPDATEs (purge NOT blocked)..."
run_sql <<'EOF'
-- Partial update 1: change payload (10000 bytes > 100 byte threshold)
-- Creates version chain: v1 -> v2
UPDATE json_partial_purged SET doc = JSON_SET(doc, '$.payload', REPEAT('X', 10000)) WHERE id = 1;

-- Partial update 2: change payload again
-- Creates version chain: v2 -> v3
UPDATE json_partial_purged SET doc = JSON_SET(doc, '$.payload', REPEAT('M', 10000)) WHERE id = 1;

-- Partial update 3: change payload again
-- Creates version chain: v3 -> v4
UPDATE json_partial_purged SET doc = JSON_SET(doc, '$.payload', REPEAT('Z', 10000)) WHERE id = 1;

SELECT 'Updates complete' AS status;
EOF

# Step 3: Wait for purge to complete.
# Since no background transaction holds a read view, purge runs automatically.
# We maximize purge frequency and sleep inside a SQL session to give it time.
log_info "Waiting for InnoDB purge to complete..."
run_sql <<'EOF'
SET GLOBAL innodb_max_purge_lag = 0;
SET GLOBAL innodb_purge_rseg_truncate_frequency = 1;
SELECT SLEEP(15);
SELECT 'Purge wait complete' AS status;
EOF

# Step 4: Flush the table to disk
log_info "Flushing table to disk (FLUSH TABLES ... FOR EXPORT)..."
run_sql -e "FLUSH TABLES json_partial_purged FOR EXPORT;"

# Step 5: Get datadir and copy the .ibd file
DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    -N -e "SELECT @@datadir;" | tr -d '[:space:]')

log_info "Copying json_partial_purged.ibd..."
docker cp "$CONTAINER_NAME:${DATADIR}${MYSQL_DATABASE}/json_partial_purged.ibd" \
    "$FIXTURES_DIR/json_partial_purged.ibd"

# Step 6: Unlock
log_info "Unlocking tables..."
run_sql -e "UNLOCK TABLES;"

# Verify the fixture
if [ -f "$FIXTURES_DIR/json_partial_purged.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/json_partial_purged.ibd" 2>/dev/null || \
           stat -f%z "$FIXTURES_DIR/json_partial_purged.ibd" 2>/dev/null)
    log_info "Generated json_partial_purged.ibd: ${size} bytes"
else
    log_error "Failed to generate json_partial_purged.ibd"
    exit 1
fi

log_info "Fixture generation complete!"
log_info "Run 'make test-inspect-blob-update' to generate golden files."

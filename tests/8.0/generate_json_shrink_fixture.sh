#!/bin/bash
#
# generate_json_shrink_fixture.sh - Generate json_shrink.ibd with a LOB
# version chain where the OLD version is LARGER than the current one.
#
# This exercises the FetchLobByVersion buffer-sizing path: after a shrinking
# JSON_SET() partial update, fetching version 1 must allocate for v1's
# length (~19KB), not the current external length (~9KB).
#
# Same approach as generate_json_partial_fixture.sh:
# 1. Start MySQL, run the INSERT (from 17_json_shrink.sql)
# 2. Open a background session with START TRANSACTION WITH CONSISTENT
#    SNAPSHOT to keep purge from removing old LOB versions
# 3. Run the shrinking partial UPDATE in another session
# 4. FLUSH TABLES ... FOR EXPORT and copy the .ibd file
#
# Usage: ./generate_json_shrink_fixture.sh [mysql_version]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-8.0.40}"
CONTAINER_NAME="ibdninja_json_shrink_$$"
MYSQL_ROOT_PASSWORD="test_password"
MYSQL_DATABASE="ibdninja_test"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

run_sql() {
    docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
        "$MYSQL_DATABASE" "$@"
}

log_info "Running 17_json_shrink.sql (CREATE TABLE + INSERT)..."
run_sql < "$SQL_DIR/17_json_shrink.sql"

log_info "Starting background session to hold read view (prevents purge)..."
docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    "$MYSQL_DATABASE" <<'EOF' &
START TRANSACTION WITH CONSISTENT SNAPSHOT;
SELECT 'Read view established' AS status;
SELECT SLEEP(300);
EOF
HOLD_PID=$!
sleep 2

log_info "Running shrinking partial UPDATE (10000 -> 200 bytes)..."
run_sql <<'EOF'
-- Shrink payload from 10000 to 200 bytes. Total modified bytes exceed the
-- 100-byte LOB_SMALL_CHANGE_THRESHOLD, so InnoDB creates new LOB index
-- entries with a version chain; the v1 entries keep the larger data.
UPDATE json_shrink SET doc = JSON_SET(doc, '$.payload', REPEAT('X', 200)) WHERE id = 1;

SELECT 'Update complete' AS status;
EOF

log_info "Flushing table to disk (FLUSH TABLES ... FOR EXPORT)..."
run_sql -e "FLUSH TABLES json_shrink FOR EXPORT;"

DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    -N -e "SELECT @@datadir;" | tr -d '[:space:]')

log_info "Copying json_shrink.ibd..."
docker cp "$CONTAINER_NAME:${DATADIR}${MYSQL_DATABASE}/json_shrink.ibd" \
    "$FIXTURES_DIR/json_shrink.ibd"

log_info "Unlocking tables..."
run_sql -e "UNLOCK TABLES;"

kill "$HOLD_PID" 2>/dev/null || true
wait "$HOLD_PID" 2>/dev/null || true

if [ -f "$FIXTURES_DIR/json_shrink.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/json_shrink.ibd" 2>/dev/null || \
           stat -f%z "$FIXTURES_DIR/json_shrink.ibd" 2>/dev/null)
    log_info "Generated json_shrink.ibd: ${size} bytes"
else
    log_error "Failed to generate json_shrink.ibd"
    exit 1
fi

log_info "Fixture generation complete!"
log_info "Run './test_inspect_blob.sh --update' to generate golden files."

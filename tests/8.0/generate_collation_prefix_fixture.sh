#!/bin/bash
#
# generate_collation_prefix_fixture.sh - Generate collation_prefix.ibd
#
# A targeted generator for the collation_prefix fixture only (16_collation_prefix.sql),
# so the other fixtures (and their golden files) are not regenerated the way
# generate_fixtures.sh would.
#
# The fixture covers:
#   - CHAR(n) COLLATE utf8mb4_0900_bin parsed as variable-length
#   - secondary index on a CHAR-column prefix (KEY (c_lat(10)))
#
# Usage: ./generate_collation_prefix_fixture.sh [mysql_version]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-8.0.40}"
CONTAINER_NAME="ibdninja_collation_prefix_$$"
MYSQL_ROOT_PASSWORD="test_password"
MYSQL_DATABASE="ibdninja_test"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
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

log_info "Running 16_collation_prefix.sql..."
run_sql < "$SQL_DIR/16_collation_prefix.sql"

log_info "Flushing table to disk (FLUSH TABLES ... FOR EXPORT)..."
run_sql -e "FLUSH TABLES collation_prefix FOR EXPORT;"

DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" \
    -N -e "SELECT @@datadir;" | tr -d '[:space:]')

log_info "Copying collation_prefix.ibd..."
docker cp "$CONTAINER_NAME:${DATADIR}${MYSQL_DATABASE}/collation_prefix.ibd" \
    "$FIXTURES_DIR/collation_prefix.ibd"

run_sql -e "UNLOCK TABLES;"

if [ -f "$FIXTURES_DIR/collation_prefix.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/collation_prefix.ibd" 2>/dev/null || \
           stat -f%z "$FIXTURES_DIR/collation_prefix.ibd" 2>/dev/null)
    log_info "Generated collation_prefix.ibd: ${size} bytes"
else
    log_error "Failed to generate collation_prefix.ibd"
    exit 1
fi

log_info "Fixture generation complete!"
log_info "Run './run_tests.sh --update' to generate golden files."

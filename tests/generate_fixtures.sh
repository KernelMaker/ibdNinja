#!/bin/bash
#
# generate_fixtures.sh - Generate test .ibd fixtures using Docker MySQL
#
# Usage: ./generate_fixtures.sh [mysql_version]
#   mysql_version: MySQL version tag (default: 8.0.40)
#
# This script:
# 1. Starts a MySQL container
# 2. Runs all SQL scripts in tests/sql/
# 3. Copies the generated .ibd files to tests/fixtures/
# 4. Cleans up the container
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_VERSION="${1:-8.0.40}"
CONTAINER_NAME="ibdninja_test_mysql_$$"
MYSQL_ROOT_PASSWORD="test_password"
MYSQL_DATABASE="ibdninja_test"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

trap cleanup EXIT

# Check prerequisites
if ! command -v docker &> /dev/null; then
    log_error "Docker is required but not installed."
    echo "Install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Create fixtures directory if it doesn't exist
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
    if docker exec "$CONTAINER_NAME" mysqladmin ping -h localhost -u root -p"$MYSQL_ROOT_PASSWORD" --silent 2>/dev/null; then
        log_info "MySQL is ready!"
        break
    fi
    if [ $i -eq 60 ]; then
        log_error "MySQL failed to start within 60 seconds"
        exit 1
    fi
    sleep 1
done

# Wait a bit more for full initialization
sleep 3

# Run all SQL scripts (skip upgrade-specific scripts that need special handling)
log_info "Running SQL scripts..."
for sql_file in "$SQL_DIR"/*.sql; do
    if [ -f "$sql_file" ]; then
        filename=$(basename "$sql_file")
        # Skip upgrade scenario scripts (they require special two-container process)
        if [[ "$filename" == 10_upgrade_*.sql ]] || [[ "$filename" == 11_upgrade_*.sql ]]; then
            log_info "  Skipping $filename (use generate_upgrade_fixture.sh)"
            continue
        fi
        log_info "  Running $filename..."
        docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$sql_file"
    fi
done

# Get datadir and table list before shutdown
log_info "Getting MySQL datadir and table list..."
DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT @@datadir;")
DATADIR=$(echo "$DATADIR" | tr -d '[:space:]')
tables=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -N -e "SHOW TABLES;")

# Gracefully shutdown MySQL to flush all dirty pages (including SDI) to disk
log_info "Shutting down MySQL gracefully to flush all pages to disk..."
docker exec "$CONTAINER_NAME" mysqladmin -u root -p"$MYSQL_ROOT_PASSWORD" shutdown 2>/dev/null || true

# Wait for MySQL to fully stop
log_info "Waiting for MySQL to stop..."
for i in {1..30}; do
    if ! docker exec "$CONTAINER_NAME" pgrep -x mysqld > /dev/null 2>&1; then
        log_info "MySQL has stopped."
        break
    fi
    if [ $i -eq 30 ]; then
        log_warn "MySQL did not stop within 30 seconds, proceeding anyway..."
    fi
    sleep 1
done

# Copy .ibd files from stopped container
log_info "Copying .ibd files to fixtures..."

for table in $tables; do
    ibd_file="${DATADIR}${MYSQL_DATABASE}/${table}.ibd"
    log_info "  Copying $table.ibd..."
    docker cp "$CONTAINER_NAME:$ibd_file" "$FIXTURES_DIR/${table}.ibd" 2>/dev/null || {
        log_warn "  Could not copy $table.ibd (might be a system table)"
    }
done

# Verify copied files
log_info "Verifying fixtures..."
for ibd_file in "$FIXTURES_DIR"/*.ibd; do
    if [ -f "$ibd_file" ]; then
        size=$(stat -f%z "$ibd_file" 2>/dev/null || stat -c%s "$ibd_file" 2>/dev/null)
        filename=$(basename "$ibd_file")
        log_info "  $filename: ${size} bytes"
    fi
done

log_info "Fixture generation complete!"
log_info "Generated fixtures in: $FIXTURES_DIR"

# List all fixtures
echo ""
echo "Generated fixtures:"
ls -la "$FIXTURES_DIR"/*.ibd 2>/dev/null || echo "  (none)"

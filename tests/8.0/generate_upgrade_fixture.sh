#!/bin/bash
#
# generate_upgrade_fixture.sh - Generate test fixture for MySQL version upgrade scenario
#
# This script simulates a MySQL upgrade from an older version to 8.0.40:
# 1. Starts MySQL (older version) container
# 2. Runs 10_upgrade_8016.sql (creates table, adds columns, inserts data)
# 3. Stops MySQL gracefully
# 4. Starts MySQL 8.0.40 with same data volume (triggers upgrade)
# 5. Runs 11_upgrade_8040.sql (more add/drop columns)
# 6. Stops MySQL gracefully and copies .ibd file
#
# Note: On ARM64 systems, MySQL 8.0.16 isn't available, so we use 8.0.28 with
#       --platform=linux/amd64 emulation to run the true 8.0.16 for accurate testing.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
SQL_DIR="$SCRIPT_DIR/sql"

MYSQL_OLD_VERSION="8.0.16"
MYSQL_NEW_VERSION="8.0.40"

# Detect architecture and adjust versions if needed
ARCH=$(uname -m)
PLATFORM_FLAG=""
if [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
    # Check if QEMU emulation is available
    if docker run --rm --platform=linux/amd64 alpine:latest echo "qemu works" 2>/dev/null | grep -q "qemu works"; then
        echo "Detected ARM64 architecture - using x86_64 emulation for MySQL $MYSQL_OLD_VERSION"
        PLATFORM_FLAG="--platform=linux/amd64"
    else
        echo "Detected ARM64 architecture without x86_64 emulation"
        echo "Falling back to ARM64-native MySQL versions: 8.0.30 -> 8.0.40"
        echo "(To use 8.0.16, install QEMU: sudo apt-get install qemu-user-static binfmt-support)"
        echo ""
        MYSQL_OLD_VERSION="8.0.30"
        # 8.0.30 is the earliest ARM64-native version, still tests upgrade scenario
    fi
fi
CONTAINER_NAME="ibdninja_upgrade_mysql_$$"
VOLUME_NAME="ibdninja_upgrade_data_$$"
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
    docker volume rm "$VOLUME_NAME" 2>/dev/null || true
}

trap cleanup EXIT

# Check prerequisites
if ! command -v docker &> /dev/null; then
    log_error "Docker is required but not installed."
    exit 1
fi

# Check if upgrade SQL files exist
if [ ! -f "$SQL_DIR/10_upgrade_8016.sql" ] || [ ! -f "$SQL_DIR/11_upgrade_8040.sql" ]; then
    log_error "Upgrade SQL files not found in $SQL_DIR"
    exit 1
fi

# Create fixtures directory
mkdir -p "$FIXTURES_DIR"

# Create Docker volume for persistent data
log_info "Creating Docker volume for MySQL data..."
docker volume create "$VOLUME_NAME"

#
# Phase 1: MySQL 8.0.16
#
log_info "=========================================="
log_info "Phase 1: Starting MySQL $MYSQL_OLD_VERSION"
log_info "=========================================="

docker run -d \
    $PLATFORM_FLAG \
    --name "$CONTAINER_NAME" \
    -v "$VOLUME_NAME:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
    -e MYSQL_DATABASE="$MYSQL_DATABASE" \
    mysql:"$MYSQL_OLD_VERSION" \
    --innodb-file-per-table=1 \
    --innodb-flush-log-at-trx-commit=1

log_info "Waiting for MySQL $MYSQL_OLD_VERSION to be ready..."
for i in {1..90}; do
    if docker exec "$CONTAINER_NAME" mysqladmin ping -h localhost -u root -p"$MYSQL_ROOT_PASSWORD" --silent 2>/dev/null; then
        log_info "MySQL $MYSQL_OLD_VERSION is ready!"
        break
    fi
    if [ $i -eq 90 ]; then
        log_error "MySQL $MYSQL_OLD_VERSION failed to start within 90 seconds"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
    sleep 1
done

# Wait for full initialization
sleep 5

log_info "Running 10_upgrade_8016.sql on MySQL $MYSQL_OLD_VERSION..."
docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$SQL_DIR/10_upgrade_8016.sql"

log_info "Verifying data in MySQL $MYSQL_OLD_VERSION..."
docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) as row_count FROM ddl_test;" 2>/dev/null

# Gracefully shutdown MySQL 8.0.16
log_info "Shutting down MySQL $MYSQL_OLD_VERSION gracefully..."
docker exec "$CONTAINER_NAME" mysqladmin -u root -p"$MYSQL_ROOT_PASSWORD" shutdown 2>/dev/null || true

# Wait for MySQL to fully stop
for i in {1..30}; do
    if ! docker exec "$CONTAINER_NAME" pgrep -x mysqld > /dev/null 2>&1; then
        log_info "MySQL $MYSQL_OLD_VERSION has stopped."
        break
    fi
    sleep 1
done

# Remove container but keep volume
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

#
# Phase 2: MySQL 8.0.40 (Upgrade)
#
log_info "=========================================="
log_info "Phase 2: Starting MySQL $MYSQL_NEW_VERSION (Upgrade)"
log_info "=========================================="

docker run -d \
    $PLATFORM_FLAG \
    --name "$CONTAINER_NAME" \
    -v "$VOLUME_NAME:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
    mysql:"$MYSQL_NEW_VERSION" \
    --innodb-file-per-table=1 \
    --innodb-flush-log-at-trx-commit=1

log_info "Waiting for MySQL $MYSQL_NEW_VERSION to be ready (upgrade may take time)..."
for i in {1..180}; do
    if docker exec "$CONTAINER_NAME" mysqladmin ping -h localhost -u root -p"$MYSQL_ROOT_PASSWORD" --silent 2>/dev/null; then
        log_info "MySQL $MYSQL_NEW_VERSION is ready!"
        break
    fi
    if [ $i -eq 180 ]; then
        log_error "MySQL $MYSQL_NEW_VERSION failed to start within 180 seconds"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
    sleep 1
done

# Wait for upgrade to complete
sleep 5

log_info "Verifying upgrade - checking data from 8.0.16..."
docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) as row_count FROM ddl_test;" 2>/dev/null

log_info "Running 11_upgrade_8040.sql on MySQL $MYSQL_NEW_VERSION..."
docker exec -i "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" < "$SQL_DIR/11_upgrade_8040.sql"

log_info "Final row count after all operations..."
docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE" -e "SELECT COUNT(*) as row_count FROM ddl_test;" 2>/dev/null

# Get datadir before shutdown
DATADIR=$(docker exec "$CONTAINER_NAME" mysql -u root -p"$MYSQL_ROOT_PASSWORD" -N -e "SELECT @@datadir;" 2>/dev/null | tr -d '[:space:]')

# Gracefully shutdown MySQL 8.0.40
log_info "Shutting down MySQL $MYSQL_NEW_VERSION gracefully to flush all pages..."
docker exec "$CONTAINER_NAME" mysqladmin -u root -p"$MYSQL_ROOT_PASSWORD" shutdown 2>/dev/null || true

# Wait for MySQL to fully stop
for i in {1..30}; do
    if ! docker exec "$CONTAINER_NAME" pgrep -x mysqld > /dev/null 2>&1; then
        log_info "MySQL $MYSQL_NEW_VERSION has stopped."
        break
    fi
    sleep 1
done

#
# Phase 3: Copy .ibd file
#
log_info "=========================================="
log_info "Phase 3: Copying .ibd file"
log_info "=========================================="

ibd_file="${DATADIR}${MYSQL_DATABASE}/ddl_test.ibd"
log_info "Copying ddl_test.ibd (upgrade scenario)..."
docker cp "$CONTAINER_NAME:$ibd_file" "$FIXTURES_DIR/ddl_test.ibd" 2>/dev/null || {
    log_error "Failed to copy ddl_test.ibd"
    exit 1
}

# Verify copied file
if [ -f "$FIXTURES_DIR/ddl_test.ibd" ]; then
    size=$(stat -c%s "$FIXTURES_DIR/ddl_test.ibd" 2>/dev/null || stat -f%z "$FIXTURES_DIR/ddl_test.ibd" 2>/dev/null)
    log_info "Successfully copied ddl_test.ibd: ${size} bytes"
else
    log_error "ddl_test.ibd was not copied!"
    exit 1
fi

log_info "=========================================="
log_info "Upgrade fixture generation complete!"
log_info "=========================================="
log_info "Generated: $FIXTURES_DIR/ddl_test.ibd"
log_info "This fixture contains records from MySQL 8.0.16 upgraded to 8.0.40"
log_info "with multiple instant add/drop column operations across versions."

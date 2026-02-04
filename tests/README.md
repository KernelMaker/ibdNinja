# ibdNinja Test Suite

Golden file tests for ibdNinja. Requires Docker for fixture generation.

## Quick Start

```bash
make test-all-fixtures   # Generate all .ibd fixtures via Docker (8.0)
make test-update         # Generate golden files
make test                # Run tests
```

## Directory Structure

```
tests/
├── 8.0/                             # MySQL 8.0 tests
│   ├── fixtures/                    # .ibd files (generated, MySQL 8.0.40)
│   ├── expected/                    # Golden files (expected outputs)
│   ├── sql/                         # SQL scripts to create test tables
│   ├── run_tests.sh                 # Test runner for 8.0
│   ├── generate_fixtures.sh         # Generate fixtures via Docker MySQL 8.0
│   └── generate_upgrade_fixture.sh  # Generate upgrade fixture (8.0.16 → 8.0.40)
├── 8.4/                             # MySQL 8.4 tests
│   ├── fixtures/                    # .ibd files (generated, MySQL 8.4.8)
│   ├── expected/                    # Golden files (expected outputs)
│   ├── sql/                         # SQL scripts (same as 8.0)
│   ├── run_tests.sh                 # Test runner for 8.4
│   ├── generate_fixtures.sh         # Generate fixtures via Docker MySQL 8.4
│   └── generate_upgrade_fixture.sh  # Generate upgrade fixture (8.0.40 → 8.4.x)
├── 9.0/                             # MySQL 9.0 tests
│   ├── fixtures/                    # .ibd files (generated, MySQL 9.6.0)
│   ├── expected/                    # Golden files (expected outputs)
│   ├── sql/                         # SQL scripts (same as 8.0 + VECTOR test)
│   ├── run_tests.sh                 # Test runner for 9.0
│   └── generate_fixtures.sh         # Generate fixtures via Docker MySQL 9.x
├── run_tests.sh                     # Top-level wrapper (runs all versions)
├── .gitignore
└── README.md
```

## Test Cases

| SQL File | Description |
|----------|-------------|
| 01_simple_table.sql | Basic table with INT, VARCHAR, PRIMARY KEY |
| 02_multi_index.sql | Multiple/composite indexes |
| 03_instant_add_column.sql | INSTANT ADD column |
| 04_data_types.sql | Various column types |
| 05_nullable_no_pk.sql | No PK, hidden row_id, NULLs |
| 06_with_deletes.sql | Delete marks |
| 07_multi_page.sql | Multi-level B+tree |
| 08_instant_add_drop.sql | Multi-version schema (ADD+DROP) |
| 09_all_column_types.sql | Comprehensive types (spatial, virtual, charsets) |
| 10_upgrade_8016.sql | Upgrade phase 1 |
| 11_upgrade_8040.sql | Upgrade phase 2 (cross-version ADD/DROP) |
| 12_blob_external.sql | External BLOB storage |
| 13_vector_test.sql | VECTOR data type (MySQL 9.0+ only, uses `STRING_TO_VECTOR()`) |

## Commands

```bash
# Run tests (all versions)
make test                      # Run all tests
make test-verbose              # Run with diff output on failures
make test-update               # Update golden files

# Version-specific tests
make test-8.0                  # Run MySQL 8.0 tests only
make test-8.4                  # Run MySQL 8.4 tests only
make test-update-8.0           # Update 8.0 golden files
make test-update-8.4           # Update 8.4 golden files
make test-9.0                  # Run MySQL 9.0 tests only
make test-update-9.0           # Update 9.0 golden files

# Fixture generation
make test-fixtures-8.0         # Generate 8.0 fixtures
make test-fixtures-8.4         # Generate 8.4 fixtures
make test-upgrade-fixture-8.0  # Generate 8.0 upgrade fixture
make test-upgrade-fixture-8.4  # Generate 8.4 upgrade fixture
make test-fixtures-9.0         # Generate 9.0 fixtures (includes VECTOR test)
make test-all-fixtures         # Generate all 8.0 fixtures
```

## Adding New Test Cases

1. Create SQL script in `8.0/sql/` (and `8.4/sql/` if applicable)
2. Run `make test-fixtures-8.0 && make test-update-8.0`
3. Commit: `8.0/sql/*.sql`, `8.0/fixtures/*.ibd`, `8.0/expected/*.txt`

For MySQL 9.0-specific features (e.g., VECTOR type):
1. Add SQL script to `9.0/sql/` only
2. Run `make test-fixtures-9.0 && make test-update-9.0`
3. Commit: `9.0/sql/*.sql`, `9.0/fixtures/*.ibd`, `9.0/expected/*.txt`

## Troubleshooting

**Docker permission errors:**
```bash
sudo usermod -aG docker $USER
# Log out and back in
```

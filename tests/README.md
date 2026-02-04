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
├── 8.0/                                        # MySQL 8.0 tests
│   ├── fixtures/                               # .ibd files (generated, MySQL 8.0.40)
│   ├── expected/                               # Golden files (expected outputs)
│   ├── sql/                                    # SQL scripts to create test tables
│   ├── run_tests.sh                            # Test runner for 8.0
│   ├── generate_fixtures.sh                    # Generate fixtures via Docker MySQL 8.0
│   ├── generate_upgrade_fixture.sh             # Generate upgrade fixture (8.0.16 → 8.0.40)
│   ├── generate_json_partial_fixture.sh        # Generate json_partial fixture (LOB version chains)
│   ├── generate_json_partial_large_fixture.sh  # Generate large multi-entry LOB fixture
│   ├── generate_json_partial_purged_fixture.sh # Generate purged LOB fixture
│   └── test_inspect_blob.sh                    # Inspect-blob test runner
├── 8.4/                                        # MySQL 8.4 tests (same structure as 8.0)
├── 9.0/                                        # MySQL 9.0 tests (same structure + VECTOR test)
├── run_tests.sh                                # Top-level wrapper (runs all versions)
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
| 13_json_partial_update.sql | JSON partial update with LOB version chains |
| 13_vector_test.sql | VECTOR data type (MySQL 9.0+ only, uses `STRING_TO_VECTOR()`) |
| 14_json_partial_large.sql | Large JSON (~48KB) spanning multiple LOB index entries |
| 15_json_partial_purged.sql | JSON partial update with purged version chains |

## Commands

```bash
# Run tests (all versions)
make test                      # Run all tests
make test-verbose              # Run with diff output on failures
make test-update               # Update golden files

# Version-specific tests
make test-8.0                  # Run MySQL 8.0 tests only
make test-8.4                  # Run MySQL 8.4 tests only
make test-9.0                  # Run MySQL 9.0 tests only
make test-update-8.0           # Update 8.0 golden files
make test-update-8.4           # Update 8.4 golden files
make test-update-9.0           # Update 9.0 golden files

# Fixture generation
make test-fixtures-8.0         # Generate 8.0 fixtures
make test-fixtures-8.4         # Generate 8.4 fixtures
make test-fixtures-9.0         # Generate 9.0 fixtures (includes VECTOR test)
make test-upgrade-fixture-8.0  # Generate 8.0 upgrade fixture
make test-upgrade-fixture-8.4  # Generate 8.4 upgrade fixture
make test-all-fixtures         # Generate all 8.0 fixtures

# JSON partial update fixtures (LOB version chains)
make test-json-partial-fixture-8.0        # Generate json_partial fixture (8.0)
make test-json-partial-fixture-8.4        # Generate json_partial fixture (8.4)
make test-json-partial-fixture-9.0        # Generate json_partial fixture (9.0)
make test-json-partial-large-fixture-8.0  # Generate large multi-entry LOB fixture (8.0)
make test-json-partial-large-fixture-8.4  # Generate large multi-entry LOB fixture (8.4)
make test-json-partial-large-fixture-9.0  # Generate large multi-entry LOB fixture (9.0)
make test-json-partial-purged-fixture-8.0 # Generate purged LOB fixture (8.0)
make test-json-partial-purged-fixture-8.4 # Generate purged LOB fixture (8.4)
make test-json-partial-purged-fixture-9.0 # Generate purged LOB fixture (9.0)

# Inspect-blob tests (JSON LOB version chain traversal)
make test-inspect-blob                    # Run all inspect-blob tests
make test-inspect-blob-verbose            # Run with verbose output
make test-inspect-blob-update             # Update inspect-blob golden files
make test-inspect-blob-8.0               # Run 8.0 inspect-blob tests only
make test-inspect-blob-update-8.0        # Update 8.0 inspect-blob golden files
```

## Adding New Test Cases

1. Create SQL script in `8.0/sql/` (and `8.4/sql/`, `9.0/sql/` if applicable)
2. Run `make test-fixtures-8.0 && make test-update-8.0`
3. Commit: `8.0/sql/*.sql`, `8.0/fixtures/*.ibd`, `8.0/expected/*.txt`

For MySQL 9.0-specific features (e.g., VECTOR type):
1. Add SQL script to `9.0/sql/` only
2. Run `make test-fixtures-9.0 && make test-update-9.0`
3. Commit: `9.0/sql/*.sql`, `9.0/fixtures/*.ibd`, `9.0/expected/*.txt`

For inspect-blob tests (JSON LOB version chains):
1. Create SQL script and fixture generation script in each version directory
2. Generate fixtures: `make test-json-partial-fixture-8.0` (or large/purged variants)
3. Update golden files: `make test-inspect-blob-update-8.0`
4. Run tests: `make test-inspect-blob-8.0`
5. Commit: SQL, fixture script, `.ibd`, `expected/*.txt`, and `test_inspect_blob.sh`

## Troubleshooting

**Docker permission errors:**
```bash
sudo usermod -aG docker $USER
# Log out and back in
```

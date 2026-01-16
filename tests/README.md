# ibdNinja Test Suite

Golden file tests for ibdNinja. Requires Docker.

## Quick Start

```bash
make test-all-fixtures   # Generate all .ibd fixtures via Docker
make test-update         # Generate golden files
make test                # Run tests
```

## Directory Structure

```
tests/
├── fixtures/                    # .ibd files (generated)
├── expected/                    # Golden files (expected outputs)
├── sql/                         # SQL scripts to create test tables
├── generate_fixtures.sh         # Generate fixtures via Docker MySQL
├── generate_upgrade_fixture.sh  # Generate upgrade scenario fixture
└── run_tests.sh                 # Main test runner
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

## Commands

```bash
make test                # Run all tests
make test-verbose        # Run with diff output on failures
make test-update         # Update golden files
make test-fixtures       # Generate regular fixtures
make test-upgrade-fixture # Generate upgrade scenario fixture
make test-all-fixtures   # Generate all fixtures
```

## Adding New Test Cases

1. Create SQL script in `sql/` (e.g., `12_my_test.sql`)
2. Run `make test-fixtures && make test-update`
3. Commit: `sql/*.sql`, `fixtures/*.ibd`, `expected/*.txt`

## Troubleshooting

**Docker permission errors:**
```bash
sudo usermod -aG docker $USER
# Log out and back in
```

-- Test case: Large JSON spanning multiple LOB index entries with version chains
-- Tests: Multi-entry LOB traversal, version chains on different entries
-- Requires: innodb_page_size=16384 (default), JSON data > 48KB to span 3+ LOB index entries
--
-- IMPORTANT: This SQL only creates the table and inserts the initial row.
-- The partial updates are done by generate_json_partial_large_fixture.sh, which
-- uses a background transaction to prevent InnoDB purge from removing old
-- LOB version chain entries before the .ibd file is copied.
--
-- With 4 chunks of 12KB each (~48KB binary JSON), the data spans 3-4 LOB index
-- entries on 16KB pages. Partial updates target different chunks to create
-- version chains on different LOB index entries.

DROP TABLE IF EXISTS json_partial_large;

CREATE TABLE json_partial_large (
    id INT PRIMARY KEY,
    doc JSON
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Insert row with ~48KB+ JSON data spread across multiple LOB index entries.
-- Four 12KB chunks ensure the data spans multiple pages.
INSERT INTO json_partial_large VALUES (1,
  JSON_OBJECT(
    'chunk_a', REPEAT('A', 12000),
    'chunk_b', REPEAT('B', 12000),
    'chunk_c', REPEAT('C', 12000),
    'chunk_d', REPEAT('D', 12000),
    'small_data', JSON_OBJECT('key1', 'value1', 'key2', 42, 'key3', CAST('true' AS JSON))
  )
);

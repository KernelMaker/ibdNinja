-- Test case: JSON partial update that SHRINKS the document
-- Tests: fetching an old LOB version that is LARGER than the current one.
-- After JSON_SET() replaces a 10000-byte payload with a 200-byte one, the
-- current external length is ~10KB smaller than version 1. A fetch of v1
-- must size its buffer for v1's length, not the current length.
--
-- IMPORTANT: This SQL only creates the table and inserts the initial row.
-- The shrinking partial update is done by generate_json_shrink_fixture.sh,
-- which uses a background transaction to prevent InnoDB purge from removing
-- the old (larger) LOB version entries before the .ibd file is copied.
--
-- 'keep' stays large so the document remains externally stored after the
-- shrink (otherwise the record would no longer carry an external reference).

DROP TABLE IF EXISTS json_shrink;

CREATE TABLE json_shrink (
    id INT PRIMARY KEY,
    doc JSON
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

INSERT INTO json_shrink VALUES (1,
  JSON_OBJECT(
    'meta', JSON_OBJECT('name', 'shrink_test', 'version', 1),
    'keep', REPEAT('K', 9000),
    'payload', REPEAT('A', 10000)
  )
);

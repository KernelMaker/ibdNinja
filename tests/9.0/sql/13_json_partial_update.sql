-- Test case: JSON partial update with LOB version chains
-- Tests: Binary JSON decoding, LOB version chain traversal, --inspect-blob feature
-- Requires: innodb_page_size=16384 (default), JSON data > 8KB to trigger external storage
--
-- IMPORTANT: This SQL only creates the table and inserts the initial row.
-- The partial updates are done by generate_json_partial_fixture.sh, which
-- uses a background transaction to prevent InnoDB purge from removing old
-- LOB version chain entries before the .ibd file is copied.
--
-- LOB partial update threshold: LOB_SMALL_CHANGE_THRESHOLD = 100 bytes
-- (defined in storage/innobase/include/lob0lob.h)
-- If total modified bytes > 100, InnoDB creates new LOB index entries with
-- version chains (replace() path) rather than in-place update (replace_inline()).
-- We use same-size REPEAT() replacements (10000 bytes >> 100) to guarantee
-- partial LOB updates that create version chains.

DROP TABLE IF EXISTS json_partial;

CREATE TABLE json_partial (
    id INT PRIMARY KEY,
    doc1 JSON,
    doc2 JSON
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Insert row with complex JSON in both columns, large enough for external LOB storage.
-- doc1 tests: nested objects, arrays, various integer types, booleans, null, empty containers
-- doc2 tests: array of objects, string values
INSERT INTO json_partial VALUES (1,
  JSON_OBJECT(
    'metadata', JSON_OBJECT(
      'name', 'test_record_1',
      'version', 1,
      'tags', JSON_ARRAY('alpha', 'beta', 'gamma', 'delta'),
      'nested', JSON_OBJECT(
        'level2', JSON_OBJECT(
          'level3', JSON_OBJECT('value', 42, 'flag', CAST('true' AS JSON))
        )
      )
    ),
    'numbers', JSON_ARRAY(0, 1, -1, 255, -128, 65535, 100000, 2147483647, -2147483648, 4294967295, 9999999999),
    'empty_obj', JSON_OBJECT(),
    'empty_arr', JSON_ARRAY(),
    'nullval', CAST('null' AS JSON),
    'booleans', JSON_OBJECT('true_val', CAST('true' AS JSON), 'false_val', CAST('false' AS JSON)),
    'payload', REPEAT('A', 10000)
  ),
  JSON_OBJECT(
    'items', JSON_ARRAY(
      JSON_OBJECT('id', 1, 'name', 'item_one', 'active', CAST('true' AS JSON)),
      JSON_OBJECT('id', 2, 'name', 'item_two', 'active', CAST('false' AS JSON)),
      JSON_OBJECT('id', 3, 'name', 'item_three', 'active', CAST('true' AS JSON))
    ),
    'description', 'second json column test data',
    'large_text', REPEAT('B', 10000)
  )
);

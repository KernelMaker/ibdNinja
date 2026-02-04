-- Test case: JSON partial update with purged version chains
-- Tests: Purge detection, free list display, interactive fallback for missing versions
-- Requires: innodb_page_size=16384 (default), JSON data > 8KB to trigger external storage
--
-- IMPORTANT: This SQL only creates the table and inserts the initial row.
-- The partial updates and purge forcing are done by generate_json_partial_purged_fixture.sh.
-- Unlike other fixtures, NO background transaction is held, allowing InnoDB's purge
-- thread to clean up old LOB version chain entries.

DROP TABLE IF EXISTS json_partial_purged;

CREATE TABLE json_partial_purged (
    id INT PRIMARY KEY,
    doc JSON
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Insert row with JSON data large enough for external LOB storage.
-- Fields: status, counter, payload (10KB to trigger external storage)
INSERT INTO json_partial_purged VALUES (1,
  JSON_OBJECT(
    'status', 'active',
    'counter', 1,
    'payload', REPEAT('A', 10000)
  )
);

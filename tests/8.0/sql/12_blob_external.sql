-- Test case: External BLOB storage (modern LOB pages)
-- Tests: LOB_FIRST, LOB_DATA page parsing, external field fetching
-- Requires: innodb_page_size=16384 (default), data > 8KB to trigger external storage

DROP TABLE IF EXISTS blob_external;

CREATE TABLE blob_external (
    id INT PRIMARY KEY,
    description VARCHAR(100),
    data LONGBLOB,
    extra TEXT
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Insert rows with data large enough to be stored externally (>8KB for 16KB pages)
-- REPEAT generates a string of the given length
INSERT INTO blob_external VALUES
    (1, 'small inline', REPEAT('A', 100), 'inline text'),
    (2, 'external blob', REPEAT('B', 16000), 'has external blob'),
    (3, 'large external', REPEAT('C', 32000), 'larger external blob'),
    (4, 'very large', REPEAT('D', 65000), 'very large external blob'),
    (5, 'mixed content', REPEAT('E', 20000), REPEAT('F', 20000));

-- Update a row to potentially generate version history
UPDATE blob_external SET data = REPEAT('X', 16000) WHERE id = 2;

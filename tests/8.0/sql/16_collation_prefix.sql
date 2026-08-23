-- Test case: utf8mb4_0900_bin CHAR columns and CHAR-prefix secondary index
-- Tests:
--   1. CHAR(n) COLLATE utf8mb4_0900_bin must be parsed as variable-length
--      (mbminlen=1 != mbmaxlen=4); a wrong fixed-length assumption corrupts
--      every field after it in the record.
--   2. A secondary index on a prefix of a fixed-width CHAR column
--      (KEY (c_lat(10)) on a latin1 CHAR(20)) stores a fixed 10-byte field;
--      ignoring the prefix misparses every field after it in that index.

DROP TABLE IF EXISTS collation_prefix;

CREATE TABLE collation_prefix (
    id INT PRIMARY KEY,
    c_bin CHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin,
    trailing_int INT NOT NULL,
    trailing_str VARCHAR(20),
    c_lat CHAR(20) CHARACTER SET latin1,
    KEY idx_lat_prefix (c_lat(10)),
    KEY idx_bin (c_bin)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

INSERT INTO collation_prefix VALUES
    (1, 'abc',        1111, 'after-one',   'prefix-key-000000001'),
    (2, 'de',         2222, 'after-two',   'prefix-key-000000002'),
    (3, 'fghij',      3333, 'after-three', 'prefix-key-000000003'),
    (4, 'klmnopqrst', 4444, 'after-four',  'prefix'),
    (5, NULL,         5555, 'after-five',  NULL);

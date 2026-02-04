-- Test case: Table with various data types
-- Tests: Different column types parsing

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
    id INT PRIMARY KEY,
    -- Integer types
    tiny_col TINYINT,
    small_col SMALLINT,
    medium_col MEDIUMINT,
    big_col BIGINT,
    -- Decimal types
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(10,2),
    -- String types
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    -- Binary types
    binary_col BINARY(16),
    varbinary_col VARBINARY(255),
    blob_col BLOB,
    -- Date/Time types
    date_col DATE,
    time_col TIME,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    -- Other types
    enum_col ENUM('A', 'B', 'C'),
    set_col SET('X', 'Y', 'Z'),
    bit_col BIT(8),
    json_col JSON
) ENGINE=InnoDB;

INSERT INTO data_types VALUES (
    1,
    127, 32767, 8388607, 9223372036854775807,
    3.14, 3.14159265359, 12345.67,
    'CHAR10', 'Variable length string', 'This is a text field',
    X'0102030405060708090A0B0C0D0E0F10', X'DEADBEEF', X'CAFEBABE',
    '2024-06-15', '14:30:00', '2024-06-15 14:30:00', CURRENT_TIMESTAMP, 2024,
    'B', 'X,Z',
    b'10101010',
    '{"key": "value", "number": 42}'
);

INSERT INTO data_types VALUES (
    2,
    -128, -32768, -8388608, -9223372036854775808,
    -1.5, -2.718281828, -99999.99,
    'ABC', 'Another string', 'More text here',
    X'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', X'12345678', X'',
    '2000-01-01', '00:00:00', '2000-01-01 00:00:00', '2000-01-01 00:00:01', 2000,
    'A', 'Y',
    b'11111111',
    '[]'
);

-- NULL values test
INSERT INTO data_types (id, tiny_col, varchar_col) VALUES (3, NULL, NULL);

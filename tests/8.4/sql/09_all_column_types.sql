SET time_zone = '+00:00';

CREATE TABLE type_test (
    -- Numeric types
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_mediumint MEDIUMINT,
    col_int INT,
    col_bigint BIGINT,
    col_decimal_small DECIMAL(5,2),
    col_decimal_large DECIMAL(20,5),
    col_float FLOAT,
    col_double DOUBLE,
    
    -- Bit types
    col_bit1 BIT(1),
    col_bit8 BIT(8),
    col_bit64 BIT(64),
    
    -- String types with different charsets/collations
    col_char_utf8 CHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    col_char_latin1 CHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci,
    col_varchar_utf8 VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    col_varchar_latin1 VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_bin,
    col_text_utf8 TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    col_text_latin1 TEXT CHARACTER SET latin1 COLLATE latin1_swedish_ci,
    
    -- Binary types
    col_binary BINARY(10),
    col_varbinary VARBINARY(100),
    col_blob BLOB,
    
    -- Temporal types with different precision
    col_datetime0 DATETIME(0),
    col_datetime3 DATETIME(3),
    col_datetime6 DATETIME(6),
    col_timestamp0 TIMESTAMP(0),
    col_timestamp3 TIMESTAMP(3),
    col_timestamp6 TIMESTAMP(6),
    col_time0 TIME(0),
    col_time3 TIME(3),
    col_time6 TIME(6),
    col_date DATE,
    col_year YEAR,
    
    -- Spatial data types
    col_geometry GEOMETRY NOT NULL,
    col_point POINT NOT NULL,
    col_linestring LINESTRING NOT NULL,
    col_polygon POLYGON NOT NULL,
    col_multipoint MULTIPOINT NOT NULL,
    col_multilinestring MULTILINESTRING NOT NULL,
    col_multipolygon MULTIPOLYGON NOT NULL,
    col_geometrycollection GEOMETRYCOLLECTION NOT NULL,
    
    -- Enum and Set
    col_enum ENUM('small', 'medium', 'large'),
    col_set SET('red', 'green', 'blue'),
    
    -- JSON type
    col_json JSON,
    
    -- Virtual columns
    col_virtual_int INT GENERATED ALWAYS AS (col_int * 2) VIRTUAL,
    col_virtual_concat VARCHAR(100) GENERATED ALWAYS AS (CONCAT(col_char_utf8, '_suffix')) VIRTUAL,
    col_virtual_datetime DATETIME GENERATED ALWAYS AS (DATE_ADD(col_datetime0, INTERVAL 1 DAY)) VIRTUAL,
    
    -- Verification column
    verify_column INT,
    
    -- Primary Key
    PRIMARY KEY (col_int),
    
    -- Regular indexes
    INDEX idx_composite (col_varchar_utf8, col_datetime0),
    UNIQUE INDEX idx_unique (col_bigint),
    
    -- Prefix length indexes
    INDEX idx_text_prefix (col_text_utf8(50)),
    INDEX idx_varchar_prefix (col_varchar_utf8(25)),
    
    -- Virtual column indexes
    INDEX idx_virtual_int (col_virtual_int),
    INDEX idx_virtual_concat (col_virtual_concat),
    
    -- Spatial indexes
    SPATIAL INDEX idx_point (col_point),
    SPATIAL INDEX idx_geometry (col_geometry),

    -- FTS
    FULLTEXT INDEX idx_fulltext (col_text_utf8)
);

-- Insert test data
INSERT INTO type_test (
    -- Numeric columns
    col_tinyint, col_smallint, col_mediumint, col_int, col_bigint,
    col_decimal_small, col_decimal_large, col_float, col_double,
    
    -- Bit columns
    col_bit1, col_bit8, col_bit64,
    
    -- String columns
    col_char_utf8, col_char_latin1, col_varchar_utf8, col_varchar_latin1,
    col_text_utf8, col_text_latin1,
    
    -- Binary columns
    col_binary, col_varbinary, col_blob,
    
    -- Temporal columns
    col_datetime0, col_datetime3, col_datetime6,
    col_timestamp0, col_timestamp3, col_timestamp6,
    col_time0, col_time3, col_time6,
    col_date, col_year,
    
    -- Spatial columns
    col_geometry, col_point, col_linestring, col_polygon,
    col_multipoint, col_multilinestring, col_multipolygon, col_geometrycollection,
    
    -- Enum and Set
    col_enum, col_set,
    
    -- JSON
    col_json,
    
    -- Verification column
    verify_column
) VALUES
-- Row 1: Minimum values
(
    -128, -32768, -8388608, 1, -9223372036854775808,
    -999.99, -99999.99999, -3.402823466E+38, -1.7976931348623157E+308,
    
    b'0', b'00000000', b'0000000000000000000000000000000000000000000000000000000000000000',
    
    '', '', '', '',
    '', '',
    
    '', '', '',
    
    '1000-01-01 00:00:00', '1000-01-01 00:00:00.000', '1000-01-01 00:00:00.000000',
    '1970-01-01 00:00:01', '1970-01-01 00:00:01.000', '1970-01-01 00:00:01.000000',
    '-838:59:59', '-838:59:59.000', '-838:59:59.000000',
    '1000-01-01', 1901,
    
    ST_GeomFromText('POINT(0 0)'),
    ST_PointFromText('POINT(0 0)'),
    ST_LineStringFromText('LINESTRING(0 0,0 0)'),
    ST_PolygonFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'),
    ST_MultiPointFromText('MULTIPOINT(0 0)'),
    ST_MultiLineStringFromText('MULTILINESTRING((0 0,0 0))'),
    ST_MultiPolygonFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(0 0))'),
    
    'small', '',
    
    '{}',
    
    666
),
-- Row 2: Maximum values
(
    127, 32767, 8388607, 2, 9223372036854775807,
    999.99, 99999.99999, 3.402823466E+38, 1.7976931348623157E+308,
    
    b'1', b'11111111', b'1111111111111111111111111111111111111111111111111111111111111111',
    
    REPEAT('A', 10), REPEAT('B', 10), REPEAT('C', 50), REPEAT('D', 50),
    REPEAT('E', 255), REPEAT('F', 255),
    
    REPEAT('G', 10), REPEAT('H', 100), REPEAT('I', 255),
    
    '9999-12-30 23:59:59', '9999-12-30 23:59:59.499', '9999-12-30 23:59:59.499999',
    '2038-01-19 03:14:07', '2038-01-19 03:14:07.499', '2038-01-19 03:14:07.499999',
    '838:59:59', '838:59:58.999', '838:59:58.999999',
    '9999-12-31', 2155,
    
    ST_GeomFromText('POINT(180 90)'),
    ST_PointFromText('POINT(180 90)'),
    ST_LineStringFromText('LINESTRING(-180 -90,180 90)'),
    ST_PolygonFromText('POLYGON((-180 -90,-180 90,180 90,180 -90,-180 -90))'),
    ST_MultiPointFromText('MULTIPOINT(-180 -90,180 90)'),
    ST_MultiLineStringFromText('MULTILINESTRING((-180 -90,180 90),(180 -90,-180 90))'),
    ST_MultiPolygonFromText('MULTIPOLYGON(((-180 -90,-180 90,180 90,180 -90,-180 -90)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(180 90),LINESTRING(-180 -90,180 90))'),
    
    'large', 'red,green,blue',
    
    '{"nested": {"array": [1,2,3], "object": {"key": "max"}}, "string": "max", "number": 9999}',
    
    666
),
-- Row 3: Mixed/average values
(
    0, 0, 0, 3, 0,
    0.00, 0.00000, 0.0, 0.0,
    
    b'1', b'10101010', b'1010101010101010101010101010101010101010101010101010101010101010',
    
    'Mixed中文', 'Mixed', 'Mixed UTF8 中文', 'Mixed Latin1',
    'Mixed Text UTF8 中文', 'Mixed Text Latin1',
    
    'BINARY_MIX', 'VARBINARY_MIX', 'BLOB_MIX',
    
    '2024-01-01 12:00:00', '2024-01-01 12:00:00.123', '2024-01-01 12:00:00.123456',
    '2024-01-01 12:00:00', '2024-01-01 12:00:00.123', '2024-01-01 12:00:00.123456',
    '12:00:00', '12:00:00.123', '12:00:00.123456',
    '2024-01-01', 2024,
    
    ST_GeomFromText('POINT(45 45)'),
    ST_PointFromText('POINT(45 45)'),
    ST_LineStringFromText('LINESTRING(-45 -45,45 45)'),
    ST_PolygonFromText('POLYGON((-45 -45,-45 45,45 45,45 -45,-45 -45))'),
    ST_MultiPointFromText('MULTIPOINT(-45 -45,45 45)'),
    ST_MultiLineStringFromText('MULTILINESTRING((-45 -45,45 45),(45 -45,-45 45))'),
    ST_MultiPolygonFromText('MULTIPOLYGON(((-45 -45,-45 45,45 45,45 -45,-45 -45)))'),
    ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(45 45),LINESTRING(-45 -45,45 45))'),
    
    'medium', 'red,blue',
    
    '{"mixed": {"array": [1], "object": {"key": "value"}}, "string": "mixed", "number": 123}',
    
    666
);
-- Add additional composite indexes
ALTER TABLE type_test
ADD INDEX idx_mixed_types (col_int, col_char_utf8(10), col_datetime0),
ADD INDEX idx_virtual_composite (col_virtual_int, col_virtual_concat(20));

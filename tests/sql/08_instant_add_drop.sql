-- Test case: Multi-version schema with INSTANT ADD and DROP columns
-- Tests: Dynamic parsing of records across multiple table definition versions
-- Based on the example from README Section 3

DROP TABLE IF EXISTS instant_add_drop;

-- V1: Initial table definition
CREATE TABLE instant_add_drop (
    col_uint INT UNSIGNED NOT NULL PRIMARY KEY,
    col_datetime_0 DATETIME DEFAULT NULL,
    col_varchar VARCHAR(10) DEFAULT NULL
) ENGINE=InnoDB;

-- Insert record under V1
INSERT INTO instant_add_drop VALUES (1, NOW(), 'Row_V1');

-- V2: Add two columns instantly
ALTER TABLE instant_add_drop ADD COLUMN col_datetime_6 DATETIME(6), ALGORITHM=INSTANT;
ALTER TABLE instant_add_drop ADD COLUMN col_char CHAR(10) DEFAULT 'abc', ALGORITHM=INSTANT;

-- Insert record under V2
INSERT INTO instant_add_drop VALUES (2, NOW(), 'Row_V2', NOW(), 'ibdNinja');

-- V3: Drop two columns instantly (MySQL 8.0.29+)
ALTER TABLE instant_add_drop DROP COLUMN col_varchar, ALGORITHM=INSTANT;
ALTER TABLE instant_add_drop DROP COLUMN col_char, ALGORITHM=INSTANT;

-- Insert record under V3
INSERT INTO instant_add_drop VALUES (3, NOW(), NOW());

-- Now the table has records from 3 different schema versions:
-- Record 1 (V1): has col_varchar value, no col_datetime_6/col_char
-- Record 2 (V2): has all columns including dropped ones
-- Record 3 (V3): no dropped columns

-- Test case: Table with INSTANT ADD column
-- Tests: INSTANT DDL, default values, row versioning

DROP TABLE IF EXISTS instant_add_col;

CREATE TABLE instant_add_col (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    value INT
) ENGINE=InnoDB;

-- Insert initial data
INSERT INTO instant_add_col VALUES
    (1, 'Row1', 100),
    (2, 'Row2', 200),
    (3, 'Row3', 300);

-- Add column instantly (MySQL 8.0.12+)
ALTER TABLE instant_add_col ADD COLUMN new_col1 INT DEFAULT 0, ALGORITHM=INSTANT;
ALTER TABLE instant_add_col ADD COLUMN new_col2 VARCHAR(50) DEFAULT 'default_value', ALGORITHM=INSTANT;

-- Insert more data after instant add
INSERT INTO instant_add_col VALUES
    (4, 'Row4', 400, 40, 'custom4'),
    (5, 'Row5', 500, 50, 'custom5');

-- Update some old rows
UPDATE instant_add_col SET new_col1 = 10 WHERE id = 1;

-- Test case: Table with nullable columns and no explicit primary key
-- Tests: Hidden row_id, NULL handling

DROP TABLE IF EXISTS nullable_no_pk;

CREATE TABLE nullable_no_pk (
    col1 INT,
    col2 VARCHAR(100),
    col3 INT,
    col4 VARCHAR(50),
    INDEX idx_col1 (col1)
) ENGINE=InnoDB;

INSERT INTO nullable_no_pk VALUES
    (1, 'Value1', 100, 'A'),
    (2, NULL, 200, 'B'),
    (NULL, 'Value3', NULL, 'C'),
    (4, 'Value4', 400, NULL),
    (NULL, NULL, NULL, NULL);

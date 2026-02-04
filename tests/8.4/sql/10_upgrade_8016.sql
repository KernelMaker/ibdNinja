-- Initial table definition (MySQL 8.0.16)
CREATE TABLE ddl_test (
    id INT PRIMARY KEY,
    original_col1 VARCHAR(50),
    original_col2 INT,
    verify_column INT
);

ALTER TABLE ddl_test
ADD INDEX idx_original_col1 (original_col1);

-- Initial data insert
INSERT INTO ddl_test VALUES
(1, 'original_data_1', 100, 666),
(2, 'original_data_2', 200, 666),
(3, 'original_data_3', 300, 666);

-- First ADD COLUMN in 8.0.16
ALTER TABLE ddl_test ADD COLUMN data_v2 DATETIME;

-- Insert after first ADD COLUMN
INSERT INTO ddl_test VALUES
(4, 'after_add_v2_1', 400, 666, '2024-01-01 10:00:00'),
(5, 'after_add_v2_2', 500, 666, '2024-01-01 11:00:00');

-- Second ADD COLUMN in 8.0.16
ALTER TABLE ddl_test ADD COLUMN data_v3 JSON;

ALTER TABLE ddl_test
ADD INDEX idx_data_v3 ((CAST(data_v3->'$.key' AS CHAR(50))));

-- Insert after second ADD COLUMN
INSERT INTO ddl_test VALUES
(6, 'after_add_v3_1', 600, 666, '2024-01-01 12:00:00', '{"key": "value1"}'),
(7, 'after_add_v3_2', 700, 666, '2024-01-01 13:00:00', '{"key": "value2"}');

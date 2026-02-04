-- Assuming that the test-add-column-8016.sql has been executed on MySQL 8.0.16,
-- and the system has been upgraded to MySQL 8.0.40 to run the test.

-- Insert data before any changes in 8.0.40
INSERT INTO ddl_test VALUES
(8, 'before_8040_changes_1', 800, 666, '2024-01-01 14:00:00', '{"key": "value3"}'),
(9, 'before_8040_changes_2', 900, 666, '2024-01-01 15:00:00', '{"key": "value4"}');

-- First ADD COLUMN in 8.0.40
ALTER TABLE ddl_test ADD COLUMN data_v4 TEXT;

-- Insert after first ADD in 8.0.40
INSERT INTO ddl_test VALUES
(10, 'after_8040_add_v4_1', 1000, 666, '2024-01-01 16:00:00', '{"key": "value5"}', 'text_data_1'),
(11, 'after_8040_add_v4_2', 1100, 666, '2024-01-01 17:00:00', '{"key": "value6"}', 'text_data_2');

-- Second ADD COLUMN in 8.0.40
ALTER TABLE ddl_test ADD COLUMN data_v5 DECIMAL(10,2);
ALTER TABLE ddl_test
ADD INDEX idx_data_v5 (data_v5);

-- Insert after second ADD
INSERT INTO ddl_test VALUES
(12, 'after_8040_add_v5_1', 1200, 666, '2024-01-01 18:00:00', '{"key": "value7"}', 'text_data_3', 123.45),
(13, 'after_8040_add_v5_2', 1300, 666, '2024-01-01 19:00:00', '{"key": "value8"}', 'text_data_4', 678.90);

-- First DROP COLUMN in 8.0.40 (dropping original column)
ALTER TABLE ddl_test DROP COLUMN original_col2;

-- Insert after first DROP
INSERT INTO ddl_test VALUES
(14, 'after_drop_original_1', 666, '2024-01-01 20:00:00', '{"key": "value9"}', 'text_data_5', 777.77),
(15, 'after_drop_original_2', 666, '2024-01-01 21:00:00', '{"key": "value10"}', 'text_data_6', 888.88);

-- Second DROP COLUMN in 8.0.40 (dropping column added in 8.0.16)
ALTER TABLE ddl_test DROP COLUMN data_v2;

-- Insert after second DROP
INSERT INTO ddl_test VALUES
(16, 'after_drop_8016col_1', 666, '{"key": "value11"}', 'text_data_7', 999.99),
(17, 'after_drop_8016col_2', 666, '{"key": "value12"}', 'text_data_8', 111.11);

-- Third DROP COLUMN in 8.0.40 (dropping column added in 8.0.40)
ALTER TABLE ddl_test DROP COLUMN data_v4;

-- Final data insert
INSERT INTO ddl_test VALUES
(18, 'final_data_1', 666, '{"key": "value13"}', 222.22),
(19, 'final_data_2', 666, '{"key": "value14"}', 333.33);

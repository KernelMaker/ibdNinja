-- Test case: Simple table with basic data types
-- Tests: Basic parsing, INT, VARCHAR, PRIMARY KEY

DROP TABLE IF EXISTS simple_table;

CREATE TABLE simple_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    email VARCHAR(255)
) ENGINE=InnoDB;

INSERT INTO simple_table VALUES
    (1, 'Alice', 30, 'alice@example.com'),
    (2, 'Bob', 25, 'bob@example.com'),
    (3, 'Charlie', 35, 'charlie@example.com'),
    (4, 'Diana', 28, 'diana@example.com'),
    (5, 'Eve', 32, 'eve@example.com');

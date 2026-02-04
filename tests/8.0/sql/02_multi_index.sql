-- Test case: Table with multiple indexes
-- Tests: Secondary indexes, composite indexes

DROP TABLE IF EXISTS multi_index;

CREATE TABLE multi_index (
    id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    department_id INT,
    created_at DATETIME,
    INDEX idx_name (last_name, first_name),
    INDEX idx_dept (department_id),
    UNIQUE INDEX idx_email (email)
) ENGINE=InnoDB;

INSERT INTO multi_index VALUES
    (1, 'John', 'Smith', 'john.smith@example.com', 10, '2024-01-15 10:00:00'),
    (2, 'Jane', 'Doe', 'jane.doe@example.com', 20, '2024-01-16 11:00:00'),
    (3, 'Bob', 'Smith', 'bob.smith@example.com', 10, '2024-01-17 12:00:00'),
    (4, 'Alice', 'Johnson', 'alice.j@example.com', 30, '2024-01-18 13:00:00'),
    (5, 'Charlie', 'Brown', 'charlie.b@example.com', 20, '2024-01-19 14:00:00');

-- Test case: Table with deleted rows
-- Tests: Delete-marked records

DROP TABLE IF EXISTS with_deletes;

CREATE TABLE with_deletes (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    status INT
) ENGINE=InnoDB;

-- Insert data
INSERT INTO with_deletes VALUES
    (1, 'Keep1', 1),
    (2, 'Delete2', 2),
    (3, 'Keep3', 3),
    (4, 'Delete4', 4),
    (5, 'Keep5', 5),
    (6, 'Delete6', 6),
    (7, 'Keep7', 7),
    (8, 'Delete8', 8),
    (9, 'Keep9', 9),
    (10, 'Delete10', 10);

-- Delete some rows (will leave delete marks until purge)
DELETE FROM with_deletes WHERE id IN (2, 4, 6, 8, 10);

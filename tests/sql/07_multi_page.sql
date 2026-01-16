-- Test case: Table large enough to span multiple pages
-- Tests: Multi-level B+tree, non-leaf pages

DROP TABLE IF EXISTS multi_page;

CREATE TABLE multi_page (
    id INT PRIMARY KEY,
    data VARCHAR(500)
) ENGINE=InnoDB;

-- Insert enough rows to create multiple pages
-- Each row is ~500+ bytes, 16KB page fits ~30 rows
-- Need ~100+ rows for multiple pages
DELIMITER //
DROP PROCEDURE IF EXISTS fill_multi_page//
CREATE PROCEDURE fill_multi_page()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 500 DO
        INSERT INTO multi_page VALUES (i, REPEAT(CONCAT('Data-', i, '-'), 30));
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL fill_multi_page();
DROP PROCEDURE fill_multi_page;

-- Test case: VECTOR data type (MySQL 9.0+)
DROP TABLE IF EXISTS vector_test;

CREATE TABLE vector_test (
    id INT PRIMARY KEY,
    description VARCHAR(100),
    embedding VECTOR(4),
    embedding_large VECTOR(16),
    json_col JSON
) ENGINE=InnoDB;

INSERT INTO vector_test VALUES (
    1, 'first vector',
    STRING_TO_VECTOR('[1.0, 2.0, 3.0, 4.0]'),
    STRING_TO_VECTOR('[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6]'),
    '{"key": "value"}'
);

INSERT INTO vector_test VALUES (
    2, 'second vector',
    STRING_TO_VECTOR('[5.0, 6.0, 7.0, 8.0]'),
    STRING_TO_VECTOR('[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]'),
    '[]'
);

INSERT INTO vector_test (id, description) VALUES (3, 'null vectors');

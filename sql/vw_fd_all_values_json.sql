-- SELECT to_json(67);
CREATE OR REPLACE VIEW vw_fd_all_values_json AS
SELECT 
    dv.entity_id AS entity_id,
    dv.class_type AS class_type,
    dv.class_name AS class_name,
    dv.char_name AS char_name,
    to_json(dv.decimal_value) AS json_value,
FROM vw_fd_decimal_values dv
UNION
SELECT
    iv.entity_id AS entity_id,
    iv.class_type AS class_type,
    iv.class_name AS class_name,
    iv.char_name AS char_name,
    to_json(iv.integer_value) AS json_value,
FROM vw_fd_integer_values iv
UNION
SELECT
    tv.entity_id AS entity_id,
    tv.class_type AS class_type,
    tv.class_name AS class_name,
    tv.char_name AS char_name,
    to_json(IF(tv.text_value IS NULL, '', tv.text_value)) AS json_value,
FROM vw_fd_text_values tv;
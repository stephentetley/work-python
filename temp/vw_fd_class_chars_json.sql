CREATE OR REPLACE VIEW vw_fd_class_chars_json AS
SELECT
    entity_id AS entity_id,
    json_group_object(class_name, json_characteristics) AS json_classes,
FROM
    (SELECT
        entity_id AS entity_id,
        class_name AS class_name,
        json_group_object(char_name, json_array_values) AS json_characteristics,
    FROM
        (SELECT 
            entity_id AS entity_id,
            class_name AS class_name,
            char_name AS char_name,
            json_group_array(json_value) AS json_array_values,
        FROM vw_fd_all_values_json
        GROUP BY entity_id, class_name, char_name
        ORDER BY entity_id, class_name, char_name)
    GROUP BY entity_id, class_name
    ORDER BY entity_id, class_name)
GROUP BY entity_id
ORDER BY entity_id


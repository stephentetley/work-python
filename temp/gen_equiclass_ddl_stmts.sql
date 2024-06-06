WITH cte AS 
    (SELECT  
        class_name AS class_name,
        lower(class_name) AS table_name, 
        string_agg(lower(char_name) || ' ' || ddl_data_type, ', ' ORDER BY char_name ASC) AS other_fields
    FROM s4_classlists.vw_refined_equi_characteristic_defs
    GROUP BY class_name)
SELECT 
    'CREATE OR REPLACE TABLE ai2_class_rep.' || t.table_name || '(ai2_reference VARCHAR NOT NULL, ' || other_fields || ', PRIMARY KEY(ai2_reference));'
FROM cte t
JOIN s4_classlists.vw_equi_class_defs ec ON ec.class_name = t.class_name
WHERE 
    ec.is_object_class = TRUE
ORDER BY t.table_name ASC;
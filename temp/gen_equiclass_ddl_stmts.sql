WITH cte AS 
    (SELECT DISTINCT ON(class_name) 
        'equiclass_' || lower(class_name) AS table_name, 
        string_agg(lower(char_name) || ' ' || ddl_data_type, ', ') OVER (PARTITION BY class_name) AS other_fields
    FROM s4_classlists.vw_refined_characteristic_defs)
SELECT 
    'CREATE OR REPLACE TABLE ai2_class_rep.' || t.table_name || '(ai2_reference VARCHAR NOT NULL, ' || other_fields || ', PRIMARY KEY(ai2_reference));'
FROM cte t
ORDER BY t.table_name ASC;
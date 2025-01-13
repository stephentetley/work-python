
-- floc characteristics

(SELECT 
    t1.funcloc AS functional_location,
    '003' AS class_type,
    1 AS status,
    'SOLUTION_ID' AS class,
    'SOLUTION_ID' AS characteristic,
    kv.value AS char_value,
FROM working.gen_flocs t1
JOIN raw_data.config kv ON kv.key = 'Solution Id'
WHERE kv.value IS NOT NULL)
UNION BY NAME
(SELECT 
    t1.funcloc AS functional_location,
    '003' AS class_type,
    1 AS status,
    'EAST_NORTH' AS class,
    'EASTING' AS characteristic,
    kv.value AS char_value,
FROM working.gen_flocs t1
JOIN raw_data.config kv ON kv.key = 'Easting'
WHERE kv.value IS NOT NULL)
UNION BY NAME
(SELECT 
    t1.funcloc AS functional_location,
    '003' AS class_type,
    1 AS status,
    'EAST_NORTH' AS class,
    'NORTHING' AS characteristic,
    kv.value AS char_value,
FROM working.gen_flocs t1
JOIN raw_data.config kv ON kv.key = 'Northing'
WHERE kv.value IS NOT NULL)
ORDER BY functional_location, class, characteristic
;



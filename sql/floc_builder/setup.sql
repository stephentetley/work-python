CREATE SCHEMA IF NOT EXISTS working;

CREATE OR REPLACE TABLE working.existing_flocs AS
SELECT 
    t.functional_location AS funcloc,
    t.description_of_functional_location AS floc_name,
FROM raw_data.ih06_export t
ORDER BY funcloc 
    



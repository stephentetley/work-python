--- source table has _duplicates_ which cause an error without the row_number interior table 
INSERT OR REPLACE INTO s4_ih_equipment_masterdata BY NAME
    SELECT 
        e.equipment AS equi_id,
        e.address_number AS address_ref,
        NULL AS catalog_profile,
        NULL AS category,
        NULL AS company_code,
        NULL AS construction_month,
        NULL AS construction_year,
        NULL AS controlling_area,
        NULL AS cost_center,
        e.description_of_technical_object AS description,
        e.functional_location AS functional_location,
        NULL AS gross_weight,
        NULL AS location,
        NULL AS main_work_center,
        NULL AS maintenance_plant,
        e.manufactserialnumber AS serial_number,
        e.manufacturer_part_number AS manufact_part_number,
        e.manufacturer_of_asset AS manufacturer,
        e.model_number AS model_number,
        e.object_type AS object_type,
        e.planning_plant AS planning_plant,
        NULL AS plant_section,
        IF(e.position IS NULL, NULL, CAST(e.position AS INTEGER)) AS display_position,
        NULL AS startup_date,     --- IF(e.inbdt IS NOT NULL, strptime(e.inbdt, '%d.%m.%Y'), NULL) AS startup_date,
        e.superord_equipment AS superord_id,
        NULL AS system_status,
        e.technical_identification_no AS technical_ident_number,
        NULL AS unit_of_weight,
        e.user_status AS user_status,
        NULL AS valid_from,     --- IF(e.data_eeqz IS NOT NULL, strptime(e.data_eeqz, '%d.%m.%Y'), NULL) AS valid_from,
        NULL AS work_center,
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY e1.equipment) AS rownum,
        FROM s4_raw_data.equi_masterdata e1
        ) e
    WHERE e.rownum = 1
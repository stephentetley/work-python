-- EAST_NORTH
WITH cte_num AS 
    (SELECT equi, charid, atflv, 
    FROM s4_fd_raw_data.valuaequi_valuaequi1
    WHERE charid in ('EASTING', 'NORTHING')
)
PIVOT cte_num
ON charid
USING first(atflv)
;

-- case pivot for the win!
SELECT 
    ve.equi AS equipment_id, 
    any_value(CASE WHEN ve.charid = 'ACTU_ATEX_CODE' THEN ve.atflv ELSE NULL END) AS actu_atex_code,
    any_value(CASE WHEN ve.charid = 'ACTU_RATED_POWER_KW' THEN ve.atflv ELSE NULL END) AS actu_rated_power_kw,
    any_value(CASE WHEN ve.charid = 'ACTU_RATED_VOLTAGE' THEN ve.atflv ELSE NULL END) AS actu_rated_voltage,
    any_value(CASE WHEN ve.charid = 'ACTU_RATED_VOLTAGE_UNITS' THEN ve.atwrt ELSE NULL END) AS actu_rated_voltage_units,
    any_value(CASE WHEN ve.charid = 'ACTU_SPEED_RPM' THEN ve.atflv ELSE NULL END) AS actu_speed_rpm,
    any_value(CASE WHEN ve.charid = 'IP_RATING' THEN ve.atwrt ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN ve.charid = 'UNICLASS_CODE' THEN ve.atwrt ELSE NULL END) AS uniclass_code,
    any_value(CASE WHEN ve.charid = 'UNICLASS_DESC' THEN ve.atwrt ELSE NULL END) AS uniclass_desc,

FROM s4_fd_raw_data.valuaequi_valuaequi1 ve
JOIN s4_fd_raw_data.classequi_classequi1 ce ON ve.equi = ce.equi 
WHERE ce.class = 'ACTUEM'
GROUP BY equipment_id
;

-- Not real case as dups...
SELECT 
    ve.equi AS equipment_id, 
    any_value(CASE WHEN ve.charid = 'AI2_AIB_REFERENCE' THEN ve.atwrt ELSE NULL END) AS ai2_aib_reference,
FROM s4_fd_raw_data.valuaequi_valuaequi1 ve
JOIN s4_fd_raw_data.classequi_classequi1 ce ON ve.equi = ce.equi 
WHERE ce.class = 'AIB_REFERENCE'
GROUP BY equipment_id

"""
Copyright 2023 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import pandas as pd
import duckdb
import io

def blank_na(obj):
    return '' if pd.isna(obj) else obj


def print_valuaequi(df: pd.DataFrame, *, out_path: str) -> str:
    """Formatting done in DB"""
    columns = ['*EQUI', 'ATAW1', 'ATAWE', 'ATAUT', 'CHARID', 'ATNAM', 'ATWRT', 'CLASSTYPE', 'ATCOD', 'ATVGLART', 'TEXTBEZ', 'ATZIS', 'VALCNT', 'ATIMB', 'ATSRT', 'ATFLV', 'ATFLB']
    with io.open(out_path, mode='w', encoding='utf-8') as outw:
        outw.write('* Upload\n')
        outw.write('* Data Model: U1\n')
        outw.write('* Entity Type: VALUAEQUI\n')
        outw.write('* Variant: VALUAEQUI1\n')
        outw.write('\t'.join(columns) + '\n')
        for row in df.itertuples():
            outw.write(f'{row.equipment}\t\t\t\t{row.char_id}\t\t{blank_na(row.char_value)}\t{row.class_type}\t{blank_na(row.code)}\t\t\t{row.instance_counter}\t{row.int_counter_value}\t\t{row.position}\t{blank_na(row.value_from)}\t{blank_na(row.value_to)}\t\n')
    outw.close()

def setup_views(*, rated_power: str, outlet_size_mm: str, con=duckdb.DuckDBPyConnection) -> None:
    con.execute(vw_fd_all_values_scalar_ddl)
    con.execute(temp_vw_cp_upload_annihilator_ddl)
    con.execute(temp_vw_cp_upload_assign_const_ddl(rated_power=rated_power, outlet_size_mm=outlet_size_mm))

vw_fd_all_values_scalar_ddl = """
    CREATE OR REPLACE VIEW vw_fd_all_values_scalar AS
    SELECT 
        fdv.entity_id AS equipment,
        fdv.class_name || '::' || fdv.char_name AS scoped_charid,
        fdv.class_type AS class_type,
        'DECIMAL' AS char_type,
        fdv.decimal_precision AS decimal_precision,
        CAST(fdv.int_counter_value AS INTEGER) AS int_counter_value,
        NULL AS text_value,
        NULL AS integer_value,
        fdv.decimal_value AS decimal_value,
    FROM vw_fd_decimal_values fdv
    UNION 
    SELECT 
        fiv.entity_id AS equipment,
        fiv.class_name || '::' || fiv.char_name AS scoped_charid,
        fiv.class_type AS class_type,
        'INTEGER' AS char_type,
        NULL AS decimal_precision,
        CAST(fiv.int_counter_value AS INTEGER) AS int_counter_value,
        NULL AS text_value,
        fiv.integer_value AS integer_value,
        NULL AS decimal_value,
    FROM vw_fd_integer_values fiv
    UNION 
    SELECT 
        ftv.entity_id AS equipment,
        ftv.class_name || '::' || ftv.char_name AS scoped_charid,
        ftv.class_type AS class_type,
        'TEXT' AS char_type,
        NULL AS decimal_precision,
        CAST(ftv.int_counter_value AS INTEGER) AS int_counter_value,
        ftv.text_value AS text_value,
        NULL AS integer_value,
        NULL AS decimal_value,
    FROM vw_fd_text_values ftv
    """

temp_vw_cp_upload_annihilator_ddl = """
    CREATE OR REPLACE VIEW vw_cp_upload_annihilator AS
    SELECT 
        favs.equipment AS equipment,
        split_part(favs.scoped_charid, '::', 2) AS char_id,
        NULL::TEXT AS char_value,
        favs.class_type AS class_type,
        NULL::INTEGER AS code,
        '000' AS instance_counter,
        lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
        '0000' AS position,
        NULL::DECIMAL AS value_from,
        NULL::DECIMAL AS value_to,
    FROM vw_fd_all_values_scalar favs
    WHERE favs.scoped_charid IN (
        'PUMSMO::PUMS_FLOW_LITRES_PER_SEC',
        'PUMSMO::PUMS_INLET_SIZE_MM',
        'PUMSMO::PUMS_INSTALLED_DESIGN_HEAD_M',
        'PUMSMO::PUMS_RATED_SPEED_RPM',
        'PUMSMO::PUMS_RATED_CURRENT_A',
        )
"""

def temp_vw_cp_upload_assign_const_ddl(*, rated_power: str, outlet_size_mm: str) -> str:
    return f"""
CREATE OR REPLACE VIEW vw_cp_upload_assign_const AS
SELECT 
    wl.entity_id AS equipment,
    'INSULATION_CLASS_DEG_C' AS char_id,
    '180 (CLASS H)' AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    0 AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::INSULATION_CLASS_DEG_C' 
WHERE wl.class_name = 'PUMSMO' 
UNION
SELECT 
    wl.entity_id AS equipment,
    'IP_RATING' AS char_value,
    '68' AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    0 AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::IP_RATING' 
WHERE wl.class_name = 'PUMSMO'  
UNION
SELECT 
    wl.entity_id AS equipment,
    'PUMS_RATED_VOLTAGE' AS char_value,
    NULL AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    400 AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::PUMS_RATED_VOLTAGE' 
WHERE wl.class_name = 'PUMSMO'  
UNION
SELECT 
    wl.entity_id AS equipment,
    'PUMS_RATED_VOLTAGE_UNITS' AS char_value,
    'VAC' AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    0 AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::PUMS_RATED_VOLTAGE_UNITS' 
WHERE wl.class_name = 'PUMSMO'  
UNION
SELECT 
    wl.entity_id AS equipment,
    'PUMS_OUTLET_SIZE_MM' AS char_value,
    NULL AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    {outlet_size_mm} AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::PUMS_OUTLET_SIZE_MM' 
WHERE wl.class_name = 'PUMSMO' 
UNION
SELECT 
    wl.entity_id AS equipment,
    'PUMS_RATED_POWER_KW' AS char_value,
    NULL AS char_value,
    '002' AS class_type,
    1 AS code,
    '000' AS instance_counter,
    lpad(IF(favs.int_counter_value IS NULL, '1', favs.int_counter_value::TEXT), 3, '0') AS int_counter_value,
    '0000' AS position,
    {rated_power} AS value_from,
    0 AS value_to,
FROM s4_fd_classes wl
LEFT OUTER JOIN vw_fd_all_values_scalar favs ON wl.entity_id = favs.equipment AND favs.scoped_charid = 'PUMSMO::PUMS_RATED_POWER_KW' 
WHERE wl.class_name = 'PUMSMO'  
"""
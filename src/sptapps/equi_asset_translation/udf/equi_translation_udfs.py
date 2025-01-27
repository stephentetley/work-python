"""
Copyright 2025 Stephen Tetley

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

import duckdb
from duckdb.typing import *
import duckdb.typing
import duckdb.typing


def register_functions(con: duckdb.DuckDBPyConnection) -> None:
    con.create_function("udf_format_signal3", udf_format_signal3, [VARCHAR, VARCHAR, VARCHAR], VARCHAR)
    con.create_function("udf_power_to_killowatts", udf_power_to_killowatts, [VARCHAR, VARCHAR], DOUBLE, exception_handling="return_null")


# TODO signal_min and signal_max should be numeric once we have typing in the AI2 eav
def udf_format_signal3(signal_min: str, signal_max: str, signal_unit: str) -> str:
    return f'{signal_min} - {signal_max} {signal_unit.upper()}'

# TODO power_value should be a float (DECIMAL) with AI2 typing
def udf_power_to_killowatts(power_units: str, power_value: str) -> float | None:
    match power_units.upper():
        case 'KILOWATTS' | 'KW':
            return float(power_value)
        case 'WATTS' | 'W':
            return float(power_value) * 1000.0
        case _e :
            print(_e)
            raise ValueError("ERROR")

# CREATE OR REPLACE MACRO equi_asset_translation.format_output_type(outtype) AS (
#     CASE 
#         WHEN upper(outtype) = 'DIGITAL' THEN 'DIGITAL'
#         WHEN upper(outtype) = 'MA' THEN 'ANALOGUE'
#         WHEN upper(outtype) = 'MV' THEN 'ANALOGUE' 
#     END
# );


# CREATE OR REPLACE MACRO equi_asset_translation.voltage_ac_or_dc(ac_or_dc) AS (
#     CASE 
#         WHEN upper(ac_or_dc) = 'DIRECT CURRENT' THEN 'VDC' 
#         WHEN upper(ac_or_dc) = 'ALTERNATING CURRENT' THEN 'VAC'
#         ELSE NULL
#     END
# );


# CREATE OR REPLACE MACRO equi_asset_translation.size_to_millimetres(size_units, size_value) AS (
#     CASE 
#         WHEN upper(size_units) = 'MILLIMETRES' THEN round(size_value, 0)
#         WHEN upper(size_units) = 'CENTIMETRES' THEN round(size_value  * 10, 0) 
#         WHEN upper(size_units) = 'METRES' THEN round(size_value  * 1000, 0) 
#         WHEN upper(size_units) = 'INCH' THEN round(size_value * 25.4, 0) 
#         ELSE NULL
#     END
# );



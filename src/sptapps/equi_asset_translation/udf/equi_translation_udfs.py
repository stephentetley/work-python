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
from numbers import Number

import duckdb
from duckdb.typing import *
import duckdb.typing
import duckdb.typing
import duckdb.typing
import duckdb.typing


def register_functions(con: duckdb.DuckDBPyConnection) -> None:
    con.create_function("udf_format_signal3", udf_format_signal3, [DOUBLE, DOUBLE, VARCHAR], VARCHAR)
    
    con.create_function("udf_format_as_integer_string", udf_format_as_integer_string, [DOUBLE], VARCHAR, exception_handling="return_null")
    con.create_function("udf_power_to_killowatts", udf_power_to_killowatts, [VARCHAR, DOUBLE], DOUBLE, exception_handling="return_null")
    con.create_function("udf_format_output_type", udf_format_output_type, [VARCHAR], VARCHAR, exception_handling="return_null")
    con.create_function("udf_voltage_ac_or_dc", udf_voltage_ac_or_dc, [VARCHAR], VARCHAR, exception_handling="return_null")
    con.create_function("udf_size_to_millimetres", udf_size_to_millimetres, [VARCHAR, DOUBLE], INTEGER, exception_handling="return_null")

def udf_format_as_integer_string(num_val: Number) -> str:
    return f'{round(num_val)}'

def udf_format_signal3(signal_min: Number, signal_max: Number, signal_unit: str) -> str:
    return f'{round(signal_min)} - {round(signal_max)} {signal_unit.upper()}'


# For kVA uses a power factor of
def udf_power_to_killowatts(power_units: str, power_value: Number) -> float | None:
    match power_units.upper():
        case 'KILOWATTS' | 'KW':
            return float(power_value)
        case 'WATTS' | 'W':
            return float(power_value) * 1000.0
        case 'KILOVOLT AMP':
            kva_power_factor = 80.0
            return float(power_value) * kva_power_factor
        case _:
            raise ValueError("ERROR")

def udf_format_output_type(output_type: str) -> str: 
    match output_type.upper():
        case 'DIGITAL':
            return 'DIGITAL'
        case 'MA'| 'MV':
            return 'ANALOGUE' 
        case _:
            raise ValueError("ERROR")

def udf_voltage_ac_or_dc(ac_or_dc: str) -> str:
    match ac_or_dc.upper():
        case 'DIRECT CURRENT': 
            return 'VDC' 
        case 'ALTERNATING CURRENT':
            return 'VAC'
        case _:
            raise ValueError("ERROR")

def udf_size_to_millimetres(size_units: str, size_value: Number) -> int:
    match size_units.upper():
        case 'MILLIMETRES' | 'MM':
            return int(size_value)
        case 'CENTIMETRES' | 'CM':
            return round(size_value * 10.0) 
        case 'METRES' | 'M':
            return round(size_value  * 1000.0) 
        case 'INCH':
            return round(size_value * 25.4)
        case _:
            raise ValueError("ERROR")




"""
Copyright 2024 Stephen Tetley

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

import polars as pl
import duckdb


def get_pivot_table(*, equipment_name: str, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    pivot_query = """
        SELECT 
            md.ai2_reference AS ai2_reference, 
            pv.* EXCLUDE (ai2_reference)
        FROM
            ai2_export.master_data md
        JOIN (PIVOT ai2_export.eav_data ON attribute_name USING first(attribute_value) GROUP BY ai2_reference) pv ON pv.ai2_reference = md.ai2_reference 
        WHERE 
            md.common_name LIKE ?
        """
    param = f"%EQUIPMENT: {equipment_name}"
    df = con.execute(pivot_query, [param]).pl()
    return df

def assert_pivot_columns(colnames: list[str], df:pl.DataFrame) -> pl.DataFrame:
    cols = df.columns
    df1 = df
    for name in colnames:
        if not name in cols:
            df1 = df1.with_columns(pl.lit("").alias(name))
    return df1
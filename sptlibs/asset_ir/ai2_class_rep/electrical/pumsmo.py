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

import duckdb
import polars as pl
import sptlibs.asset_ir.ai2_class_rep.utils as utils
import sptlibs.asset_ir.class_rep.gen_table as gen_table

def create_pumsmo_table(*, con: duckdb.DuckDBPyConnection) -> None: 
    gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='PUMSMO', con=con)

# TODO - not enough to rely on 'SUBMERSIBLE CENTRIFUGAL PUMP', should be 
# EAV:Integral Motor Y/N = 'YES' as well
def ingest_pumsmo_eav_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    utils.ingest_equipment_eav_data(
        pivot_table_getter=utils.simple_pivot_getter(equipment_ai2_name='SUBMERSIBLE CENTRIFUGAL PUMP'), 
        equipment_ai2_column_names=pivot_columns, 
        extract_trafo=extract_pumsmo_chars, 
        insert_stmt=pumsmo_insert_stmt,
        df_view_name='df_pumsmo_vw',
        con=con)


pivot_columns = [ 
    "impeller_type", 
    "location_on_site",
    ]


def extract_pumsmo_chars(df: pl.DataFrame) -> pl.DataFrame: 
    return df.select([ 
        (pl.col("ai2_reference").alias("equi_id")),
        (pl.lit("").alias("uniclass_code")),
        (pl.lit("").alias("uniclass_desc")),
        (pl.col("impeller_type").alias("pums_impeller_type")),
        (pl.col("location_on_site").alias("location_on_site")),
        ])




pumsmo_insert_stmt = """
    INSERT INTO ai2_class_rep.equi_pumsmo BY NAME
    SELECT 
        df.equi_id AS equi_id,
        df.location_on_site AS location_on_site,
        df.pums_impeller_type AS pums_impeller_type,
    FROM df_pumsmo_vw AS df
    ON CONFLICT DO NOTHING;
    """

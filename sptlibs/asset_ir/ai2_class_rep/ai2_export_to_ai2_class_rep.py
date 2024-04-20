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
import sptlibs.gridref as gridref
import sptlibs.asset_ir.ai2_class_rep.duckdb_setup as duckdb_setup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    duckdb_setup.setup_tables(con=con)

# DB must have `ai2_export` tables set up
def ai2_export_to_ai2_classes(*, con: duckdb.DuckDBPyConnection) -> None:
    __translate_equipment_master_data(con=con)
    __translate_memo_text_data(con=con)
    __translate_asset_condition_data(con=con)
    __translate_east_north_data(con=con)

def __translate_equipment_master_data(*, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO ai2_class_rep.equi_master_data BY NAME
        SELECT
            md.ai2_reference AS ai2_reference,
            md.common_name AS common_name,
            regexp_extract(md.common_name, '/([^/]*)/EQUIPMENT:', 1) AS equipment_name,
            regexp_extract(md.common_name, '.*EQUIPMENT: (.*)$', 1) AS equipment_type,
            md.installed_from AS installed_from,
            md.manufacturer AS manufacturer,
            md.model AS model,
            eav_specific_model.attribute_value AS specific_model_frame,
            eav_serial_num.attribute_value AS serial_number,
            md.asset_status AS asset_status,
            eav_pandi.attribute_value AS p_and_i_tag,
        FROM ai2_export.master_data md
        LEFT OUTER JOIN ai2_export.eav_data eav_serial_num ON eav_serial_num.ai2_reference = md.ai2_reference AND eav_serial_num.attribute_name = 'serial_no'
        LEFT OUTER JOIN ai2_export.eav_data eav_specific_model ON eav_specific_model.ai2_reference = md.ai2_reference AND eav_specific_model.attribute_name = 'specific_model_frame'
        LEFT OUTER JOIN ai2_export.eav_data eav_pandi ON eav_pandi.ai2_reference = md.ai2_reference AND eav_pandi.attribute_name = 'p_and_i_tag_no'
        WHERE md.common_name LIKE '%EQUIPMENT:%'
        ;
    """
    con.execute(insert_stmt)

def __translate_memo_text_data(*, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO ai2_class_rep.memo_text BY NAME
        SELECT 
            md.ai2_reference AS ai2_reference,
            eav_memo1.attribute_value AS memo_line1,
            eav_memo2.attribute_value AS memo_line2,
            eav_memo3.attribute_value AS memo_line3,
            eav_memo4.attribute_value AS memo_line4,
            eav_memo5.attribute_value AS memo_line5,
        FROM ai2_export.master_data md
        LEFT OUTER JOIN ai2_export.eav_data eav_memo1 ON eav_memo1.ai2_reference = md.ai2_reference AND eav_memo1.attribute_name = 'memo_line_1'
        LEFT OUTER JOIN ai2_export.eav_data eav_memo2 ON eav_memo2.ai2_reference = md.ai2_reference AND eav_memo2.attribute_name = 'memo_line_2'
        LEFT OUTER JOIN ai2_export.eav_data eav_memo3 ON eav_memo3.ai2_reference = md.ai2_reference AND eav_memo3.attribute_name = 'memo_line_3'
        LEFT OUTER JOIN ai2_export.eav_data eav_memo4 ON eav_memo4.ai2_reference = md.ai2_reference AND eav_memo4.attribute_name = 'memo_line_4'
        LEFT OUTER JOIN ai2_export.eav_data eav_memo5 ON eav_memo5.ai2_reference = md.ai2_reference AND eav_memo5.attribute_name = 'memo_line_5'
        WHERE md.common_name LIKE '%EQUIPMENT:%'
        ;
    """
    con.execute(insert_stmt)


def __translate_asset_condition_data(*, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO ai2_class_rep.asset_condition BY NAME
        SELECT 
            md.ai2_reference AS ai2_reference, 
            eav_asset_cond1.attribute_value AS condition_grade,
            eav_asset_cond2.attribute_value AS condition_grade_reason,
            TRY_CAST(eav_asset_cond3.attribute_value AS INTEGER) AS survey_date,
        FROM ai2_export.master_data md
        LEFT OUTER JOIN ai2_export.eav_data eav_asset_cond1 ON eav_asset_cond1.ai2_reference = md.ai2_reference AND eav_asset_cond1.attribute_name = 'condition_grade'
        LEFT OUTER JOIN ai2_export.eav_data eav_asset_cond2 ON eav_asset_cond1.ai2_reference = md.ai2_reference AND eav_asset_cond1.attribute_name = 'condition_grade_reason'
        LEFT OUTER JOIN ai2_export.eav_data eav_asset_cond3 ON eav_asset_cond1.ai2_reference = md.ai2_reference AND eav_asset_cond1.attribute_name = 'agasp_survey_year'
        WHERE md.common_name LIKE '%EQUIPMENT:%'
        ;
    """
    con.execute(insert_stmt)

# This is effectively using a UDF...
def __translate_east_north_data(*, con: duckdb.DuckDBPyConnection) -> None:
    select_stmt = """
        SELECT 
            md.ai2_reference AS ai2_reference, 
            eav_east_north.attribute_value AS grid_ref,
        FROM ai2_export.master_data md
        LEFT OUTER JOIN ai2_export.eav_data eav_east_north ON eav_east_north.ai2_reference = md.ai2_reference AND eav_east_north.attribute_name = 'loc_ref'
        WHERE md.common_name LIKE '%EQUIPMENT:%'
        ;
    """
    df = con.execute(select_stmt).pl()
    df = df.with_columns(easting = pl.col("grid_ref").apply(lambda gr: gridref.to_east_north(gr)[0], return_dtype=pl.Int64), 
                         northing = pl.col("grid_ref").apply(lambda gr: gridref.to_east_north(gr)[1], return_dtype=pl.Int64))
    insert_stmt = """
        INSERT INTO ai2_class_rep.east_north BY NAME
        SELECT 
            df.ai2_reference AS ai2_reference,
            df.grid_ref AS grid_ref,
            df.easting AS easting,
            df.northing AS northing,
        FROM 
            df_dataframe_view df
        ;
        """
    con.register(view_name='df_dataframe_view', python_object=df)
    con.execute(insert_stmt)
    con.commit()


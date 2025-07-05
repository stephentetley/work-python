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

import shutil
import duckdb
import pandas as pd
from sptlibs.utils.sql_script_runner import SqlScriptRunner

# Pandas (rather than polars) is necessary so we can use `openpyxl` and 
# be able to write into existing Excel files which have formatting we
# need to follow.


def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='setup_equi_change_tables.sql')
    
def write_excel_upload(*,
                       upload_template_path: str, 
                       dest: str,
                       con: duckdb.DuckDBPyConnection) -> None: 
    shutil.copy(upload_template_path, dest)
    with pd.ExcelWriter(dest, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        header_pandas = con.sql("SELECT * FROM excel_uploader_equi_change.vw_change_request_header;").df()
        header_pandas.to_excel(
            writer,
            sheet_name='Change Request Header',
            startcol=0,
            startrow=5,
            index=False,
            header=False,
        )
        notes_pandas = con.sql("SELECT * FROM excel_uploader_equi_change.change_request_notes;").df()
        notes_pandas.to_excel(
            writer,
            sheet_name='Change Request Notes',
            startcol=0,
            startrow=5,
            index=False,
            header=False,
        )        
        flocs_pandas = con.sql("SELECT * FROM excel_uploader_equi_change.vw_equipment_data;").df()
        flocs_pandas.to_excel(
            writer,
            sheet_name='EQ-Equipment Data',
            startcol=0,
            startrow=5,
            index=False,
            header=False,
        )
        flocs_chars_pandas = con.sql("SELECT * FROM excel_uploader_equi_change.vw_classification;").df()
        flocs_chars_pandas.to_excel(
            writer,
            sheet_name='EQ-Classification',
            startcol=0,
            startrow=5,
            index=False,
            header=False,
        )


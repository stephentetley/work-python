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

import os
import duckdb
from typing import NamedTuple
import xlsxwriter
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.data_access.import_utils as import_utils
import sptlibs.utils.export_utils as export_utils


# file_prefix e.g. 'cso'
# table_name, e.g. 'ims_landing.cso_assets'
class TableSource(NamedTuple):
    file_prefix: str
    table_name: str


def create_ims_files(*, ims_source_path: str, output_root: str, con: duckdb.DuckDBPyConnection) -> None: 
    _create_output_dirs(output_root)
    runner = SqlScriptRunner(__file__, con=con)
    runner.set_variable(var_name='file_source', var_value=ims_source_path)
    runner.exec_sql_file(rel_file_path='setup_landing_tables.sql')
    runner.exec_sql_file(rel_file_path='unpivot_site_create_macro.sql')
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('sps'), con=con)
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('dtk'), con=con)
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('inlet_pumping'), con=con)
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('cso'), con=con)
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('cso_storage'), con=con)
    _gen_ims_files_for_table(output_root=output_root, table_source=_make_table_source('suds'), con=con)


def _make_table_source(abbrev: str) -> TableSource:
    return TableSource(abbrev, f"ims_landing.{abbrev}_assets")

def _create_output_dirs(output_root: str) -> None:
    def create_child(name): 
        child = os.path.normpath(os.path.join(output_root, name))
        if not os.path.exists(child):
            os.mkdir(child)
    if os.path.exists(output_root):
        for name in ['sps', 'dtk', 'inlet_pumping', 'cso', 'cso_storage', 'suds']:
            create_child(name)
    
def _gen_ims_files_for_table(*, output_root:str, table_source: TableSource, con: duckdb.DuckDBPyConnection) -> None: 
    output_path = os.path.normpath(os.path.join(output_root, table_source.file_prefix))
    sel = f'SELECT "SiteReference", "InstCommonName" FROM {table_source.table_name};'
    df = con.sql(query=sel).pl()
    for row in df.rows(named=True):
        norm_common_name = import_utils.normalize_name(row['InstCommonName'])
        sai_num = row['SiteReference']
        file_name = f"{table_source.file_prefix}_{norm_common_name}_{sai_num.lower()}.xlsx"
        out_file = os.path.normpath(os.path.join(output_path, file_name))
        print(f"{out_file} ...")
        with xlsxwriter.Workbook(out_file) as workbook:
            query = f"SELECT * FROM unpivot_site('{table_source.table_name}', '{sai_num}');"
            export_utils.write_sql_query_to_excel(select_query=query, 
                                                  workbook=workbook,
                                                  con=con,
                                                  sheet_name = 'Sheet1')



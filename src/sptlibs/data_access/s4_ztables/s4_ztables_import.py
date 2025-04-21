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
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_import(*, sources: list[XlsxSource], con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ztables_create_tables.sql')
    runner.exec_sql_file(rel_file_path='ztables_create_macros.sql')
    for idx, source in enumerate(sources):
        load_stmt = _make_load_landing_table(idx=idx, source=source)
        con.execute(load_stmt)
    df = con.execute("SELECT ztable_name, landing_table FROM s4_ztables_landing.vw_table_information;").pl()
    for row in df.rows(named=True):
        ztable_name = row['ztable_name']
        landing_table = row['landing_table']
        insert_stmt = _make_insert_into_query(ztable_name=ztable_name, landing_table=landing_table)
        con.execute(insert_stmt)


def _make_load_landing_table(*, idx: str, source:XlsxSource) -> str:
    return f"""
            CREATE OR REPLACE TABLE s4_ztables_landing.ztable{idx+1} AS
            SELECT 
                *
            FROM read_xlsx('{source.path}');
        """


def _make_insert_into_query(*, ztable_name: str, landing_table:str) -> str:
    return f"""
            INSERT INTO s4_ztables.{ztable_name}
            SELECT * FROM get_{ztable_name}_table_data('s4_ztables_landing.{landing_table}');
        """


def copy_ztable_tables(*, source_db_path: str, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # copy tables using duckdb builtins
    import_utils.duckdb_import_tables_from_duckdb(source_db_path=source_db_path, 
                                                  con=dest_con,
                                                  schema_name='s4_ztables',
                                                  create_schema=True,
                                                  source_tables=['s4_ztables.eqobj',
                                                                 's4_ztables.flobjl', 
                                                                 's4_ztables.flocdes',
                                                                 's4_ztables.manuf_model',
                                                                 's4_ztables.obj'])

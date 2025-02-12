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

# This was a quick hack - it is expected to bitrot as the need for it has passed

import duckdb
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    s4_classlists_import.copy_standard_classlists_tables(con=con)
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='pdt_raw_data_create_tables.sql')
    runner.exec_sql_file(rel_file_path='pdt_class_rep_create_tables.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_pdt_class_rep_tables.sql')


def duckdb_build_class_rep(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='pdt_class_rep_insert_into.sql')

def duckdb_build_equiclass_summary_views(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner2(__file__, con=con)
    df = con.execute("""
        SELECT lower(t.class_name) AS class_name FROM s4_classlists.vw_equi_class_defs t WHERE t.is_object_class=true;
        """
    ).pl()
    for row in df.iter_rows(named=True):
        args = {'class_name': row['class_name']}
        runner.exec_jinja_sql_file(args=args, rel_file_path='pdt_class_rep/gen_pdt_class_rep_equisummary_views.sql.jinja')
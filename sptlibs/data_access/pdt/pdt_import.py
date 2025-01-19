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
import sptlibs.data_access.s4_classlists.duckdb_import as classlist_duckdb_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    classlist_duckdb_import.copy_standard_classlists_tables(con=con)
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='pdt_raw_data/pdt_raw_data_create_tables.sql', con=con)
    runner.exec_sql_file(file_rel_path='pdt_class_rep/pdt_class_rep_create_tables.sql', con=con)
    runner.exec_sql_generating_file(file_rel_path='pdt_class_rep/gen_pdt_class_rep_tables.sql', con=con)


def duckdb_build_class_rep(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='pdt_class_rep/pdt_class_rep_insert_into.sql', con=con)

def duckdb_build_equiclass_summary_views(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    df = con.execute("""
        SELECT lower(t.class_name) AS class_name FROM s4_classlists.vw_equi_class_defs t WHERE t.is_object_class=true;
        """
    ).pl()
    for row in df.iter_rows(named=True):
        args = {'class_name': row['class_name']}
        runner.exec_jinja_sql_file(args=args, file_rel_path='pdt_class_rep/gen_pdt_class_rep_equisummary_views.sql.jinja', con=con)
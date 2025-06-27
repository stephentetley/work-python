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
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import

# 


# must have s4_classlists setup
def duckdb_init_s4_classrep(*,
                            s4_classlists_db_path: str,
                            equi_class_tables: list[str],
                            floc_class_tables: list[str],
                            con: duckdb.DuckDBPyConnection) -> None:
    s4_classlists_import.copy_classlists_tables(source_db_path=s4_classlists_db_path, dest_con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='s4_classrep_create_tables.sql')
    # equi...
    if not floc_class_tables:
        query = """
            INSERT INTO s4_classrep.floc_classes_used 
            SELECT DISTINCT ON (class_name) 
                t.class_name
            FROM s4_classlists.vw_floc_class_defs t;
        """
        con.execute(query)
    else:
        con.execute('BEGIN TRANSACTION;')
        for name in equi_class_tables:
            query = f"INSERT INTO s4_classrep.floc_classes_used VALUES ('{name}');"
            print
            con.execute(query)
        con.execute('COMMIT;')    
    runner.exec_sql_generating_file(rel_file_path='gen_create_flocclass_tables.sql')
    # equi...
    if not equi_class_tables:
        query = """
            INSERT INTO s4_classrep.equi_classes_used 
            SELECT DISTINCT ON (class_name) 
                t.class_name
            FROM s4_classlists.vw_equi_class_defs t;
        """
        con.execute(query)
    else:
        con.execute('BEGIN TRANSACTION;')
        for name in equi_class_tables:
            query = f"INSERT INTO s4_classrep.equi_classes_used VALUES ('{name}');"
            print
            con.execute(query)
        con.execute('COMMIT;')        
    runner.exec_sql_generating_file(rel_file_path='gen_create_equiclass_tables.sql')
    runner.exec_sql_generating_file(rel_file_path='s4_classrep_create_views.sql')

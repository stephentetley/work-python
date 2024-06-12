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
from jinja2 import Template
import sptlibs.data_import.s4_classlists.duckdb_import as classlist_duckdb_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    classlist_duckdb_import.copy_standard_classlists_tables(con=con)
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='pdt_raw_data/pdt_raw_data_create_tables.sql', con=con)
    runner.exec_sql_file(file_rel_path='pdt_class_rep/pdt_class_rep_create_tables.sql', con=con)

def build_class_rep(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='pdt_class_rep/pdt_class_rep_insert_into.sql', con=con)

def build_equiclass_summary_views(*, con: duckdb.DuckDBPyConnection) -> None: 
    df = con.execute("""
        SELECT lower(t.class_name) AS class_name FROM s4_classlists.vw_equi_class_defs t WHERE t.is_object_class=true;
        """
    ).pl()
    for row in df.iter_rows(named=True):
        # TODO this shouldn't use try-continue...
        try: 
            con.execute(Template(_vw_equisummary_template).render(class_name=row['class_name']))
        except:
            continue




_vw_equisummary_template = """
    CREATE OR REPLACE VIEW pdt_class_rep.vw_equisummary_{{class_name}} AS
    SELECT 
        emd.source_file AS source_file,
        emd.equi_name AS equi_name,
        emd.manufacturer AS manufacturer,
        emd.model AS model,
        emd.specific_model_frame AS specific_model_frame,
        emd.serial_number AS serial_number,
        ec.* EXCLUDE (equi_name, source_file),
    FROM pdt_class_rep.equiclass_{{class_name}} ec
    JOIN pdt_class_rep.equi_master_data emd ON emd.equi_name = ec.equi_name AND emd.source_file = ec.source_file;
"""
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

import os
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
import sptlibs.data_access.import_utils as import_utils

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ai2_export_create_tables.sql')


def duckdb_import_landing_files(*, sources: list[str], con: duckdb.DuckDBPyConnection) -> None: 
    for idx, path in enumerate(sources):
        file_name = os.path.basename(path)
        query = f"""
            INSERT INTO ai2_export_landing.landing_files BY NAME
            SELECT 
                'ai2_export_landing.export{idx+1}' AS qualified_table_name,
                '{file_name}' AS file_name,
                '{path}' AS file_path;
            """
        con.execute(query)
    _import_landing_tables(con=con)
    _setup_floc_master_data(con=con)
    _setup_floc_eav_data(con=con)
    _setup_equi_master_data(con=con)
    _setup_equi_eav_data(con=con)

def _import_landing_tables(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT * FROM ai2_export_landing.landing_files;"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        qualified_table_name = row['qualified_table_name']
        ai2_file_path = row['file_path']
        excel_table_import.duckdb_import(xls_path=ai2_file_path,
                                         table_name=qualified_table_name,
                                         sheet_name='Sheet1',
                                         con=con)

def _setup_equi_master_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT t.qualified_table_name FROM ai2_export_landing.landing_files t;"
    df = con.execute(query).pl()
    tables = [row['qualified_table_name'] for row in df.rows(named=True)]        
    selects = [f"SELECT * FROM extract_ai2_equi_data_from_raw('{t}')" for t in tables]
    body = "\nUNION BY NAME\n".join(selects)
    query = f"""
        INSERT INTO ai2_export.equi_master_data
        {body};
    """
    con.execute(query)

def _setup_floc_master_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT t.qualified_table_name FROM ai2_export_landing.landing_files t;"
    df = con.execute(query).pl()
    tables = [row['qualified_table_name'] for row in df.rows(named=True)]        
    selects = [f"SELECT * FROM extract_ai2_floc_data_from_raw('{t}')" for t in tables]
    body = "\nUNION BY NAME\n".join(selects)
    query = f"""
        INSERT INTO ai2_export.floc_master_data
        {body};
    """
    con.execute(query)

def _setup_equi_eav_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT t.qualified_table_name FROM ai2_export_landing.landing_files t;"
    df = con.execute(query).pl()
    tables = [row['qualified_table_name'] for row in df.rows(named=True)]        
    selects = [f"SELECT * FROM extract_ai2_equi_eav_data_from_raw('{t}')" for t in tables]
    body = "\nUNION BY NAME\n".join(selects)
    query = f"""
    INSERT INTO ai2_export.equi_eav_data
        {body};
    """
    con.execute(query)

def _setup_floc_eav_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT t.qualified_table_name FROM ai2_export_landing.landing_files t;"
    df = con.execute(query).pl()
    tables = [row['qualified_table_name'] for row in df.rows(named=True)]        
    selects = [f"SELECT * FROM extract_ai2_floc_eav_data_from_raw('{t}')" for t in tables]
    body = "\nUNION BY NAME\n".join(selects)
    query = f"""
    INSERT INTO ai2_export.floc_eav_data
        {body};
    """
    con.execute(query)


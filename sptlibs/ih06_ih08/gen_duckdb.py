"""
Copyright 2023 Stephen Tetley

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
import tempfile
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup
import sptlibs.classlist.duckdb_copy as classlist_duckdb_copy
import sptlibs.ih06_ih08.duckdb_setup as duckdb_setup
import sptlibs.ih06_ih08.load_raw_xlsx as load_raw_xlsx
import sptlibs.ih06_ih08.unpivot_classes as unpivot_classes
import sptlibs.ih06_ih08.summary_report as summary_report
    
class GenDuckdb:
    def __init__(self) -> None:
        self.db_name = 'ih06_ih08.duckdb'
        self.output_directory = tempfile.gettempdir()
        self.xlsx_ih06_imports = []
        self.xlsx_ih08_imports = []
        self.ddl_stmts = ['CREATE SCHEMA IF NOT EXISTS s4_classlists;',
                            classlist_duckdb_setup.s4_characteristic_defs_ddl,
                            classlist_duckdb_setup.s4_enum_defs_ddl, 
                            duckdb_setup.s4_ih_funcloc_masterdata_ddl,
                            duckdb_setup.s4_ih_equipment_masterdata_ddl,
                            duckdb_setup.s4_ih_classes_ddl,
                            duckdb_setup.s4_ih_char_values_ddl,
                            duckdb_setup.vw_s4_classes_used_ddl,
                            duckdb_setup.vw_characteristic_defs_with_type_ddl,
                            duckdb_setup.vw_s4_charateristics_used_ddl
                            ]
        self.copy_tables_stmts = [duckdb_setup.s4_ih_equipment_masterdata_insert, 
                                  duckdb_setup.s4_ih_funcloc_masterdata_insert ]
        self.xlsx_output_name = 'ih_summary.xlsx'

    def set_output_directory(self, *, output_directory: str) -> None: 
        self.output_directory = output_directory

    def set_db_name(self, *, db_name: str) -> None:
        '''Just the name, not the path.'''
        self.db_name = db_name

    def set_output_report_name(self, *, xlsx_name: str) -> None:
        """Just the file name, not the directory."""
        self.xlsx_output_name = xlsx_name

    def add_ih06_export(self, src: XlsxSource) -> None:
        self.xlsx_ih06_imports.append(src)

    def add_ih08_export(self, src: XlsxSource) -> None:
        self.xlsx_ih08_imports.append(src)

    def add_classlist_tables(self, *, classlists_duckdb_path: str) -> None:
        self.copy_tables_stmts.append(classlist_duckdb_copy.s4_classlists_table_copy(classlists_duckdb_path=classlists_duckdb_path))

    def gen_duckdb(self) -> str:
        ''''''
        duckdb_outpath = os.path.normpath(os.path.join(self.output_directory, self.db_name))
        try:
            os.remove(duckdb_outpath)
        except OSError:
            pass
        con = duckdb.connect(database=duckdb_outpath)
        # Setup tables
        for stmt in self.ddl_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
        # TODO properly account for multiple sheets / appending data
        # flocs
        for src in self.xlsx_ih06_imports:
            # equi and valuaequi tables
            load_raw_xlsx.load_ih06(xlsx_src=src, con=con)
        # equi
        for src in self.xlsx_ih08_imports:
            # equi and valuaequi tables
            load_raw_xlsx.load_ih08(xlsx_src=src, con=con)
        
        for stmt in self.copy_tables_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue

        unpivot_classes.make_ih_char_and_classes(con=con)
        # Output xlsx (new db connection)...
        output_xls = os.path.normpath(os.path.join(self.output_directory, self.xlsx_output_name))
        summary_report.make_summary_report(output_xls=output_xls, con=con)
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath


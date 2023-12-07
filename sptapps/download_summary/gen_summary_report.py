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
import pandas as pd
from sptlibs.file_download.gen_duckdb import GenDuckdb
import sptapps.download_summary.duckdb_queries as duckdb_queries
import sptapps.download_summary.duckdb_setup as duckdb_setup
import sptapps.download_summary.df_transforms as df_transforms
import sptapps.download_summary.make_summary_report as make_summary_report
from sptlibs.data_frame_xlsx_table import DataFrameXlsxTable

# TODO add class specific rewrites, e.g. separating PLI and SAI in AI2_REFERENCE

class GenSummaryReport:
    def __init__(self) -> None:
        self.source_directories = []
        self.glob_pattern = '*download.txt'
        self.output_directory = tempfile.gettempdir()
        self.classlists_db_path = None
        self.duckdb_output_name = 'file_downloads2.duckdb'
        self.xlsx_output_name = 'downloads_summary.xlsx'

    def set_output_directory(self, *, output_directory: str) -> None:
        self.output_directory = output_directory

    def set_classlists_db_path(self, *, classlists_db_path: str) -> None:
        self.classlists_db_path = classlists_db_path

    def add_downloads_source_directory(self, *, src_dir: str, glob_pattern: str) -> None:
        self.source_directories.append((src_dir, glob_pattern))


    def set_duckdb_output_name(self, *, duckdb_output_name: str) -> None:
        """Just the file name, not the directory."""
        self.duckdb_output_name = duckdb_output_name

    def set_output_report_name(self, *, xlsx_name: str) -> None:
        """Just the file name, not the directory."""
        self.xlsx_output_name = xlsx_name

    def gen_summary_report(self) -> str:
        try: 
            genduckdb = GenDuckdb()
            genduckdb.set_output_directory(output_directory=self.output_directory)
            genduckdb.db_name = self.duckdb_output_name
            for (dir, glob) in self.source_directories: 
                genduckdb.add_file_downloads_in_directory(path=dir, glob_pattern=glob)
            genduckdb.add_classlist_tables(classlists_duckdb_path=self.classlists_db_path)
            duckdb_path = genduckdb.gen_duckdb()
            
            # Output xlsx (new db connection)...
            output_xls = os.path.normpath(os.path.join(self.output_directory, self.xlsx_output_name))
            con = duckdb.connect(duckdb_path)
            con.execute(duckdb_setup.vw_floc_characteristics_summary_ddl)
            con.execute(duckdb_setup.vw_equi_characteristics_summary_ddl)
            with pd.ExcelWriter(output_xls) as xlwriter: 
                # floc summary
                con.execute(duckdb_queries.floc_summary_report)
                df = con.df()                
                df = df_transforms.funcloc_rewrite_floc_classes(df)
                df.to_excel(xlwriter, engine='xlsxwriter', sheet_name='funcloc_master')
                table_writer = DataFrameXlsxTable(df=df)
                table_writer.to_excel(writer=xlwriter, sheet_name='funcloc_master')
                # equi summary
                con.execute(duckdb_queries.equi_summary_report)
                df = con.df()                
                df1 = df_transforms.equipment_rewrite_equi_classes(df)
                table_writer = DataFrameXlsxTable(df=df1)
                table_writer.to_excel(writer=xlwriter, sheet_name='equipment_master')
                # floc tabs
                con.execute(duckdb_queries.get_floc_classes_used_query)
                for (class_type, class_name) in con.fetchall():
                    tab_name = make_summary_report.make_class_tab_name(class_type=class_type, class_name=class_name)
                    print(tab_name)
                    con.execute(query= duckdb_queries.floc_class_tab_summary_report, parameters={'class_name': class_name})
                    df2 = con.df()
                    df3 = df_transforms.class_char_rewrite_characteristics(df2)
                    table_writer = DataFrameXlsxTable(df=df3)
                    table_writer.to_excel(writer=xlwriter, sheet_name=tab_name)
                # equi tabs
                con.execute(duckdb_queries.get_equi_classes_used_query)
                for (class_type, class_name) in con.fetchall():
                    tab_name = make_summary_report.make_class_tab_name(class_type=class_type, class_name=class_name)
                    print(tab_name)
                    con.execute(query= duckdb_queries.equi_class_tab_summary_report, parameters={'class_name': class_name})
                    df2 = con.df()
                    df3 = df_transforms.class_char_rewrite_characteristics(df2)
                    df3.to_excel(xlwriter, engine='xlsxwriter', sheet_name=tab_name)
                    table_writer = DataFrameXlsxTable(df=df3)
                    table_writer.to_excel(writer=xlwriter, sheet_name=tab_name)
                con.close()
            return output_xls
        except Exception as exn:
                print('Exception!')
                print(f'exn: {str(exn)}')

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
import sptlibs.ih06_ih08.materialize_summary_tables as materialize_summary_tables
import sptlibs.s4_floc_equi_summary.summary_report as summary_report
    
class GenSummaryReport:
    def __init__(self, *, db_path: str, xlsx_output_name: str) -> None:
        self.db_path = db_path
        self.xlsx_output_name = xlsx_output_name

    def gen_summary_report(self) -> str:
        '''Output summary report.'''
        con = duckdb.connect(database=self.db_path)
        # Output xlsx
        summary_report.make_summary_report(xls_output_path=self.xlsx_output_name, func=materialize_summary_tables.materialize_summary_tables, con=con)
        con.close()
        print(f'{self.xlsx_output_name} created')
        return self.xlsx_output_name


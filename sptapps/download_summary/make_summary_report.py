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

import sptlibs.export_utils as export_utils
import sptapps.download_summary.duckdb_queries as duckdb_queries

def output_equi_summary_report(*, duckdb_path: str, csv_outpath: str) -> str:
    export_utils.output_csv_report(duckdb_path=duckdb_path, select_stmt=duckdb_queries.equi_summary_report, csv_outpath=csv_outpath)

def make_class_tab_name(*, class_type: str, class_name=str) -> str: 
    match class_type: 
        case '002':
            return 'e.%s' % class_name.lower()
        case '003':
            return 'f.%s' % class_name.lower()
        case _:
            return '%s.%s' % class_type, class_name.lower()




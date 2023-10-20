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

import duckdb


def output_csv_report(*, duckdb_path: str, select_stmt: str, csv_outpath: str) -> str:
    con = duckdb.connect(duckdb_path)
    con.sql(gen_report_stmt(select_stmt=select_stmt, csv_outpath=csv_outpath))
    con.close()
    


def gen_report_stmt(*, select_stmt: str, csv_outpath: str) -> str:
    return f"""
    COPY (
        {select_stmt}
    ) TO '{csv_outpath}' (FORMAT CSV, DELIMITER ',', HEADER)
    """


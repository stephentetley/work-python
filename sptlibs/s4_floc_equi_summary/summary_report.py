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
from typing import Callable
import sptlibs.s4_floc_equi_summary.setup_tables as setup_tables
import sptlibs.s4_floc_equi_summary.gen_report as gen_report

def make_summary_report(*, xls_output_path: str, func: Callable[[duckdb.DuckDBPyConnection], None], con: duckdb.DuckDBPyConnection) -> None:
    setup_tables.setup_tables(con=con)
    func(con)
    gen_report.gen_report(xls_output_path=xls_output_path, con=con)




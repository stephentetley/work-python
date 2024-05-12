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
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils
import sptlibs.data_import.file_download.duckdb_setup as duckdb_setup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 


def gen_duckdb(*, path: str, con: duckdb.DuckDBPyConnection) -> None:
    con = duckdb.connect(self.duckdb_output_path)
    # Create `s4_fd_raw_data` tables
    duckdb_setup.setup_tables(con=con)
    for path in self.imports:
        try:
            fd = FileDownload.from_download(path=path)
            qual_table_name = fd.gen_std_table_name(schema_name='s4_fd_raw_data')
            fd.store_to_duckdb(table_name=qual_table_name, con=con)
        except Exception as exn:
            print(exn)
            continue

    if os.path.exists(self.classlists_source): 
        classlist_duckdb_setup.setup_tables(con=con)
        classlist_duckdb_copy.copy_tables(classlists_source_db_path=self.classlists_source, con=con)
    else:
        con.close()
        raise FileNotFoundError('classlist db not found')
                
    con.close()
    print(f'{self.duckdb_output_path} created')
    return self.duckdb_output_path
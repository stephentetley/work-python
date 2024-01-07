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
import glob
import duckdb
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup
import sptlibs.classlist.duckdb_copy as classlist_duckdb_copy
import file_download.duckdb_setup as duckdb_setup
import sptlibs.file_download.load_file_download as load_file_download

class GenDuckdb:
    def __init__(self, *, classlists_duckdb_path: str, duckdb_output_path: str) -> None:
        self.classlists_source = classlists_duckdb_path
        self.duckdb_output_path = duckdb_output_path
        self.imports = []


    def add_file_download(self, *, path: str) -> None:
        self.imports.append((path))

    def add_downloads_source_directory(self, *, source_dir: str, glob_pattern: str) -> None:
        globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
        for file_name in globlist: 
            self.imports.append(os.path.join(source_dir, file_name))

    def gen_duckdb(self) -> str:
        con = duckdb.connect(self.duckdb_output_path)
        # Create `s4_fd_raw_data` tables
        duckdb_setup.setup_tables(con=con)
        for path in self.imports:
            try:
                load_file_download.load_file_download(path=path, con=con)
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
    



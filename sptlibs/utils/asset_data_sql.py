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

class AssetDataSql:
    def __init__(self) -> None:
        """Throws exception if $ASSET_DATA_SQL_DIR is missing."""
        sql_root_dir = os.environ.get('ASSET_DATA_SQL_DIR', '')
        if sql_root_dir:
            if os.path.exists(sql_root_dir):
                self.sql_root_dir = sql_root_dir
            else: 
                print("Cannot find $ASSET_DATA_SQL_DIR") 
                raise NotADirectoryError(f"Cannot find $ASSET_DATA_SQL_DIR = `{sql_root_dir}`")
        else:
            print("Environment Variable $ASSET_DATA_SQL_DIR not set")
            raise OSError("Environment Variable $ASSET_DATA_SQL_DIR not set")

        
    def exec_sql_file(self, *, file_rel_path: str, con: duckdb.DuckDBPyConnection) -> None:
        sql_file_path = os.path.normpath(os.path.join(self.sql_root_dir, file_rel_path))
        if os.path.exists(sql_file_path):
            with open(sql_file_path) as file:
                statements = file.read()
                con.execute(statements)
        else: 
            return None



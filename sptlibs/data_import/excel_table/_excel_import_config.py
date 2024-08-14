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
import tomllib


class _ExcelImportConfig:
    def __init__(self, *, toml_path: str) -> None:
        self.config = None
        self.focus = None
        if os.path.exists(toml_path): 
            with open(toml_path) as fileObj:
                txt = fileObj.read()
                self.config = tomllib.loads(txt)
                self.focus = self.config.get('sheet_import', {})
        else:
            print(f"Cannot find `{toml_path}`")


    def get_excel_tab_name(self, *, alt: str) -> str | None: 
        return self.focus.get('excel_tab_name', alt)
        
    def get_schema_name(self) -> str | None:
        return self.focus.get('schema_name', None)
    
    def get_qualified_tablename(self) -> str | None:
        s1 = self.focus.get('schema_name', None)
        s2 = self.focus.get('table_name', None)
        if s1 and s2:
            return f'{s1}.{s2}'
        else: 
            return None

    
    def get_db_output_path(self, output_dir: str) -> str | None:
        s1 = self.focus.get('duckdb_name', None)
        if s1 and os.path.exists(output_dir):
            return os.path.normpath(os.path.join(output_dir, s1))
        else:
            return None




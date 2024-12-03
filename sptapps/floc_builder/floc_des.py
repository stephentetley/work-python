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
import polars as pl


class FlocDes:
    def __init__(self, source_path: str) -> None:
        if os.path.exists(source_path):
            self.lookups = _read_xlsx_source(source_path=source_path)
        else:
            self.lookups = {}
    
    def get_description(self, obj_type: str) -> str | None:
        return self.lookups[obj_type]

def _read_xlsx_source(source_path: str) -> dict[str, str]:
    df = pl.read_excel(source=source_path, sheet_name='Sheet1', engine='calamine', drop_empty_rows=True)
    return {row[0]: row[1] for row in df.iter_rows()}
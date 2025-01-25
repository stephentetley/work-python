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
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
import sptlibs.data_access.ai2_metadata.ai2_metadata_import as ai2_metadata_import
from sptlibs.utils.xlsx_source import XlsxSource


def setup_equi_translation(*, con: duckdb.DuckDBPyConnection,
                           s4_classlists_db_source: str, 
                           ai2_equipment_attributes_source) -> None:
    s4_classlists_import.copy_classlists_tables(classlists_source_db_path=s4_classlists_db_source, 
                                            setup_tables=True, 
                                            dest_con=con)
    ai2_metadata_import.duckdb_import(equipment_attributes_source=ai2_equipment_attributes_source,
                                      con=con)
    
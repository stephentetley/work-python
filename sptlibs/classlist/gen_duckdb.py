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
import tempfile
import duckdb
import pandas as pd
import sptlibs.classlist.classlist_parser as classlist_parser
import sptlibs.classlist.duckdb_setup as duckdb_setup

    
class GenDuckdb:
    def __init__(self, *, floc_classlist_path: str, equi_classlist_path: str) -> None:
        self.db_name = 'classlists.duckdb'
        self.output_directory = tempfile.gettempdir()
        self.floc_classlist_path = floc_classlist_path
        self.equi_classlist_path = equi_classlist_path

    def set_output_directory(self, *, output_directory: str) -> None: 
        self.output_directory = output_directory

    def set_db_name(self, *, db_name: str) -> None: 
        self.db_name = db_name


    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.normpath(os.path.join(self.output_directory, self.db_name))
        con = duckdb.connect(database=duckdb_outpath)
        duckdb_setup.setup_tables(con=con)
        dict_flocs = classlist_parser.parse_floc_classfile(self.floc_classlist_path)
        dict_equis = classlist_parser.parse_equi_classfile(self.equi_classlist_path)
        # characteristics...
        df_chars = pd.concat(objs=[dict_flocs['characteristics'], dict_equis['characteristics']], ignore_index=True)
        con.register(view_name='vw_df_chars', python_object=df_chars)
        con.execute(duckdb_setup.df_s4_characteristic_defs_insert(dataframe_view='vw_df_chars'))
        # characteristic enums...
        df_enums = pd.concat(objs=[dict_flocs['enum_values'], dict_equis['enum_values']], ignore_index=True)
        con.register(view_name='vw_df_enums', python_object=df_enums)
        con.execute(duckdb_setup.df_s4_enum_defs_insert(dataframe_view='vw_df_enums'))
        con.close()
        return duckdb_outpath


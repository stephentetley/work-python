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
import pandas as pd
import sptlibs.classlist.classlist_parser as classlist_parser
import sptlibs.classlist.duckdb_setup as duckdb_setup

    
class GenDuckdb:
    def __init__(self, *, floc_classlist_path: str, equi_classlist_path: str, duckdb_output_path: str) -> None:
        self.duckdb_output_path = duckdb_output_path
        self.floc_classlist_path = floc_classlist_path
        self.equi_classlist_path = equi_classlist_path


    def gen_duckdb(self) -> str:
        con = duckdb.connect(database=self.duckdb_output_path)
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
        print(f'{self.duckdb_output_path} created')
        return self.duckdb_output_path


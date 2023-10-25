# temp_parse_classlist.py

import os
import sptlibs.classlist.classlist_parser as classlist_parser
from sptlibs.classlist.gen_sqlite import GenSqlite

source = 'g:/work/2023/classlist/003-floc-classlist.txt'
output_directory = 'g:/work/2023/classlist'

if os.path.exists(source): 
    cf1 = classlist_parser.parse_classsfile(source)
    chars = cf1['characteristics']
    print(chars)
    vals = cf1['enum_values']
    print(vals)

    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.add_classlist(source)
    gensqlite.gen_sqlite()



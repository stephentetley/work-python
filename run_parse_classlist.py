# temp_parse_classlist.py

import os
import sptlibs.classlist.classlist_parser as classlist_parser
from sptlibs.classlist.gen_sqlite import GenSqlite

equisource = 'g:/work/2023/classlist/002-equi-classlist.txt'
flocsource = 'g:/work/2023/classlist/003-floc-classlist.txt'
output_directory = 'g:/work/2023/classlist'

## TODO floc and equi look like they have different layouts...

if os.path.exists(flocsource): 
    cf1 = classlist_parser.parse_floc_classfile(flocsource)
    chars = cf1['characteristics']
    print(chars)
    vals = cf1['enum_values']
    print(vals)

    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.add_floc_classlist(flocsource)
    gensqlite.add_equi_classlist(equisource)
    gensqlite.gen_sqlite()



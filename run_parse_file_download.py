# temp_valuaequi.py

import os
import sptlibs.file_download.file_download_parser as file_download_parser
from sptlibs.file_download.gen_sqlite import GenSqlite

source = 'g:/work/2023/file_download/valuaequi01.txt'
output_directory = 'g:/work/2023/file_download'

if os.path.exists(source): 
    print(f'Found: {source}')
    ans1 = file_download_parser.parse_file_download(source)
    if ans1 is not None:
        print(ans1['entity_type'])
        print(ans1['dataframe'])
    else:
        print(f'Parsing {source} failed')

    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.add_file_download(source, table_name='valuaequi', df_trafo=None)
    gensqlite.gen_sqlite()

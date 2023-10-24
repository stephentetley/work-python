# temp_valuaequi.py

import os
from sptlibs.file_download.file_download_parser import FileDownloadParser
from sptlibs.file_download.gen_sqlite import GenSqlite

source = 'g:/work/2023/file_download/valuaequi01.txt'
output_directory = 'g:/work/2023/file_download'

if os.path.exists(source): 
    print(f'Found: {source}')
    ans1 = FileDownloadParser(source)
    print(ans1.entity_type)
    print(ans1.dataframe)

    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.add_file_download(source, table_name='valuaequi', df_trafo=None)
    gensqlite.gen_sqlite()

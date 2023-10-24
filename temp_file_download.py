# temp_valuaequi.py

import os
from sptlibs.file_download.file_download_parser import FileDownloadParser

source = 'g:/work/2023/file_download/valuaequi01.txt'

if os.path.exists(source): 
    print(f'Found: {source}')
    ans1 = FileDownloadParser(source)
    print(ans1.entity_type)
    print(ans1.dataframe)

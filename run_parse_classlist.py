# temp_parse_classlist.py

import os
import sptlibs.classlist.classlist_parser as classlist_parser
# from sptlibs.file_download.file_download_parser import FileDownloadParser
# from sptlibs.file_download.gen_sqlite import GenSqlite

source = 'g:/work/2023/classlist/003-floc-classlist.txt'
output_directory = 'g:/work/2023/classlist'


print('Start')
cf1 = classlist_parser.parse_classsfile(source)
print(cf1)


# temp_parse_classlist.py

import os
import sptlibs.classlist.classlist_parser as classlist_parser

source = 'g:/work/2023/classlist/003-floc-classlist.txt'
output_directory = 'g:/work/2023/classlist'


print('Start')
cf1 = classlist_parser.parse_classsfile(source)
# print(cf1)
chars = cf1['characteristics']
print(chars)
vals = cf1['enum_values']
print(vals)



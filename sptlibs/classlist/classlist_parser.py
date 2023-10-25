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
import pandas as pd

def _is_empty_line(s: str) -> bool: 
    return s in ['', '  |', '  |   |', '      |', '  |   |   |', '  |       |']

def _is_class_line(s: str) -> bool:
    try:
        return s[0] == ' ' and s[2:5] in ['|--', '---']
    except: 
        return False
    
def _is_char_line(s: str) -> bool:
    try:
        return s[0] == ' ' and s[6:9] in ['|--', '---']
    except: 
        return False

def _is_value_line(s: str) -> bool:
    try:
        return s[0] == ' ' and s[10:13] in ['|--', '---']
    except: 
        return False

def _try_get_int(s: str) -> int:
    try: 
        return int(s)
    except Exception as exn: 
        # print(f'{s} => {exn}')
        return None


def _get_floc_class_props(s: str) -> dict:
    try: 
        dict = {}
        dict['type'] = s[11:14].rstrip()
        dict['name'] = s[15:47].rstrip()
        dict['description'] = s[48:84].rstrip()
        return dict
    except: 
        return None

def _get_equi_class_props(s: str) -> dict:
    try: 
        dict = {}
        dict['type'] = s[11:14].rstrip()
        dict['name'] = s[15:47].rstrip()
        dict['description'] = s[50:90].rstrip()
        return dict
    except: 
        return None


def _get_floc_char_props(s: str) -> dict:
    try: 
        dict = {}
        dict['name'] = s[15:47].rstrip()
        dict['description'] = s[48:95].rstrip()
        dict['type'] = s[95:105].rstrip()
        dict['length'] = _try_get_int(s[105:115].rstrip())
        dict['precision'] = _try_get_int(s[115:126].rstrip())
        return dict
    except: 
        return None

def _get_equi_char_props(s: str) -> dict:
    try: 
        dict = {}
        dict['name'] = s[15:50].rstrip()
        dict['description'] = s[50:101].rstrip()
        dict['type'] = s[101:111].rstrip()
        dict['length'] = _try_get_int(s[111:121].rstrip())
        dict['precision'] = _try_get_int(s[121:131].rstrip())
        return dict
    except: 
        return None
        

def _get_floc_value_props(s: str) -> dict:
    try: 
        dict = {}
        dict['value'] = s[19:48].rstrip()
        dict['description'] = s[48:].rstrip()
        return dict
    except: 
        return None

def _get_equi_value_props(s: str) -> dict:
    try: 
        dict = {}
        dict['value'] = s[19:50].rstrip()
        dict['description'] = s[50:].rstrip()
        return dict
    except: 
        return None
    
def parse_floc_classfile(path: str) -> dict:
    classes = []
    class1 = None
    char1 = None
    if os.path.exists(path): 
        with open(path, 'r') as infile:
            for line in infile.readlines():
                line = line.rstrip()
                if _is_class_line(line):
                    class1 = _get_floc_class_props(line)
                    class1['characteristics'] = []
                    classes.append(class1)
                elif _is_char_line(line):
                    char1 = _get_floc_char_props(line)
                    char1['values'] = []
                    class1['characteristics'].append(char1)
                elif _is_value_line(line):
                    value1 = _get_floc_value_props(line)
                    char1['values'].append(value1)
                else:
                    continue
    dict = {}
    dict['characteristics'] = _make_charcteristics_dataframe(classes)
    dict['enum_values'] = _make_enum_values_dataframe(classes)
    return dict

def parse_equi_classfile(path: str) -> dict:
    classes = []
    class1 = None
    char1 = None
    if os.path.exists(path): 
        with open(path, 'r') as infile:
            for line in infile.readlines():
                line = line.rstrip()
                if _is_class_line(line):
                    class1 = _get_equi_class_props(line)
                    class1['characteristics'] = []
                    classes.append(class1)
                elif _is_char_line(line):
                    char1 = _get_equi_char_props(line)
                    char1['values'] = []
                    class1['characteristics'].append(char1)
                elif _is_value_line(line):
                    value1 = _get_equi_value_props(line)
                    char1['values'].append(value1)
                else:
                    continue
    dict = {}
    dict['characteristics'] = _make_charcteristics_dataframe(classes)
    dict['enum_values'] = _make_enum_values_dataframe(classes)
    return dict

def _make_charcteristics_dataframe(classes: list[dict]) -> pd.DataFrame:
    values_rows = []
    for class1 in classes:
        class_type = class1['type']
        class_name = class1['name']
        class_desc = class1['description']
        for char1 in class1['characteristics']:
            row = [class_type, class_name, class_desc, char1['name'], char1['description'], char1['type'], char1['length'], char1['precision']]
            values_rows.append(row)
    return pd.DataFrame(values_rows, columns = ['class_type', 'class_name', 'class_description', 'char_name', 'char_description', 'char_type', 'char_length', 'char_precision'])

def _make_enum_values_dataframe(classes: list[dict]) -> pd.DataFrame:
    values_rows = []
    for class1 in classes:
        class_type = class1['type']
        class_name = class1['name']
        for char1 in class1['characteristics']:
            char_name = char1['name']
            for value1 in char1['values']:
                row = [class_type, class_name, char_name, value1['value'], value1['description']]
                values_rows.append(row)
    return pd.DataFrame(values_rows, columns = ['class_type', 'class_name', 'char_name', 'enum_value', 'enum_description'])


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


def _get_class_props(s: str) -> dict:
    try: 
        dict = {}
        dict['type'] = s[11:14].rstrip()
        dict['name'] = s[15:47].rstrip()
        dict['description'] = s[48:84].rstrip()
        return dict
    except: 
        return None

def _get_char_props(s: str) -> dict:
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
    
def _get_value_props(s: str) -> dict:
    try: 
        dict = {}
        dict['value'] = s[19:47].rstrip()
        dict['description'] = s[48:].rstrip()
        return dict
    except: 
        return None

def parse_classsfile(path: str) -> list[dict]:
    classes = []
    class1 = None
    char1 = None
    if os.path.exists(path): 
        with open(path, 'r') as infile:
            for line in infile.readlines():
                line = line.rstrip()
                if _is_class_line(line):
                    class1 = _get_class_props(line)
                    class1['characteristics'] = []
                    classes.append(class1)
                elif _is_char_line(line):
                    char1 = _get_char_props(line)
                    char1['values'] = []
                    class1['characteristics'] += char1
                elif _is_value_line(line):
                    value1 = _get_value_props(line)
                    char1['values'] += value1
                else:
                    continue
    return classes


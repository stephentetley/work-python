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

import json
import pandas as pd

# Rewrite columns in a dtaframe that start with 'json_'


def pp_json_columns(df: pd.DataFrame) -> pd.DataFrame:
    json_columns = sorted(filter(lambda s: s.startswith('json_'), df.columns.values.tolist()))
    for col_name in json_columns: 
        new_col = col_name[5:]
        df[new_col] = df[col_name].apply(lambda x: _simplify(x))
    df = df.drop(json_columns, axis=1)
    return df

def _simplify(x: str) -> str: 
    if x:
        return pp_value(json.loads(x))
    else:
        return ''


def pp_value(jvalue) -> str:
    '''Expects scalars or arrays of scalars. Strings get unquoted, len(1) arrays get printed as singletons.'''
    if jvalue == None:
        return ''
    elif isinstance(jvalue, str): 
        return jvalue
    elif isinstance(jvalue, list):
        xs = _strip_nulls(jvalue)
        xs.sort()
        return ', '.join(map(pp_value, xs))
    else:
        return str(jvalue)


def _pp_values_from_dict(key: str, jsdict: dict) -> str:
    jsvals = jsdict.get(key, '')
    return pp_value(jsvals)
    
def _notEmpty1(x): 
    if x == None: 
        return False
    elif x == '':
        return False
    else: 
        return True

def _strip_nulls(ls): 
    return list(filter(_notEmpty1, ls))

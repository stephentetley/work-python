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
import sptapps.download_summary.unjson as unjson

def equipment_rewrite_equi_classes(df: pd.DataFrame) -> pd.DataFrame:
    df['equi_classes'] = df['equi_classes'].apply(lambda x: _rewrite_equi_classes1(x))
    return df

def _rewrite_equi_classes1(x: str) -> str:
    js = json.loads(x)
    return unjson.pp_value(js)

def class_char_rewrite_characteristics(df: pd.DataFrame) -> pd.DataFrame:
    df['json'] = df['json_chars'].apply(lambda x: json.loads(x))
    keys = df.iloc[0].loc['json'].keys()
    for key in keys:
        df[key] = df['json'].apply(lambda jv: _pp_values_from_dict(key, jv))
    df = df.drop(['json', 'json_chars'], axis=1)
    return df


def _pp_values_from_dict(key: str, jsdict: dict) -> str:
    jsvals = jsdict.get(key, '')
    return unjson.pp_value(jsvals)
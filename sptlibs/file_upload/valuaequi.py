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

import pandas as pd
import io

def blank_na(obj):
    return '' if pd.isna(obj) else obj


def print_valuaequi(df: pd.DataFrame, *, out_path: str) -> str:
    """Formatting done in DB"""
    columns = ['*EQUI', 'ATAW1', 'ATAWE', 'ATAUT', 'CHARID', 'ATNAM', 'ATWRT', 'CLASSTYPE', 'ATCOD', 'ATVGLART', 'TEXTBEZ', 'ATZIS', 'VALCNT', 'ATIMB', 'ATSRT', 'ATFLV', 'ATFLB']
    with io.open(out_path, mode='w', encoding='utf-8') as outw:
        outw.write('* Upload\n')
        outw.write('* Data Model: U1\n')
        outw.write('* Entity Type: VALUAEQUI\n')
        outw.write('* Variant: VALUAEQUI1\n')
        outw.write('\t'.join(columns) + '\n')
        for row in df.itertuples():
            outw.write(f'{row.equipment}\t\t\t\t{row.char_id}\t\t{blank_na(row.char_value)}\t{row.class_type}\t{blank_na(row.code)}\t\t\t{row.instance_counter}\t{row.int_counter_value}\t\t{row.position}\t{blank_na(row.value_from)}\t{blank_na(row.value_to)}\t\n')
    outw.close()
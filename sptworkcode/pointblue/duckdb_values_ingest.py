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


def insert_values_from_sqlite(sqlite_path):
    return f"""
INSERT INTO values_string
SELECT 
    DISTINCT(spb.equipment) AS item_id,
    'ai2_aib_reference',
    spb.ai2_aib_reference AS value
FROM sqlite_scan('{sqlite_path}', 's4_point_blue') spb;

"""

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


def retire_report(csv_output_path):
    return f"""
COPY (
SELECT 
    w.asset_id AS 'AI2 Plinun',
    w.asset_name AS 'Common Name',
    vs.item_id AS 'S4 Equipment Id',
    sem.func_loc AS 'S4 Floc'
FROM worklist w 
JOIN values_string vs ON w.asset_id = vs.value 
JOIN s4_equipment_master sem ON vs.item_id = CAST(sem.equi_id as TEXT)
ORDER BY sem.func_loc 
) TO '{csv_output_path}' (FORMAT CSV, DELIMITER ',', HEADER)

"""

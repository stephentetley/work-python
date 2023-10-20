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

import sptlibs.export_utils as export_utils


def output_retire_report(*, duckdb_path: str, csv_outpath: str) -> str:
    export_utils.output_csv_report(duckdb_path=duckdb_path, select_stmt=retire_report_body, csv_outpath=csv_outpath)

retire_report_body = """
    SELECT 
        w.asset_id AS 'AI2_uid',
        w.asset_name AS 'Common Name',
        vs.item_id AS 'SAP S4 uid',
        sem.func_loc AS 'S4 Floc', 
        sem.equi_name AS 'S4 Equi Name',
        strftime(sem.startup_date, '%d.%m.%Y') AS 'S4 Startup Date'
    FROM aib_worklist w 
    JOIN values_string vs ON w.asset_id = vs.value 
    JOIN s4_equipment_master sem ON vs.item_id = CAST(sem.equi_id as TEXT)
    ORDER BY sem.func_loc 
    """

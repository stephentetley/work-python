# temp_telem_asset_replace.py

# > cd ~/_working/coding/work/work-python/
# > conda activate /home/stephen/miniconda3
# > conda activate develop-env
# > export PYTHONPATH="$HOME/_working/coding/work/work-python/src"
# > python scripts/temp_telem_asset_replace.py

import duckdb
import os
import glob
import sptapps.telemetry_asset_replace.setup_db as setup_db

duckdb_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_22/telem_asset_replace_db.duckdb')    
worklist_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_22/asset_replacement_worklist_20250620.xlsx')


con = duckdb.connect(database=duckdb_path, read_only=False)
setup_db.init_db(worklist_path=worklist_path, 
                 con=con)
con.close()

print(f"Done - added raw data to: {duckdb_path}")




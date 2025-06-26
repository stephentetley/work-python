# temp_telem_asset_replace.py

# > cd ~/_working/coding/work/work-python/
# > conda activate /home/stephen/miniconda3
# > conda activate develop-env
# > export PYTHONPATH="$HOME/_working/coding/work/work-python/src"
# >         

import duckdb
import os
import glob
import sptapps.telemetry_asset_replace.setup_db as setup_db
import sptlibs.data_access.rts_outstations.rts_outstations_import as rts_outstations_import
import sptlibs.data_access.ih06_ih08.ih08_import as ih08_import
import sptlibs.data_access.ai2_export.ai2_export_import as ai2_export_import


duckdb_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/telem_asset_replace_jun25_db.duckdb')    
worklist_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/asset_replacement_worklist_20250625.xlsx')
rts_source_path = os.path.expanduser('~/_working/work/2025/rts/rts_outstations_report_20250625.tsv')
ih08_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ih08_s4_prod_netwtl.xlsx')
ai2_equi_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ai2_equi_outstation_export.xlsx')
ai2_floc_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ai2_parent1_export.xlsx')


if os.path.exists(duckdb_path):
    os.remove(duckdb_path)

con = duckdb.connect(database=duckdb_path, read_only=False)
setup_db.init_db(worklist_path=worklist_path,
                 sheet_name='AB',
                 con=con)
rts_outstations_import.duckdb_import(rts_source_path, con=con)
ih08_import.duckdb_init(con=con)
ih08_import.duckdb_import_files(file_paths=[ih08_source],
                                con=con)
ai2_export_import.duckdb_init(con=con)
ai2_export_import.duckdb_import_landing_files(sources=[ai2_equi_source, ai2_floc_source],
                                              con=con)
con.close()

print(f"Done - added raw data to: {duckdb_path}")




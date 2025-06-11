# temp_floc_delta.py

# > cd $HOME/_working/coding/work/work-python/
# > conda activate /home/stephen/miniconda3
# > conda activate develop-env
# > export PYTHONPATH="$HOME/_working/coding/work/work-python/src"
# > python scripts/temp_floc_delta.py

import duckdb
import asset_tools.apps.floc_delta.generate_flocs as generate_flocs
import os
import glob

duckdb_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/june_gen_missing_flocs/floc_delta_telem_db.duckdb')    
ztable_source = './src/asset_tools/runtime/config/s4_ztables_latest.duckdb'
uploader_template_source = os.path.expanduser('~/_working/coding/work/work-python/src/asset_tools/runtime/config/Uploader_Template.xlsx')
worklist_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/june_gen_missing_flocs/A1_edited_worklist_june.xlsx')
ih06_glob = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/june_gen_missing_flocs/s4_existing_flocs/*.xlsx')
ih06_paths = glob.glob(pathname=ih06_glob)
outfile_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/june_gen_missing_flocs/telem_missing_sloc_upload1.xlsx')

for name in ih06_paths:
    print(name)

print(os.path.getsize(uploader_template_source))    

con = duckdb.connect(database=duckdb_path, read_only=False)
generate_flocs.duckdb_init(worklist_path=worklist_path,
                           ih06_paths=ih06_paths,
                           ztable_source_db=ztable_source,
                           con=con)
generate_flocs.gen_xls_upload(uploader_template=uploader_template_source,
                              uploader_outfile=outfile_path,
                              con=con)
con.close()

print(f"Done - added raw data to: {duckdb_path}")


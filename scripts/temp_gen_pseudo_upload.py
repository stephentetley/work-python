import os
import shutil
import duckdb
import sptapps.pseudo_upload.create_pseudo_upload_files as create_pseudo_upload_files

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_gen_ims_files.py

duckdb_source_path  = 'g:/work/2025/equi_translation/lstnut_pseudo_uploader/lstnut_pseudo_upload.duckdb'
duckdb_output_path  = 'g:/work/2025/equi_translation/lstnut_pseudo_uploader/pseudo_upload1.duckdb'
output_root = 'g:/work/2025/equi_translation/lstnut_pseudo_uploader/output'
location_on_site_updates = 'g:/work/2025/equi_translation/lstnut_pseudo_uploader/pseudo_upload/location_on_site.xlsx'
worklist = 'g:/work/2025/equi_translation/lstnut_pseudo_uploader/pseudo_upload/worklist.xlsx'

if os.path.exists(duckdb_source_path):
    shutil.copy(src=duckdb_source_path, dst=duckdb_output_path)

con = duckdb.connect(database=duckdb_output_path, read_only=False)

create_pseudo_upload_files.create_pseudo_upload_files(worklist_path=worklist, 
                                                      loc_on_site_path=location_on_site_updates,
                                                      output_root=output_root, 
                                                      con=con)


con.close()
print(f'wrote {duckdb_output_path}')



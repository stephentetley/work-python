
import duckdb
import sptapps.ims_xlsx.create_ims_files as create_ims_files

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_gen_ims_files.py

duckdb_output_path  = 'g:/work/2025/sps_for_ims/ims_gen_files_db.duckdb'
xlsx_source = 'g:/work/2025/sps_for_ims/source_files/AI2MitigationPlans20250212.xlsx'
output_root = 'g:/work/2025/sps_for_ims/output'


con = duckdb.connect(database=duckdb_output_path, read_only=False)

create_ims_files.create_ims_files(ims_source_path=xlsx_source, output_root=output_root, con=con)


con.close()
print(f'wrote {duckdb_output_path}')



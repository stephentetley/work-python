# temp_polars.py


import polars as pl
import sptlibs.file_upload.valuafloc as valuafloc

output_path    = 'g:/work/2024/file_upload/temp_gen_upload.txt'


df = pl.DataFrame(
    {
        'function_location': ['ASD01', None],
        'characteristic_id': ['EASTING', 'NORTHING'],
        'characteristic_value': [None, None],
        'class_type': [3, 3],
        'code': [1, 1], 
        'instance_counter': [0, 0], 
        'int_counter_value': [1, 1],
        'position': [0, 0], 
        'value_from': [516068, 447506],
        'value_to': [0, 0]
    },
)

valuafloc.output_valuafloc(df, out_path=output_path)

for row in df.iter_rows(named=True):
    if row['function_location'] is None:
        print("None...")
    print(row)

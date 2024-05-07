# 

import polars as pl
import sptlibs.data_import.import_utils as import_utils

csv_input_path      = 'G:/work/2024/rts/os-report-sample.csv'
csv_output_path     = 'G:/work/2024/rts/os-report-pretty.csv'

df = import_utils.read_csv_source(csv_input_path, normalize_column_names=True, has_header=True)

print(df)

df1 = df.select([ 
        (pl.col("os_name").str.strip_chars()),
        (pl.col("od_name").str.strip_chars()),
        (pl.col("os_addr").str.strip_chars().str.replace(',\s*', '_')),
        (pl.col("os_type").str.strip_chars()),
        (pl.col("os_comment").str.strip_chars()),
        (pl.col("od_comment").str.strip_chars()),
        (pl.col("media").str.strip_chars()),
        (pl.col("scan_sched").str.strip_chars()),
        (pl.col("set_name").str.strip_chars()),
        (pl.col("parent_ou").str.strip_chars()),
        (pl.col("parent_ou_comment").str.strip_chars()),
        (pl.col("last_polled").str.to_datetime("%d/%m/%Y %H:%M")),
        (pl.col("last_power_up").str.to_datetime("%d/%m/%Y %H:%M")),
])

print(df1)

df1.write_csv(file=csv_output_path)
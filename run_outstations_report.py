# run_outstations_report.py

import duckdb
import data_import.rts.duckdb_import_outstations_report as duckdb_import_outstations_report

csv_input_path      = 'G:/work/2024/rts/os-report.csv'
csv_output_path         = 'G:/work/2024/rts/os-report-pretty2.csv'
duckdb_output_path      = 'G:/work/2024/rts/outstations_report.duckdb'

conn = duckdb.connect(database=duckdb_output_path)
duckdb_import_outstations_report.import_outstations_report(csv_input_path, con=conn)

export_stmt = f"COPY rts_raw_data.outstations_report TO '{csv_output_path}' (HEADER, DELIMITER ',');"
conn.execute(export_stmt)
conn.close()

print(f"{duckdb_output_path} created")
print(f"{csv_output_path} written")

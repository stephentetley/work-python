# temp_ai2_export.py

import polars as pl
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.classlists.duckdb_import as classlists_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_reports_import
import sptlibs.asset_ir.ai2_class_rep.ai2_export_to_ai2_class_rep as ai2_export_to_ai2_class_rep
import sptlibs.asset_ir.ai2_class_rep.instrument.lstnut as lstnut
import sptlibs.asset_ir.ai2_class_rep.instrument.fstnem as fstnem
import sptlibs.asset_ir.class_rep.gen_table as cr_gen_table

equi_src = 'g:/work/2024/classlists/002-equi-classlist-feb24.txt'
floc_src = 'g:/work/2024/classlists/003-floc-classlist.txt'
source1 = XlsxSource('G:/work/2024/lstnut/batch2/ai2-export-range.xlsx', 'Sheet1')
source2 = XlsxSource('G:/work/2024/lstnut/batch2/ai2-export-serial-number.xlsx', 'Sheet1')
source3 = XlsxSource('G:/work/2024/lstnut/batch2/ai2-export-setpoints1.xlsx', 'Sheet1')
source4 = XlsxSource('G:/work/2024/lstnut/batch2/ai2-export-setpoints2.xlsx', 'Sheet1')
source5 = XlsxSource('G:/work/2024/lstnut/batch2/ai2-export-signal.xlsx', 'Sheet1')
source6 = XlsxSource('G:/work/2024/ai2_to_s4/ai2-magflow-attribs-export1.xlsx', 'Sheet1')
source7 = XlsxSource('G:/work/2024/ai2_to_s4/ai2-magflow-attribs-export2.xlsx', 'Sheet1')
output_path = 'G:/work/2024/ai2_to_s4/lstnut.duckdb'


conn = duckdb.connect(database=output_path)
classlists_import.init(con=conn)
classlists_import.import_floc_classes(floc_src, con=conn)
classlists_import.import_equi_classes(equi_src, con=conn)

ai2_reports_import.init(con=conn)
ai2_reports_import.import_ai2_exports([source1, source2, source3, source4, source5, source6, source7], con=conn)

ai2_export_to_ai2_class_rep.init(con=conn)
ai2_export_to_ai2_class_rep.ai2_export_to_ai2_classes(con=conn)


# New attempt...
lstnut.create_lstnut_table(con=conn)
lstnut.ingest_lstnut_eav_data(con=conn)
fstnem.create_fstnem_table(con=conn)
fstnem.ingest_fstnem_eav_data(con=conn)

conn.close()
print("done")

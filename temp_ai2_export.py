# temp_ai2_export.py

import polars as pl
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.classlists.duckdb_import as classlists_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_reports_import
import sptlibs.data_ir.ai2_class_rep.ai2_export_to_ai2_classss_rep as ai2_export_to_ai2_classss_rep
import sptlibs.data_ir.class_rep.gen_table as cr_gen_table

equi_src = 'g:/work/2024/classlists/002-equi-classlist-feb24.txt'
floc_src = 'g:/work/2024/classlists/003-floc-classlist.txt'
source1 = XlsxSource('G:/work/2024/ai2_to_s4/ai2-magflow-attribs-export1.xlsx', 'Sheet1')
source2 = XlsxSource('G:/work/2024/ai2_to_s4/ai2-magflow-attribs-export2.xlsx', 'Sheet1')
source3 = XlsxSource('G:/work/2024/ai2_to_s4/lstnut_export1.xlsx', 'Sheet1')
output_path = 'G:/work/2024/ai2_to_s4/magflow.duckdb'


conn = duckdb.connect(database=output_path)
classlists_import.init(con=conn)
classlists_import.import_floc_classes(floc_src, con=conn)
classlists_import.import_equi_classes(equi_src, con=conn)

ai2_reports_import.init(con=conn)
ai2_reports_import.import_ai2_export(source1, con=conn)
ai2_reports_import.import_ai2_export(source2, con=conn)
ai2_reports_import.import_ai2_export(source3, con=conn)

ai2_export_to_ai2_classss_rep.init(con=conn)
ai2_export_to_ai2_classss_rep.ai2_export_to_ai2_classes(con=conn)

cr_gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='LSTNUT', con=conn)
cr_gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='FSTNEM', con=conn)

conn.close()
print("done")

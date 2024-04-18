# temp_ai2_export.py

import polars as pl
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.classlists.duckdb_import as classlists_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_reports_import
import asset_ir.ai2_class_rep.ai2_export_to_ai2_class_rep as ai2_export_to_ai2_class_rep
import sptlibs.asset_ir.class_rep.gen_table as cr_gen_table

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

ai2_export_to_ai2_class_rep.init(con=conn)
ai2_export_to_ai2_class_rep.ai2_export_to_ai2_classes(con=conn)

cr_gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='LSTNUT', con=conn)
cr_gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='FSTNEM', con=conn)

####

columns_query = """
    SELECT 
        lower(ec.char_name) AS column_name
    FROM s4_classlists.equi_characteristics ec
    WHERE 
        ec.class_name = 'LSTNUT'
    ;
"""
lstnut_columns = conn.execute(columns_query).pl().get_column('column_name').to_list()

print("lstnut_columns: {}".format(lstnut_columns))


lstnut_pivot = """
    SELECT 
        md.ai2_reference AS ai2_reference, 
        pv.* EXCLUDE (ai2_reference)
    FROM
        ai2_export.master_data md
    JOIN (PIVOT ai2_export.eav_data ON attribute_name USING first(attribute_value) GROUP BY ai2_reference) pv ON pv.ai2_reference = md.ai2_reference 
    WHERE 
        md.common_name LIKE '%EQUIPMENT: ULTRASONIC LEVEL INSTRUMENT'
    """

df = conn.execute(lstnut_pivot).pl()
print(df)

source_columns = df.columns
print("source_columns: {}".format(source_columns))


existing_columns = [x for x in lstnut_columns if x in source_columns]
missing_columns = [x for x in lstnut_columns if not x in source_columns]


## initialize by copying existing columns:
print("Existing columns:{}".format(existing_columns))
data = {x: df.get_column(x) for x in existing_columns}
data["ai2_reference"] = df.get_column("ai2_reference")

df2 = pl.DataFrame(
    data
)
print(df2)

## add columns missing from `df` because they had no data:
df2 = df2.with_columns(
    [pl.lit("").alias(x) for x in missing_columns]
)

print(df2)

conn.close()
print("done")

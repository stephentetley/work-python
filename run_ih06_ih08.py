import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_sqlite import GenSqlite
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb
from sptlibs.ih06_ih08.column_range import ColumnRange
import sptlibs.import_utils as import_utils
output_directory = 'G:/work/2023/ih06_ih08'

    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.set_db_name(db_name='ih08_multi.sqlite3')
# gensqlite.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
gensqlite.add_ih08_export(XlsxSource('g:/work/2023/ih06_ih08/ih08-with-multiclasses.XLSX', 'Sheet1'), table_name='ih08')
sqlite_path = gensqlite.gen_sqlite()

# genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
# genduckdb.add_s4_equipment_master_insert(sqlite_table='ih08', has_aib_characteritics=True)
# # genduckdb.add_s4_funcloc_master_insert(sqlite_table='ctos', has_aib_characteritics=False)
# genduckdb.gen_duckdb()


def make_equi_table(df: pd.DataFrame, cr: ColumnRange) -> dict: 
    indices = list(range(cr.range_start, cr.range_end + 1, 1))
    if cr.range_name == 'equi_masterdata':
        df1 = df.iloc[:, indices]
        df1 = import_utils.normalize_df_column_names(df1)
        return {'table_name': 'equi_masterdata', 'data_frame': df1}
    else:
        table_name = 'equiclass_%s' % cr.range_name.lower()
        indices.insert(0, 1) # add equipment id
        df1 = df.iloc[:, indices]
        class_column_name = 'Class %s is assigned' % cr.range_name
        class_column_value = '%s is assigned' % cr.range_name
        df2 = df1[df1[class_column_name] == class_column_value]
        df2 = import_utils.normalize_df_column_names(df2)
        return {'table_name': table_name, 'data_frame': df2}
            



xlsx = pd.ExcelFile('g:/work/2023/ih06_ih08/ih08-with-multiclasses.XLSX')
df = pd.read_excel(xlsx, 'Sheet1')
re_class_start = re.compile(r"Class (?P<class_name>[\w_]+) is assigned")

ranges = []
# start at column 1 to drop column 0 `selected line`
range1 = ColumnRange(range_name='equi_masterdata', start=1)

for (ix, col) in enumerate(df.columns):
    find_class_start = re_class_start.search(col)
    if find_class_start:
        ranges.append(range1)
        class_name = find_class_start.group('class_name')
        range1 = ColumnRange(range_name=class_name, start=ix)
    else:
        range1.range_end = ix
    print(ix, col)

ranges.append(range1)

con = sqlite3.connect('g:/work/2023/ih06_ih08/ih08-with-multiclasses.sqlite3')

range1:  ColumnRange
for range1 in ranges:
    tab1 = make_equi_table(df, range1)
    df1 = tab1['data_frame']
    print(range1)
    print(tab1)
    df1.to_sql(name=tab1['table_name'], if_exists='replace', con=con)
    con.commit()

con.close()

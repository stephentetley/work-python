# polars is in my (base) env

from sptlibs.ai2_eav.gen_duckdb import GenDuckdb
from sptlibs.xlsx_source import XlsxSource

parents_source = XlsxSource('g:/work/2024/ai2_to_s4/ai2-magflow-meter-parents-export.xlsx', 'Sheet1')


gen_duckdb = GenDuckdb(duckdb_output_path='g:/work/2024/ai2_to_s4/magflow.db')
gen_duckdb.add_parents_source(source=parents_source)
gen_duckdb.add_eav_source(source=XlsxSource('g:/work/2024/ai2_to_s4/ai2-magflow-attribs-export1.xlsx', 'Sheet1'))
gen_duckdb.add_eav_source(source=XlsxSource('g:/work/2024/ai2_to_s4/ai2-magflow-attribs-export2.xlsx', 'Sheet1'))
gen_duckdb.gen_duckdb()


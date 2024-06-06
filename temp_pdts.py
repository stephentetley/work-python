# temp_polars.py

import duckdb
import polars as pl
from jinja2 import Template
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils

pdt_src = XlsxSource('G:/work/2024/pdts/BCS 02 Valves2.xlsx', 'PDT_Valves')

pdt1 = import_utils.read_xlsx_source(pdt_src, normalize_column_names=True)

print(pdt1)

con = duckdb.connect()

pdt_src = XlsxSource('G:/work/2024/pdts/BCS 02 Valves.xlsx', 'PDT_Valves')

pdt2 = import_utils.read_xlsx_source(pdt_src, normalize_column_names=True)

print(pdt2)


con.register("df_pdt1", pdt1)
con.register("df_pdt2", pdt2)

# UNPIVOT to get (entity, attribute, value) triples.
unpivot_template = """
WITH cte AS (
    UNPIVOT {{polars_df}} ON COLUMNS (* EXCLUDE asset_name) INTO NAME entity_name VALUE attr_value
)
SELECT t.entity_name AS entity_name, t.asset_name AS attr_name, t.attr_value AS attr_value FROM cte t ORDER BY t.entity_name, t.asset_name;
"""

ans1 = con.execute(Template(unpivot_template).render(polars_df='df_pdt1')).pl()
print(ans1)


ans2 = con.execute(Template(unpivot_template).render(polars_df='df_pdt2')).pl()
print(ans2)

con.close()

# polars is in my (base) env

import polars as pl
import duckdb

# Common columns:
# 'AssetId', 'Reference', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
# 'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'


columns_to_drop = [
    'AssetId', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
    'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'
]

primary_columns = [
    'Reference', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
    'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'
]


def melt_attributes(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.drop(columns_to_drop)
    pivot_cols = df1.columns.remove("Reference") 
    df2 = df1.melt(id_vars="Reference", value_vars=pivot_cols)
    return df2


def melt_primary_attributes(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.select(primary_columns)
    pivot_cols = df1.columns.remove("Reference") 
    df2 = df1.melt(id_vars="Reference", value_vars=pivot_cols)
    return df2

df = pl.read_excel(
    source="g:/work/2024/ai2_to_s4/ai2-magflow-attribs-export2.xlsx",
    sheet_name="Sheet1",
)

equipment_eav_ddl = """
    CREATE OR REPLACE TABLE ai2_raw_data.equipment_eav  (
        sainum VARCHAR,
        attr_name VARCHAR,
        attr_value VARCHAR,
        PRIMARY KEY(sainum, attr_name)
    );
"""

con = duckdb.connect('g:/work/2024/ai2_to_s4/magflow.db', read_only=False)
con.execute(equipment_eav_ddl)


equipment_eav_insert = """
    INSERT INTO ai2_raw_data.equipment_eav BY NAME
    SELECT 
        df.Reference AS sainum,
        df.variable AS attr_name,
        df.value AS attr_value,
    FROM vw_df AS df
    ON CONFLICT DO NOTHING;
"""

df1 = melt_attributes(df)
print(df1)

df2 = melt_primary_attributes(df)
print(df2)

con.register(view_name='vw_df', python_object=df2)
con.execute(equipment_eav_insert)
con.commit()


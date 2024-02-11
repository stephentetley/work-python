# polars is in my (base) env

import polars as pl
# import duckdb

# Common columns:
# 'AssetId', 'Reference', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
# 'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'

columns_to_drop = [
    'AssetId', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
    'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'
]


def df_transform(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.drop(columns=columns_to_drop)
    return df1

df = pl.read_excel(
    source="g:/work/2024/ai2_to_s4/ai2-magflow-attribs-export2.xlsx",
    sheet_name="Sheet1",
)

df1 = df_transform(df)

print(df1)

pivot_cols = df1.columns.remove("Reference") 

df2 = df1.melt(id_vars="Reference", value_vars=pivot_cols)

print(df2)

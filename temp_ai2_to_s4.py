# polars is in my (base) env

import polars as pl
import polars.selectors as cs
import duckdb

con = duckdb.connect('g:/work/2024/ai2_to_s4/magflow.db', read_only=False)

df = con.execute("""
    SELECT * FROM ai2_raw_data.equipment_eav
                 """
).pl()
print(df)

def rename1(s: str, t: str, df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({s: t})

out = df.pivot(index="sai_num", columns="attr_name", values="attr_value", aggregate_function="first")
print(rename1("Common Name", "common_name", out))

# if column doesn't exist, rename throws error...
# print(rename1("Common NameX", "common_name", out))

print(out.columns)


out1 = out.select(
    [ (pl.col("sai_num"))
    , (pl.col("Manufacturer").alias("manufacturer"))
    , (pl.col("Model").alias("model"))
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%d.%m.%Y")).alias("startup_date")
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%Y")).alias("constr_year")
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%m")).alias("constr_month")
    ]
)

print(out1)



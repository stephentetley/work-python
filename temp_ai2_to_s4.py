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

# NOTE
# Best way to insure the pivot table has necessary columns is to add fake recods with them.
# sai_num     ┆ attr_name            ┆ attr_value
# Doing this then pivotting solves the ai2 to s4 translation puzzle

sample = pl.DataFrame([ 
    {'sai_num': 'X001', 'attr_name': 'Weight kg', 'attr_value': '10.0'},
    {'sai_num': 'X001', 'attr_name': 'Specific Model/Frame', 'attr_value': 'Hydro1'},
])

stk = df.vstack(sample)
out = stk.pivot(index="sai_num", columns="attr_name", values="attr_value", aggregate_function="first")
print(rename1("Common Name", "common_name", out))





# if column doesn't exist, rename throws error...
# print(rename1("Common NameX", "common_name", out))

# def ext_serial_no(df: pl.DataFrame) -> pl.DataFrame: 
#     if "Serial No" in df.columns:
#         return df.rename({"Serial No": "manuf_serial_number"})
#     else:
#         df.with_columns(manuf_serial_number = pl.lit(""))

# def ext_specific_model_frame(df: pl.DataFrame) -> pl.DataFrame: 
#     if "Serial No" in df.columns:
#         return df.rename({"Specific Model/Frame": "manuf_part_number"})
#     else:
#         return df.with_columns(manuf_part_number = pl.lit(""))

print(out.columns)

out1 = out.select(
    [ (pl.col("sai_num"))
    , (pl.col("Common Name").str.extract(r"/([^/]+)/EQUIPMENT:", 1).alias("name"))
    , (pl.col("Common Name").str.extract(r"EQUIPMENT: (.+)", 1).alias("equi_type"))
    , (pl.col("Manufacturer").alias("manufacturer"))
    , (pl.col("Model").alias("model"))
    , (pl.col("Specific Model/Frame").alias("manuf_part_no"))
    , (pl.col("Weight kg").alias("weight"))
    , (pl.lit("KG").alias("weight_unit"))
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%d.%m.%Y")).alias("startup_date")
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%Y")).alias("constr_year")
    , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%m")).alias("constr_month")
    ]
)

print(out1)



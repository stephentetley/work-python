# polars is in my (base) env

import polars as pl
import polars.selectors as cs
import duckdb
import sptapps.ai2_to_s4.master_data as master_data

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
# Doing this then pivoting solves the ai2 to s4 translation puzzle


stk = df.vstack(master_data.eav_sample)
out = stk.pivot(index="sai_num", columns="attr_name", values="attr_value", aggregate_function="first")
print(rename1("Common Name", "common_name", out))



print(out.columns)

out1 = master_data.extract_masterdata(out)


print(out1)

con.execute('CREATE SCHEMA IF NOT EXISTS ai2_to_s4;')
master_data.store_masterdata(con=con, exec_ddl=True, df=out1)


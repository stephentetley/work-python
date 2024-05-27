# temp_polars.py

from typing import Callable

import polars as pl
# import sptapps.data_update.file_upload.valuafloc as valuafloc

output_path    = 'g:/work/2024/file_upload/temp_gen_upload.txt'


df_chars = pl.DataFrame(
    {
        'equipment': ['1001001', '1001001', '1001005', '1001005'],
        'class_type': ['002', '002', '002', '002'],
        'characteristic_id': ['EASTING', 'NORTHING', 'EASTING', 'NORTHING'],
        'characteristic_value': [None, None, None, None],
        
        'code': [1, 1, 1, 1], 
        'instance_counter': [0, 0, 0, 0], 
        'int_counter_value': [1, 1, 1, 1],
        'position': [0, 0, 0, 0], 
        'value_from': [516068, 447506, 516068, 447506],
        'value_to': [0, 0, 0, 0]
    },
)


df_equi = pl.DataFrame(
    {
        'equipment': ['1001001', '1001005'],
        'functional_location': ['ABB01-PWD-PWT-CFG-SYS01', 'ABB01-PWD-PWT-CFG-SYS01'],
        'description': ['Pump-1', 'Pump-2'],
        'object_type': ['PUMP', 'PUMP'],
        'manufacturer': ['OPTIMO', 'OPTIMO'],
        'model': ['OP 1', 'OP 1'],
        'serial_number': ['OP-1-4567', 'OP-1-4787'],
    },
)

df = df_chars.join(df_equi, on='equipment', how='left')


def trim_columns(df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(pl.col("*").exclude("instance_counter", "position", "value_to", "functional_location", "object_type"))

# valuafloc.output_valuafloc(df, out_path=output_path)

print(trim_columns(df))

## update 'EASTING'
def set_easting(easting: int, df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("characteristic_id") == "EASTING").then(easting).otherwise(pl.col("value_from")).alias("value_from")
    )


## update 'NORTHING'
def set_northing(northing: int, df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("characteristic_id") == "NORTHING").then(northing).otherwise(pl.col("value_from")).alias("value_from")
    )

print(trim_columns(set_easting(500000, set_northing(480000, df))))

def set_easting_e(easting: int) -> pl.Expr:
    return pl.when(pl.col("characteristic_id") == "EASTING").then(easting).otherwise(pl.col("value_from")).alias("value_from")

def set_northing_e(easting: int) -> pl.Expr:
    return pl.when(pl.col("characteristic_id") == "NORTHING").then(easting).otherwise(pl.col("value_from")).alias("value_from")


def run_trafo(expr: pl.Expr, df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        expr
    )


print(trim_columns(run_trafo(set_easting_e(500000), df)))

# type Exprs = list[pl.Expr]

def run_trafos(exprs: list[pl.Expr], df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        exprs
    )

print(trim_columns(run_trafos([], df)))

## Can't have multiple expressions within same `with_columns` setting "value_from"
# print(run_trafos([set_easting_e(500000), set_northing_e(480000)], df))


def set_easting_northing(easting: int, northing: int) -> pl.Expr:
    return (pl.when(pl.col("characteristic_id") == "EASTING", pl.col("object_type") == "LSTN")
            .then(easting)
            .when(pl.col("characteristic_id") == "NORTHING")
            .then(northing)
            .otherwise(pl.col("value_from")).alias("value_from"))

print(trim_columns(run_trafo(set_easting_northing(500000, 480000), df)))


def incr_easting_northing(easting: int, northing: int) -> pl.Expr:
    return (pl.when(pl.col("characteristic_id") == "EASTING")
            .then(pl.col("value_from") + easting)
            .when(pl.col("characteristic_id") == "NORTHING")
            .then(pl.col("value_from") + northing)
            .otherwise(pl.col("value_from")).alias("value_from"))

print(trim_columns(run_trafo(incr_easting_northing(499,402), df)))

df_equi_east_north = pl.DataFrame(
    {
        'equipment': ['1001001', '1001005'],
        'easting': [516068, 516068],
        'northing': [447506, 447506],
    },
)


df2 = df_equi.join(df_equi_east_north, on='equipment', how='left')

print(df2)


def set_easting_northing2(easting: int, northing: int) -> Callable[[pl.DataFrame], pl.DataFrame]:
    def inner(df):
        return df.with_columns(
            pl.lit(easting).alias("easting"),
            pl.lit(northing).alias("northing"),
        )
    return inner

def run_trafo2(trafo: Callable[[pl.DataFrame], pl.DataFrame], df: pl.DataFrame) -> pl.DataFrame:
    return trafo(df)

print(run_trafo2(set_easting_northing2(400005, 400006), df2))

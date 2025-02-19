
import shutil
import polars as pl
import pandas as pd


# looks like we have to use `openpyxl` to overwrite a template and go via Pandas...

flocs = pl.DataFrame(
    {'objtype': ['SITE', 'CAA', 'NET', 'TEL'],
     'floc': ['SAI01', 'SAI01-CAA', 'SAI01-CAA-NET', 'SAI01-CAA-NET-TEL'],
     'description': ['Sailing Boat SPS', 'Control and Automation', 'Networks', 'Telemetry']
    })

flocs = flocs.with_columns(
    currency = pl.lit(""),
    masked_floc = pl.col('floc')
)

print(flocs)
flocs = flocs.select(['floc', 'masked_floc', 'description', 'objtype', 'currency'])
print(flocs)

flocs_pandas = flocs.to_pandas()

print(flocs_pandas)

dest = 'g:/work/2025/s4_uploader/floc1.xlsx'
shutil.copy('g:/work/2025/s4_uploader/floc_template.xlsx', dest)


with pd.ExcelWriter(dest, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
    flocs_pandas.to_excel(
        writer,
        sheet_name='Functional Location',
        startcol=0,
        startrow=2,
        index=False,
        header=False,
    )

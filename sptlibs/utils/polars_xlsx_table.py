"""
Copyright 2024 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""


import polars as pl
from xlsxwriter import Workbook

class PolarsXlsxTable:

    def __init__(self, *, df: pl.DataFrame) -> None:
        self.data_frame = df
        self.header_format = None


    def write_excel(self, *, workbook: Workbook, sheet_name: str, column_formats: dict) -> None:
        self.data_frame.write_excel(
            workbook, sheet_name,
            header_format={
                'bold': True,
                'text_wrap': False,
                'align': 'left',
                'fg_color': '#FFFF00',
                'border': None}, 
            freeze_panes=(1, 0),
            autofit=True, 
            column_formats = column_formats)

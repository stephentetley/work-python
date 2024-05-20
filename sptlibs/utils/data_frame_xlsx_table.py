"""
Copyright 2023 Stephen Tetley

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


import pandas as pd
import xlsxwriter

class DataFrameXlsxTable:

    def __init__(self, *, df: pd.DataFrame) -> None:
        self.data_frame = df
        self.header_format = None


    def to_excel(self, *, writer: pd.ExcelWriter, sheet_name: str) -> None:
        # Don't writer index (index=False)
        self.data_frame.to_excel(writer, engine='xlsxwriter', sheet_name=sheet_name, index=False, startrow=1, header=False)
        workbook: xlsxwriter.Workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        if self.header_format:
            header_format = self.header_format
        else:
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': False,
                'align': 'left',
                'fg_color': '#FFFF00',
                'border': None})
            self.header_format = header_format
        for col_num, value in enumerate(self.data_frame.columns.values):
            worksheet.write(0, col_num, value, header_format)
        (max_row, max_col) = self.data_frame.shape
        worksheet.autofilter(0, 0, max_row, max_col - 1)
        worksheet.autofit()
        worksheet.freeze_panes(1, 0)  # Freeze the first row.
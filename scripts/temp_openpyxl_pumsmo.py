from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table
from openpyxl.utils import get_column_letter

# Ideally column_index and ws_meta would be encapsulated state
def add_enums_table(*, 
                    table_name: str,
                    column_header: str,
                    column_index: int,
                    enum_values: list[str],
                    ws_meta: Worksheet) -> None:
    ws_meta.cell(row=1, column=column_index).value = column_header
    for ix, s in enumerate(enum_values):
        ws_meta.cell(row=ix+2, column=column_index).value = s
    column_letter = get_column_letter(column_index)
    ref_string = f"{column_letter}1:{column_letter}{ix+2}"
    print(f"{column_header} {ref_string}")
    tab = Table(displayName=table_name, ref=ref_string)
    ws_meta.add_table(tab)

wb = Workbook()
ws = wb.active
ws.title = "pumsmo"
wb.create_sheet("metadata_tables")

md = wb["metadata_tables"]


add_enums_table(table_name='TABLE1',
                column_header='INSULATION_CLASS_DEG_C',
                column_index=1,
                enum_values= ['200 (CLASS K)',
                              '180 (CLASS H)',
                              '155 (CLASS F)',
                              '130 (CLASS B)',
                              '120 (CLASS E)',
                              '105 (CLASS A)'
                              ],
                ws_meta=md)


ws.cell(row=1, column=1).value = 'Insulation Class'

dv = DataValidation(type="list", formula1='INDIRECT("TABLE1[INSULATION_CLASS_DEG_C]")', allow_blank=True)
ws.add_data_validation(dv)
dv.add('A2:A1000')


add_enums_table(table_name='TABLE2',
                column_header='PUMS_LIFTING_TYPE',
                column_index=2,
                enum_values= ['BLUE ROPE',
                              'CHAIN',
                              'DAVIT ARM'
                              ],
                ws_meta=md)

ws.cell(row=1, column=2).value = 'Lifting Type'

dv = DataValidation(type="list", formula1='INDIRECT("TABLE2[PUMS_LIFTING_TYPE]")', allow_blank=True)
ws.add_data_validation(dv)
dv.add('B2:B1000')


wb.save("pumsmo.xlsx")
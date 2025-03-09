# temp_concat_aide_changes.py
import duckdb
import sptlibs.data_access.aide_changes.aide_changes as aide_changes

con = duckdb.connect(read_only=False)
aide_changes.duckdb_init(xlsx_directory_glob='g:/work/2025/soev/2025/*.xlsx', con=con)
aide_changes.export_xlsx(xlsx_path='g:/work/2025/soev/output2025.xlsx', con=con)
con.close()


import pandas as pd
import duckdb
import sptlibs.file_upload.valuaequi as valuaequi


duckdb_path = 'g:/work/2023/file_upload/concertor_pumps/80_fd.duckdb'

def uploads_selector_query(*, view_name: str) -> str: 
    return f"""
    SELECT 
        cua.equipment AS equipment,
        cua.char_id AS char_id,
        cua.char_value AS char_value,
        cua.class_type AS class_type,
        cua.code AS code,
        cua.instance_counter AS instance_counter,
        cua.int_counter_value AS int_counter_value,
        cua.position AS position,
        cua.value_from AS value_from,
        cua.value_to AS value_to,
    FROM {view_name} cua
    ORDER BY char_id, equipment
    """ 


con = duckdb.connect(database=duckdb_path, read_only=False)
con.execute(uploads_selector_query(view_name='vw_cp_upload_annihilator'))
df = con.df()
valuaequi.print_valuaequi(df, out_path='g:/work/2023/file_upload/concertor_pumps/80_4_annihilator_upload.txt')

con.execute(uploads_selector_query(view_name='vw_cp_upload_assign_const'))
df = con.df()
valuaequi.print_valuaequi(df, out_path='g:/work/2023/file_upload/concertor_pumps/80_4_assign_const_upload.txt')

con.close()


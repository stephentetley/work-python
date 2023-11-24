import pandas as pd
import duckdb
import sptlibs.file_upload.valuaequi as valuaequi

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

duckdb_path = 'g:/work/2023/file_upload/concertor_pumps/150_fd.duckdb'

con = duckdb.connect(database=duckdb_path, read_only=False)
valuaequi.setup_views(con= con, outlet_size_mm='150', rated_power='5.5')
# gen annihilator
con.execute(uploads_selector_query(view_name='vw_cp_upload_annihilator'))
df = con.df()
valuaequi.print_valuaequi(df, out_path='g:/work/2023/file_upload/concertor_pumps/150_5-5_annihilator_upload.txt')
# assign const
con.execute(uploads_selector_query(view_name='vw_cp_upload_assign_const'))
df = con.df()
valuaequi.print_valuaequi(df, out_path='g:/work/2023/file_upload/concertor_pumps/150_5-5_assign_const_upload.txt')
con.close()


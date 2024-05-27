# temp_s4_eavalues.py


import duckdb
from sptlibs.utils.asset_data_config import AssetDataConfig
import sptlibs.data_import.s4_classlists.duckdb_import as classlists_duckdb_import
import sptlibs.data_import.file_download.duckdb_import as file_download_duckdb_import
import asset_ir.s4_class_rep.duckdb_init as s4_class_rep_duckdb_setup

def main():
    config = AssetDataConfig()
    config.set_focus('file_download_summary')
    classlists_db = config.get_expanded_path('classlists_db_src')

    glob_pattern        = 'hs*.txt'
    source_directory    = 'g:/work/2024/file_download/hsm'
    output_db           = 'g:/work/2024/file_download/hsm/hsm_s4_class_rep.duckdb'


    conn = duckdb.connect(database=output_db)
    classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

    file_download_duckdb_import.init_s4_fd_raw_data_tables(con=conn)
    file_download_duckdb_import.store_download_files(source_dir=source_directory, glob_pattern=glob_pattern, con=conn)

    s4_class_rep_duckdb_setup.init_s4_class_rep_tables(con=conn)
    s4_class_rep_duckdb_setup.materialize_data(con=conn)

main()
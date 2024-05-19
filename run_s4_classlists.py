# run_classlists.py

import duckdb
import sptlibs.cmdline_utils as cmdline_utils
import sptlibs.data_import.classlists.duckdb_import as duckdb_import



def main(): 

    config = cmdline_utils.get_asset_data_config().get('s4_classlists', None)

    if config:
        equi_src = cmdline_utils.get_expanded_path('equi_classlist_src', config)
        floc_src = cmdline_utils.get_expanded_path('floc_classlist_src', config)
        output_path = cmdline_utils.get_expanded_path('classlists_outpath', config)
        conn = duckdb.connect(database=output_path)
        duckdb_import.init(con=conn)
        duckdb_import.import_floc_classes(floc_src, con=conn)
        duckdb_import.import_equi_classes(equi_src, con=conn)
        conn.close()
        print(f"Done - created: {output_path}")

main()
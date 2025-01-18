"""
Copyright 2025 Stephen Tetley

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

# While debugging, set PYTHONPATH and run from work-python root:

# (base) > $env:PYTHONPATH='E:\coding\work\work-python\'
# (base) > python .\sptapps\file_download_summary\file_download_summary.py
#
# Point browser to:
# > http://localhost:5000/file_download_summary


import os
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename
import duckdb
from sptlibs.utils.asset_data_config import AssetDataConfig
import sptlibs.data_access.s4_classlists.duckdb_import as classlists_duckdb_import
import sptlibs.data_access.file_download.duckdb_import as file_download_duckdb_import
import sptlibs.class_rep.s4_class_rep.duckdb_init as s4_class_rep_duckdb_setup
import sptapps.reports.s4_class_rep_report.gen_report as gen_report


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './sptapps/file_download_summary/runtime/uploads'
app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'

def create_report(fd_files: list[str]) -> None: 
    config = AssetDataConfig()
    classlists_db = config.get_classlists_db()


    report_name = 'fd-summary-report.xlsx'

    fullpath = os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER'])
    xlsx_output_path = os.path.join(fullpath, report_name)

    conn = duckdb.connect(read_only=False)
    classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

    file_download_duckdb_import.init_s4_fd_raw_data_tables(con=conn)
    for file_path in fd_files:
        file_download_duckdb_import.store_download_file(path=file_path, con=conn)

    s4_class_rep_duckdb_setup.init_s4_class_rep_tables(con=conn)
    gen_report.gen_report(xls_output_path=xlsx_output_path, con=conn)
    conn.close()
    app.logger.info(f"Created - {xlsx_output_path}")




@app.route('/file_download_summary')
def index():
    return render_template('upload.html')

def store_file(file_sto: werkzeug.datastructures.FileStorage) -> str:
    save_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file_sto.filename))
    app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path


@app.route('/uploader', methods=['POST'])
def upload_file():
    temp_paths = [store_file(file_sto) for file_sto in request.files.getlist('files')]
    create_report(temp_paths)
    return redirect(url_for('download', filename='fd-summary-report.xlsx'))


@app.route('/downloads/<path:filename>')
def download(filename):
    app.logger.info("Downloading from:")
    app.logger.info(current_app.root_path)
    app.logger.info(app.config['DOWNLOAD_FOLDER'])
    app.logger.info(filename)
    fullpath = os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER'])
    app.logger.info(fullpath)
    return send_from_directory(directory=fullpath, path=filename, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
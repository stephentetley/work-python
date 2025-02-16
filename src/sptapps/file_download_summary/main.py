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

# (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
# (base) > python .\src\sptapps\file_download_summary\file_download_summary.py
#
# Point browser to:
# > http://localhost:5000/


import os
from flask import Flask, render_template, session, request, redirect, url_for, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename
import duckdb
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
import sptlibs.data_access.file_download.file_download_import as file_download_import
import sptlibs.classrep.s4_classrep.s4_classrep_setup as s4_classrep_setup
import sptapps.reports.s4_class_rep_report.gen_report as gen_report


app = Flask(__name__)
app.config["SECRET_KEY"] = "no-way-jose0987654321"
app.config['RESOURCE_FOLDER'] = './runtime/config'
app.config['UPLOAD_FOLDER'] = './runtime/uploads'
app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'

def create_report(fd_files: list[str]) -> None: 
    
    config_folder = os.path.join(current_app.root_path, app.config['RESOURCE_FOLDER'])
    classlists_db = os.path.normpath(os.path.join(config_folder, 's4_classlists_latest.duckdb'))

    report_name = 'fd-summary-report.xlsx'

    output_folder = os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER'])
    xlsx_output_path = os.path.join(output_folder, report_name)

    # Use an in-memory connection
    conn = duckdb.connect(read_only=False)
    s4_classlists_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

    file_download_import.duckdb_table_init(con=conn)
    for file_path in fd_files:
        file_download_import.duckdb_import(path=file_path, con=conn)

    s4_classrep_setup.duckdb_init(con=conn)
    gen_report.gen_report(xls_output_path=xlsx_output_path, con=conn)
    conn.close()
    app.logger.info(f"Created - {xlsx_output_path}")


def store_upload_file(file_sto: werkzeug.datastructures.FileStorage) -> str:
    fullpath = os.path.normpath(os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER']))
    save_path = os.path.join(fullpath, secure_filename(file_sto.filename))
    app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path


@app.route('/')
def index():
    return render_template('upload.html')



@app.route('/upload', methods=['POST'])
def upload_file():
    upload_paths = [store_upload_file(file_sto) for file_sto in request.files.getlist('files')]
    uploads_cat = '>>>'.join(upload_paths)
    session['upload_paths'] = uploads_cat
    return render_template('loading.html')

@app.route('/results')
def results():
    uploads_cat = session['upload_paths'] 
    upload_paths = uploads_cat.split('>>>')
    create_report(upload_paths)
    return render_template('result.html')

@app.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = 'fd-summary-report.xlsx'
        fullpath = os.path.normpath(os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER']))
        app.logger.info("Downloading from:")
        app.logger.info(current_app.root_path)
        app.logger.info(app.config['DOWNLOAD_FOLDER'])
        app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)


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

# For development, set PYTHONPATH and run from work-python root:

# (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
# (base) > python .\src\sptapps\triage_aide_changes\main.py
#
# Point browser to:
# > http://localhost:5000/


import os
import duckdb
from flask import Flask, render_template, session, request, redirect, url_for, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename

import sptapps.triage_aide_changes.generate_report as generate_report


app = Flask(__name__)
app.config["SECRET_KEY"] = "no-way-jose0987654321"
app.config['RESOURCE_FOLDER'] = './runtime/config'
app.config['UPLOAD_FOLDER'] = './runtime/uploads'
app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'





def create_report(ih06_path: str, 
                  ih08_path: str,
                  ai2_site_export: str,
                  aide_changes: str) -> str: 

    config_folder = os.path.join(current_app.root_path, app.config['RESOURCE_FOLDER'])
    type_translations_xlsx = os.path.normpath(os.path.join(config_folder, 'equi_type_translation.xlsx'))
   
    output_folder = os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER'])
    
    temp_duckdb_path = os.path.normpath(os.path.join(output_folder, 'triage_changes.duckdb'))
    
    report_name = 'triage_aide_changes.xlsx'
    xlsx_output_path = os.path.normpath(os.path.join(output_folder, report_name))

    # Use an file connection for the time being until we start to generate an xlsx file...
    # Delete it first   
    if os.path.exists(temp_duckdb_path):
        os.remove(temp_duckdb_path)

    con = duckdb.connect(database=temp_duckdb_path, read_only=False)
    app.logger.info(f"before: <generate_flocs>")
    generate_report.duckdb_init(equi_type_translation=type_translations_xlsx,
                                ih06_source=ih06_path,
                                ih08_source=ih08_path,
                                ai2_site_export=ai2_site_export,
                                aide_changelist=aide_changes,
                                con=con)
    generate_report.gen_report(xls_output_path=xlsx_output_path, con=con)
    con.close()
    app.logger.info(f"Created - {report_name}")
    return report_name

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
def upload():
    if request.method == "POST":
        ih06_sto = request.files.get('ih06')
        ih06_file = store_upload_file(ih06_sto)
        ih08_sto = request.files.get('ih08')
        ih08_file = store_upload_file(ih08_sto)
        ai2_site_sto = request.files.get('ai2_site')
        ai2_site_file = store_upload_file(ai2_site_sto)
        aide_changes_sto = request.files.get('aide_changes')
        aide_changes_file = store_upload_file(aide_changes_sto)
        session["ih06_file"] = ih06_file
        session["ih08_file"] = ih08_file
        session["ai2_site_file"] = ai2_site_file
        session["aide_changes_file"] = aide_changes_file
        return render_template('loading.html')


@app.route('/result')
def result():
    ih06_file = session["ih06_file"]
    ih08_file = session["ih08_file"]
    ai2_site_file = session["ai2_site_file"]
    aide_changes_file = session["aide_changes_file"]
    outfile_name = create_report(ih06_path=ih06_file,
                                 ih08_path=ih08_file,
                                 ai2_site_export=ai2_site_file,
                                 aide_changes=aide_changes_file)
    session["outfile_name"] = outfile_name
    app.logger.info(f'outfile_name: {outfile_name}')
    return render_template('result.html')

@app.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = session["outfile_name"]
        fullpath = os.path.normpath(os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER']))
        app.logger.info("Downloading from:")
        app.logger.info(current_app.root_path)
        app.logger.info(app.config['DOWNLOAD_FOLDER'])
        app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

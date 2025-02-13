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
# (base) > python .\src\sptapps\floc_delta\main.py
#
# Point browser to:
# > http://localhost:5000/


import os
import duckdb
from flask import Flask, render_template, session, request, redirect, url_for, send_from_directory, current_app
from flask import after_this_request
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename

import sptapps.floc_delta.generate_flocs as generate_flocs


app = Flask(__name__)
app.config["SECRET_KEY"] = "no-way-jose0987654321"
app.config['RESOURCE_FOLDER'] = './runtime/config'
app.config['UPLOAD_FOLDER'] = './runtime/uploads'
app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'





def create_report(worklist_path: str, ih06_path: str) -> str: 
    app.logger.info(f"worklist - {worklist_path}")
    app.logger.info(f"ih06 - {ih06_path}")

    config_folder = os.path.join(current_app.root_path, app.config['RESOURCE_FOLDER'])
    ztables_db = os.path.normpath(os.path.join(config_folder, 's4_ztables_latest.duckdb'))
   
    report_name = 'new_flocs.duckdb'
    output_path = os.path.normpath(os.path.join(current_app.root_path, app.config['DOWNLOAD_FOLDER']))
    
    duckdb_path = os.path.join(output_path, report_name)

    con = duckdb.connect(database=duckdb_path, read_only=False)
    app.logger.info(f"before: <generate_flocs>")
    generate_flocs.generate_flocs(worklist_path=worklist_path, 
                                  ih06_path=ih06_path, 
                                  ztable_source_db=ztables_db, 
                                  con=con)
    con.close()
    app.logger.info(f"Created - {duckdb_path}")
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
        worklist_sto = request.files.get('worklist')
        worklist_path = store_upload_file(worklist_sto)
        session["ih06_file"] = ih06_file
        session["worklist_path"] = worklist_path
        return render_template('loading.html')


@app.route('/results')
def results():
    @after_this_request
    def show_results(response):
        return response
    
    ih06_file = session["ih06_file"]
    worklist_path = session["worklist_path"]
    outfile_name = create_report(worklist_path=worklist_path,
                                     ih06_path=ih06_file)
    session["outfile_name"] = outfile_name
    app.logger.info(f'outfile_name: {outfile_name}')
    return render_template('results.html')

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

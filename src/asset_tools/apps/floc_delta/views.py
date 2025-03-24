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



import os
import duckdb
from flask import Flask, render_template, session, request, redirect, url_for, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename

from asset_tools.apps.floc_delta.forms import FlocDeltaForm
import asset_tools.apps.floc_delta.generate_flocs as generate_flocs
from . import floc_delta




def create_report(worklist_path: str, ih06_path: str) -> str: 
    current_app.logger.info(f"worklist - {worklist_path}")
    current_app.logger.info(f"ih06 - {ih06_path}")

    config_folder = os.path.join(current_app.root_path, current_app.config['RESOURCE_FOLDER'])
    ztables_db = os.path.normpath(os.path.join(config_folder, 's4_ztables_latest.duckdb'))
    uploader_template = os.path.normpath(os.path.join(config_folder, 'Uploader_Template.xlsx'))
   
    report_name = 'flocs_delta_upload.xlsx'
    output_folder = os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER'])
    
    temp_duckdb_path = os.path.normpath(os.path.join(output_folder, 'new_flocs.duckdb'))
    report_path = os.path.normpath(os.path.join(output_folder, report_name))

    # Use an file connection for the time being until we start to generate an xlsx file...
    # Delete it first   
    if os.path.exists(temp_duckdb_path):
        os.remove(temp_duckdb_path)

    con = duckdb.connect(database=temp_duckdb_path, read_only=False)
    current_app.logger.info(f"before: <generate_flocs>")
    generate_flocs.duckdb_init(worklist_path=worklist_path, 
                               ih06_path=ih06_path, 
                               ztable_source_db=ztables_db,
                               con=con)
    generate_flocs.gen_xls_upload(uploader_template=uploader_template,
                                  uploader_outfile=report_path,
                                  con=con)
    con.close()
    current_app.logger.info(f"Created - {report_path}")
    return report_name

def store_upload_file(file_sto: werkzeug.datastructures.file_storage.FileStorage) -> str:
    fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['UPLOAD_FOLDER']))
    save_path = os.path.join(fullpath, secure_filename(file_sto.filename))
    current_app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path


@floc_delta.route('/upload', methods=['POST', 'GET'])
def upload():
    form = FlocDeltaForm()
    if form.validate_on_submit():
        current_app.logger.info("form.validate_on_submit():")
        ih06_sto = form.ih06_export.data
        ih06_file = store_upload_file(ih06_sto)
        worklist_sto = form.worklist.data
        worklist_path = store_upload_file(worklist_sto)
        session["ih06_file"] = ih06_file
        session["worklist_path"] = worklist_path
        return render_template('floc_delta/loading.html')
    return render_template('floc_delta/upload.html', form = form)

@floc_delta.route('/result')
def result():
    ih06_file = session["ih06_file"]
    worklist_path = session["worklist_path"]
    outfile_name = create_report(worklist_path=worklist_path,
                                     ih06_path=ih06_file)
    session["outfile_name"] = outfile_name
    current_app.logger.info(f'outfile_name: {outfile_name}')
    return render_template('floc_delta/result.html')

@floc_delta.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = session["outfile_name"]
        fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER']))
        current_app.logger.info("Downloading from:")
        current_app.logger.info(current_app.root_path)
        current_app.logger.info(current_app.config['DOWNLOAD_FOLDER'])
        current_app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)


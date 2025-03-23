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

from asset_tools.triage_aide_changes.forms import TriageAideChangesForm
import asset_tools.triage_aide_changes.generate_report as generate_report
from . import triage_aide_changes


def create_report(ih06_path: str, 
                  ih08_path: str,
                  ai2_site_export: str,
                  aide_changes: str) -> str: 

    config_folder = os.path.join(current_app.root_path, current_app.config['RESOURCE_FOLDER'])
    type_translations_xlsx = os.path.normpath(os.path.join(config_folder, 'equi_type_translation.xlsx'))
   
    output_folder = os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER'])
    
    temp_duckdb_path = os.path.normpath(os.path.join(output_folder, 'triage_changes.duckdb'))
    
    report_name = 'triage_aide_changes.xlsx'
    xlsx_output_path = os.path.normpath(os.path.join(output_folder, report_name))

    # Use an file connection for the time being (we want to inspect the database)...
    # Delete it first   
    if os.path.exists(temp_duckdb_path):
        os.remove(temp_duckdb_path)

    con = duckdb.connect(database=temp_duckdb_path, read_only=False)
    current_app.logger.info(f"before: <generate_flocs>")
    generate_report.duckdb_init(equi_type_translation=type_translations_xlsx,
                                ih06_source=ih06_path,
                                ih08_source=ih08_path,
                                ai2_site_export=ai2_site_export,
                                aide_changelist=aide_changes,
                                con=con)
    generate_report.gen_xls_report(xls_output_path=xlsx_output_path, con=con)
    con.close()
    current_app.logger.info(f"Created - {report_name}")
    return report_name

def store_upload_file(file_sto: werkzeug.datastructures.FileStorage) -> str:
    fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['UPLOAD_FOLDER']))
    save_path = os.path.join(fullpath, secure_filename(file_sto.filename))
    current_app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path



@triage_aide_changes.route('/upload', methods=['POST', 'GET'])
def upload():
    form = TriageAideChangesForm()
    if form.validate_on_submit():
        current_app.logger.info("form.validate_on_submit():")
        ih06_sto = form.ih06_site_flocs.data
        ih06_file = store_upload_file(ih06_sto)
        ih08_sto = form.ih08_site_equi.data
        ih08_file = store_upload_file(ih08_sto)
        ai2_site_sto = form.ai2_site_export.data
        ai2_site_file = store_upload_file(ai2_site_sto)
        aide_changes_sto = form.aide_changes_export.data
        aide_changes_file = store_upload_file(aide_changes_sto)
        session["ih06_file"] = ih06_file
        session["ih08_file"] = ih08_file
        session["ai2_site_file"] = ai2_site_file
        session["aide_changes_file"] = aide_changes_file
        return render_template('triage_aide_changes/loading.html')
    return render_template('triage_aide_changes/upload.html', form = form)

# @triage_aide_changes.route('/upload', methods=['POST'])
# def upload():
#     if request.method == "POST":
#         ih06_sto = request.files.get('ih06')
#         ih06_file = store_upload_file(ih06_sto)
#         ih08_sto = request.files.get('ih08')
#         ih08_file = store_upload_file(ih08_sto)
#         ai2_site_sto = request.files.get('ai2_site')
#         ai2_site_file = store_upload_file(ai2_site_sto)
#         aide_changes_sto = request.files.get('aide_changes')
#         aide_changes_file = store_upload_file(aide_changes_sto)
#         session["ih06_file"] = ih06_file
#         session["ih08_file"] = ih08_file
#         session["ai2_site_file"] = ai2_site_file
#         session["aide_changes_file"] = aide_changes_file
#         return render_template('triage_aide_changes/loading.html')


@triage_aide_changes.route('/result')
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
    current_app.logger.info(f'outfile_name: {outfile_name}')
    return render_template('triage_aide_changes/result.html')

@triage_aide_changes.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = session["outfile_name"]
        fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER']))
        current_app.logger.info("Downloading from:")
        current_app.logger.info(current_app.root_path)
        current_app.logger.info(current_app.config['DOWNLOAD_FOLDER'])
        current_app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)


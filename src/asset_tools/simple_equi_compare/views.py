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

from asset_tools.simple_equi_compare.forms import SimpleEquiCompareForm
import asset_tools.simple_equi_compare.generate_report as generate_report
from . import simple_equi_compare

def create_report(ih08_files: list[str], ai2_export_files: list[str]) -> str: 
    for f1 in ih08_files:
        current_app.logger.info(f"ih08 - {f1}")
    for f1 in ai2_export_files:
        current_app.logger.info(f"ai2_export - {f1}")

    config_folder = os.path.join(current_app.root_path, current_app.config['RESOURCE_FOLDER'])
    manuf_model_norm_path = os.path.normpath(os.path.join(config_folder, 'normalize_manuf_model.xlsx'))
    ppg_names_path = os.path.normpath(os.path.join(config_folder, 'all_process_processgroup_names.xlsx'))
    site_mapping_path = os.path.normpath(os.path.join(config_folder, 'SiteMapping.xlsx'))
   
    report_name = 'equi_compare.xlsx'
    output_folder = os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER'])
    
    temp_duckdb_path = os.path.normpath(os.path.join(output_folder, 'simple_equi_compare.duckdb'))
    report_path = os.path.normpath(os.path.join(output_folder, report_name))

    # Use an file connection for the time being until we start to generate an xlsx file...
    # Delete it first   
    if os.path.exists(temp_duckdb_path):
        os.remove(temp_duckdb_path)

    con = duckdb.connect(database=temp_duckdb_path, read_only=False)
    current_app.logger.info(f"before: <generate_report>")
    generate_report.duckdb_init(metadata_manuf_model_norm_path=manuf_model_norm_path,
                                metadata_ppg_names_path=ppg_names_path,
                                metadata_site_mapping_path=site_mapping_path,
                                s4_ih08_paths=ih08_files,
                                ai2_export_paths=ai2_export_files,
                                con=con)
    generate_report.gen_xls_report(xls_output_path=report_path,
                                   con=con)
    con.close()
    current_app.logger.info(f"Created - {report_path}")
    return report_name

def store_upload_file(file_sto: werkzeug.datastructures.FileStorage) -> str:
    fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['UPLOAD_FOLDER']))
    save_path = os.path.join(fullpath, secure_filename(file_sto.filename))
    current_app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path


@simple_equi_compare.route('/upload', methods=['POST', 'GET'])
def upload():
    form = SimpleEquiCompareForm()
    if form.validate_on_submit():
        current_app.logger.info("form.validate_on_submit():")
        s4_upload_paths = [store_upload_file(file_sto) for file_sto in form.ih08_exports.data]
        s4_uploads_cat = '>>>'.join(s4_upload_paths)
        session['uploads_s4'] = s4_uploads_cat
        ai2_upload_paths = [store_upload_file(file_sto) for file_sto in form.ai2_exports.data]
        ai2_uploads_cat = '>>>'.join(ai2_upload_paths)
        session['uploads_ai2'] = ai2_uploads_cat
        return render_template('simple_equi_compare/loading.html')
    return render_template('simple_equi_compare/upload.html', form = form)



@simple_equi_compare.route('/result')
def result():
    s4_uploads_cat = session['uploads_s4'] 
    s4_upload_paths = s4_uploads_cat.split('>>>')
    ai2_uploads_cat = session['uploads_ai2'] 
    ai2_upload_paths = ai2_uploads_cat.split('>>>')
    outfile_name = create_report(ih08_files=s4_upload_paths, ai2_export_files=ai2_upload_paths)
    session["outfile_name"] = outfile_name
    current_app.logger.info(f'outfile_name: {outfile_name}')
    return render_template('simple_equi_compare/result.html')


@simple_equi_compare.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = session["outfile_name"]
        fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER']))
        current_app.logger.info("Downloading from:")
        current_app.logger.info(current_app.root_path)
        current_app.logger.info(current_app.config['DOWNLOAD_FOLDER'])
        current_app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)



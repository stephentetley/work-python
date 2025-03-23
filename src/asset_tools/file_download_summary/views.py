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
from flask import Flask, render_template, session, request, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures
from werkzeug.utils import secure_filename
import duckdb

from asset_tools.file_download_summary.forms import FileDownloadSummaryForm
import asset_tools.file_download_summary.generate_report as generate_report
from . import file_download_summary


def create_report(fd_files: list[str]) -> None: 
    
    config_folder = os.path.join(current_app.root_path, current_app.config['RESOURCE_FOLDER'])
    classlists_db = os.path.normpath(os.path.join(config_folder, 's4_classlists_latest.duckdb'))

    report_name = 'fd-summary-report.xlsx'

    output_folder = os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER'])
    xlsx_output_path = os.path.join(output_folder, report_name)

    # Use an in-memory connection
    con = duckdb.connect(read_only=False)
    generate_report.duckdb_init(file_download_files=fd_files,
                                classlists_db_path=classlists_db, 
                                con=con)
    generate_report.gen_xls_report(xls_output_path=xlsx_output_path, con=con)
    con.close()
    current_app.logger.info(f"Created - {xlsx_output_path}")


def store_upload_file(file_sto: werkzeug.datastructures.FileStorage) -> str:
    fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['UPLOAD_FOLDER']))
    save_path = os.path.join(fullpath, secure_filename(file_sto.filename))
    current_app.logger.info(save_path)
    file_sto.save(save_path)
    return save_path


@file_download_summary.route('/upload', methods=['POST', 'GET'])
def upload():
    form = FileDownloadSummaryForm()
    if form.validate_on_submit():
        current_app.logger.info("form.validate_on_submit():")
        upload_paths = [store_upload_file(file_sto) for file_sto in form.aiw_file_downloads.data]
        uploads_cat = '>>>'.join(upload_paths)
        session['upload_paths'] = uploads_cat
        return render_template('file_download_summary/loading.html')
    return render_template('file_download_summary/upload.html', form = form)


@file_download_summary.route('/result')
def result():
    uploads_cat = session['upload_paths'] 
    upload_paths = uploads_cat.split('>>>')
    create_report(upload_paths)
    return render_template('file_download_summary/result.html')

@file_download_summary.route('/download', methods=['POST'])
def download():
    if request.method == "POST":
        outfile_name = 'fd-summary-report.xlsx'
        fullpath = os.path.normpath(os.path.join(current_app.root_path, current_app.config['DOWNLOAD_FOLDER']))
        current_app.logger.info("Downloading from:")
        current_app.logger.info(current_app.root_path)
        current_app.logger.info(current_app.config['DOWNLOAD_FOLDER'])
        current_app.logger.info(fullpath)
        return send_from_directory(directory=fullpath, path=outfile_name, as_attachment=True)



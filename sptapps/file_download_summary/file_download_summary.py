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

# While debugging, run from work-python root:
# (base) > python .\sptapps\file_download_summary\file_download_summary.py
#
# Point browser to:
# > http://localhost:5000/file_download_summary


import os
from flask import Flask, render_template, request, redirect, url_for
from werkzeug.utils import secure_filename


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = './data/uploads'


@app.route('/file_download_summary')
def index():
    return render_template('file_download_summary.html')


@app.route('/uploader', methods=['POST'])
def upload_file():
    uploaded_files = request.files.getlist('files')
    for fd_file in uploaded_files:
        save_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(fd_file.filename))
        app.logger.info(save_path)
        fd_file.save(save_path)
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
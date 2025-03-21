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
# (base) > flask --app .\src\asset_tools\main.py run
#
# Point browser to:
# > http://localhost:5000/


import os
import duckdb
from flask import Flask, render_template, session, request, redirect, url_for, send_from_directory, current_app
import werkzeug
import werkzeug.datastructures


app = Flask(__name__)
app.config["SECRET_KEY"] = "no-way-jose0987654321"
app.config['RESOURCE_FOLDER'] = './runtime/config'
app.config['UPLOAD_FOLDER'] = './runtime/uploads'
app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'


def create_app(config_name):
    app = Flask(__name__)

    from .floc_delta import floc_delta as floc_delta_blueprint
    app.register_blueprint(floc_delta_blueprint, url_prefix='/floc_delta')

    return app

app = create_app('default')

@app.route('/')
def index():
    return render_template('index.html')

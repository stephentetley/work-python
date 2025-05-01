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

# windows:
# (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
# (base) > flask --app .\src\asset_tools\main.py run
#
# linux:
# (base) > conda activate develop-env
# (develop-env) > export PYTHONPATH='/home/_working/coding/work/work-python'
# (develop-env) > flask --app ./src/asset_tools/main.py run


# Point browser to:
# > http://localhost:5000/



from flask import Flask, render_template, session, request, current_app




def create_app(config_name):
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "no-way-jose0987654321"
    app.config['RESOURCE_FOLDER'] = './runtime/config'
    app.config['UPLOAD_FOLDER'] = './runtime/uploads'
    app.config['DOWNLOAD_FOLDER'] = './runtime/downloads/'
    from .apps.file_download_summary import file_download_summary as file_download_summary_blueprint
    app.register_blueprint(file_download_summary_blueprint, url_prefix='/file_download_summary')

    from .apps.floc_delta import floc_delta as floc_delta_blueprint
    app.register_blueprint(floc_delta_blueprint, url_prefix='/floc_delta')
    
    from .apps.simple_equi_compare import simple_equi_compare as simple_equi_compare_blueprint
    app.register_blueprint(simple_equi_compare_blueprint, url_prefix='/simple_equi_compare')
    
    from .apps.triage_aide_changes import triage_aide_changes as triage_aide_changes_blueprint
    app.register_blueprint(triage_aide_changes_blueprint, url_prefix='/triage_aide_changes')
    
    return app

app = create_app('default')

@app.route('/')
def index():
    return render_template('index.html')

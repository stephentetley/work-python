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

from flask_wtf import FlaskForm
from wtforms import SubmitField
from flask_wtf.file import FileField, FileRequired, FileAllowed

class TriageAideChangesForm(FlaskForm):
    ih06_site_flocs = FileField('IH06 Site Flocs (xlsx)', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    ih08_site_equi = FileField('IH08 Site Equi with AIB_REFERENCE (xlsx)', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    ai2_site_export = FileField('AI2 Site export - flocs and equi (xlsx)', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    aide_changes_export = FileField('AIDE Changes export (xlsx)', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    submit = SubmitField('Submit')

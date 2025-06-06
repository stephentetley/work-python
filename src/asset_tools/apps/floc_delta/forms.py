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
from flask_wtf.file import MultipleFileField, FileField, FileRequired, FileAllowed

class FlocDeltaForm(FlaskForm):
    ih06_exports = MultipleFileField('IH06 Exports', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    worklist = FileField('Worklist', validators=[FileRequired(), FileAllowed(['xlsx'], 'Excel *.xlsx files only')])
    submit = SubmitField('Submit')

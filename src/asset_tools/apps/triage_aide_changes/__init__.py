from flask import Blueprint

triage_aide_changes = Blueprint('triage_aide_changes', __name__)

from . import views
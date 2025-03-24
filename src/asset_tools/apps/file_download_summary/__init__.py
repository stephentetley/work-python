from flask import Blueprint

file_download_summary = Blueprint('file_download_summary', __name__)

from . import views
from flask import Blueprint

floc_delta = Blueprint('floc_delta', __name__)

from . import views
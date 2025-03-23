from flask import Blueprint

simple_equi_compare = Blueprint('simple_equi_compare', __name__)

from . import views
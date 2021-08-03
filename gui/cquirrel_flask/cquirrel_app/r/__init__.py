from flask import Blueprint

r = Blueprint('r', __name__)

from . import views


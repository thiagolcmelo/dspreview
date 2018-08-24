# third-party imports
from flask import Blueprint

home = Blueprint('home', __name__)

# local imports
from . import views

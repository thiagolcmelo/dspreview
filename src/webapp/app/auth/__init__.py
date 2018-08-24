# third-party imports
from flask import Blueprint

auth = Blueprint('auth', __name__)

# local imports
from . import views

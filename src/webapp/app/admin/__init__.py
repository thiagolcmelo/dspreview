# third-party imports
from flask import Blueprint

admin = Blueprint('admin', __name__)

# local imports
from . import views

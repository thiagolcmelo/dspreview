# python standard
import os

# local imports
from webapp.app import create_app

config_name = os.getenv('FLASK_CONFIG') or "development"
app = create_app(config_name)

if __name__ == '__main__':
    app.run()
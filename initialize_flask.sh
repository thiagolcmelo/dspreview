#!/bin/bash
# this script initialize the flask migrations
cd src/webapp
export FLASK_CONFIG=production #development
export FLASK_APP=run.py
flask db init
flask db migrate
flask db upgrade
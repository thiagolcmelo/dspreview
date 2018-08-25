# -*- coding: utf-8 -*-

# python standard
import sys
import os
import re
import string
import logging
from collections import namedtuple
from random import choices

# third-party imports
from sqlalchemy import create_engine

# local imports
from utils.config_helper import ConfigHelper

############################################################################
logger = logging.getLogger('dspreview_application')
############################################################################


def rand_word(N):
    """
    generate a secret of size N
    """
    lowers = string.ascii_lowercase
    uppers = string.ascii_uppercase
    digits = string.digits
    return ''.join(choices(lowers + uppers + digits, k=N))


def get_connection_info():
    ci = namedtuple("ConnectionInfo",
                    "dbhost, dbport, dbuser, dbpass, dbname")
    config = ConfigHelper()
    ci.dbhost = os.getenv("DB_HOST") or config.get_config("DB_HOST")
    ci.dbport = os.getenv("DB_PORT") or config.get_config("DB_PORT")
    ci.dbuser = os.getenv("DB_USER") or config.get_config("DB_USER")
    ci.dbpass = os.getenv("DB_PASS") or config.get_config("DB_PASS")
    ci.dbname = os.getenv("DB_NAME") or config.get_config("DB_NAME")
    return ci


def get_connection_info_test():
    ci = namedtuple("ConnectionInfoTest",
                    "dbhost, dbport, dbuser, dbpass, dbname")
    config = ConfigHelper()
    ci.dbhost = os.getenv("DB_HOST") or config.get_config("DB_HOST")
    ci.dbport = os.getenv("DB_PORT") or config.get_config("DB_PORT")
    ci.dbname = os.getenv("DB_TEST_NAME") or config.get_config("DB_TEST_NAME")
    ci.dbuser = os.getenv("DB_TEST_USER") or config.get_config("DB_TEST_USER")
    ci.dbpass = os.getenv("DB_TEST_PASS") or config.get_config("DB_TEST_PASS")
    return ci


def get_connection_strs():
    ci = get_connection_info()
    cit = get_connection_info_test()
    # all these values are necessary
    if not all([ci.dbhost, ci.dbport, ci.dbuser, ci.dbpass, ci.dbname]):
        raise Exception("""It is not possible to connect in the MySQL
                        database. All of the following environment
                        variables must be set
                        - DB_HOST
                        - DB_PORT
                        - DB_USER
                        - DB_PASS
                        - DB_NAME
                        - DB_TEST_NAME (for development only)
                        - DB_TEST_USER (for development only)
                        - DB_TEST_PASS (for development only)
                        another possibility woul be the .dspreview.json
                        ;)""")
    # connection string for operational use
    full = "mysql+mysqldb://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}"
    simple = "mysql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}"

    constr = namedtuple("ConnectionStrings",
                        "con_str, flask_str, test_str")
    constr.con_str = full.format(dbuser=ci.dbuser, dbpass=ci.dbpass,
                                 dbhost=ci.dbhost, dbport=ci.dbport,
                                 dbname=ci.dbname)
    constr.flask_str = simple.format(dbuser=ci.dbuser, dbpass=ci.dbpass,
                                     dbhost=ci.dbhost, dbport=ci.dbport,
                                     dbname=ci.dbname)
    constr.test_str = simple.format(dbuser=cit.dbuser,
                                    dbpass=cit.dbpass,
                                    dbhost=cit.dbhost, dbport=cit.dbport,
                                    dbname=cit.dbname)
    return constr


def initialize_database():
    try:
        flask_str = get_connection_strs().flask_str

        # CREATE CONFIG FILES
        logger.info("Creating configuration files")
        instance_folder = sys.prefix + "/var/webapp.app-instance"
        if not os.path.exists(instance_folder):
            os.makedirs(instance_folder)
        config_file = instance_folder + "/config.py"
        config_lines = []

        track_mod_line = "SQLALCHEMY_TRACK_MODIFICATIONS=False\n"
        sql_config_line = "SQLALCHEMY_DATABASE_URI='{constr}'\n"
        sql_config_line = sql_config_line.format(constr=flask_str)
        secret_key_line = "SECRET_KEY='{secret}'\n"
        secret_key_line = secret_key_line.format(secret=rand_word(20))

        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                config_lines = f.readlines()

        has_uri_cfg = False
        has_secret = False
        has_track_mod = False

        for i in range(len(config_lines)):
            if re.search(".*SQLALCHEMY_DATABASE_URI.*", config_lines[i]):
                config_lines[i] = sql_config_line
                has_uri_cfg = True
            if re.search(".*SECRET_KEY.*", config_lines[i]):
                has_secret = True
            if re.search(".*SQLALCHEMY_TRACK_MODIFICATIONS.*",
                         config_lines[i]):
                has_track_mod = True

        if not has_uri_cfg:
            config_lines.append(sql_config_line)
        if not has_secret:
            config_lines.append(secret_key_line)
        if not has_track_mod:
            config_lines.append(track_mod_line)

        with open(config_file, "w") as f:
            for l in config_lines:
                f.write(l)

        # CREATE CONFIG FILES
        logger.info("Configuring database")
        from webapp.app import db, create_app
        app = create_app('production')
        app.config.update(
            SQLALCHEMY_DATABASE_URI=flask_str
        )
        with app.app_context():
            db.create_all()
    except Exception as err:
        logger.exception(err)


def get_context():
    """
    This context is necessary for using the flask models outside the app

    example:
        > with get_context():
        >     cls = Classifications.query.all()
    """
    flask_str = get_connection_strs().flask_str
    logger.info("Creating app context")
    from webapp.app import create_app
    app = create_app('production')
    app.config.update(
        SQLALCHEMY_DATABASE_URI=flask_str
    )
    return app.app_context()


def get_connection():
    """
    Create a connection to the MySQL database
    """
    con_str = get_connection_strs().con_str
    logger.info("Creating sql connection")
    return create_engine(con_str, pool_recycle=1, pool_timeout=57600).connect()

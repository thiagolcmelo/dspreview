# -*- coding: utf-8 -*-

# python standard
import os
import re
import string
from random import choices

# third-party imports
from sqlalchemy import create_engine

# local imports
from utils.config_helper import ConfigHelper


def rand_word(N):
    lowers = string.ascii_lowercase
    uppers = string.ascii_uppercase
    digits = string.digits
    return ''.join(choices(lowers + uppers + digits, k=N))


class SqlHelper(object):
    """
    This helper creates connections and initialize the database for the
    operational data (DCM,DSP)
    """

    def __init__(self):
        config = ConfigHelper()
        self.dbhost = os.getenv("DB_HOST") or config.get_config("DB_HOST")
        self.dbport = os.getenv("DB_PORT") or config.get_config("DB_PORT")
        self.dbuser = os.getenv("DB_USER") or config.get_config("DB_USER")
        self.dbpass = os.getenv("DB_PASS") or config.get_config("DB_PASS")
        self.dbname = os.getenv("DB_NAME") or config.get_config("DB_NAME")
        self.dbtestname = os.getenv("DB_TEST_NAME") or \
            config.get_config("DB_TEST_NAME")
        self.dbtestuser = os.getenv("DB_TEST_USER") or \
            config.get_config("DB_TEST_USER")
        self.dbtestpass = os.getenv("DB_TEST_PASS") or \
            config.get_config("DB_TEST_PASS")

        # all these values are necessary
        if not all([self.dbhost, self.dbport, self.dbuser, self.dbpass,
                    self.dbname]):
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

        self.con_str = full.format(dbuser=self.dbuser, dbpass=self.dbpass,
                                   dbhost=self.dbhost, dbport=self.dbport,
                                   dbname=self.dbname)
        self.flask_str = simple.format(dbuser=self.dbuser, dbpass=self.dbpass,
                                       dbhost=self.dbhost, dbport=self.dbport,
                                       dbname=self.dbname)
        self.test_str = simple.format(dbuser=self.dbtestuser,
                                      dbpass=self.dbtestpass,
                                      dbhost=self.dbhost, dbport=self.dbport,
                                      dbname=self.dbtestname)

    def initialize_database(self):
        # create some configuration
        if not os.path.exists("src/instance"):
            os.makedirs("src/instance")
        config_file = "src/instance/config.py"
        config_lines = []

        track_mod_line = "SQLALCHEMY_TRACK_MODIFICATIONS=False\n"
        sql_config_line = "SQLALCHEMY_DATABASE_URI='{constr}'\n"
        sql_config_line = sql_config_line.format(constr=self.flask_str)
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
        # """
        # Initialize the whole database
        # """
        # pass in test configurations
        # app = create_app('production')
        # app.config.update(
        #     SQLALCHEMY_DATABASE_URI=self.flask_str
        # )
        # with app.app_context():
        #     db.create_all()
        # set the SQLALCHEMY_DATABASE_URI for flask

        # # Then initialize things related to the operational
        # engine = create_engine(self.con_str)
        # Base.metadata.bind = engine
        # Base.metadata.create_all(engine)

    def get_connection(self):
        """
        Create a connection to the MySQL database
        """
        return create_engine(self.con_str, pool_recycle=1,
                             pool_timeout=57600).connect()

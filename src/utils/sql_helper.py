# -*- coding: utf-8 -*-

# python standard
import sys
import os
import re
import json
import subprocess

# third-party imports
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, validates
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, Index
from sqlalchemy.sql import func

Base = declarative_base()

class DCM(Base):
    """
    Create a DCM table
    """

    __tablename__ = 'dcm_raw'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    campaign_id = Column(Integer, nullable=False)
    campaign = Column(String(75), nullable=False)
    placement_id = Column(Integer, nullable=False)
    placement = Column(String(75), nullable=False)
    impressions = Column(Float, nullable=False)
    clicks = Column(Integer, nullable=False)
    reach = Column(Float, nullable=False)
    brand = Column(String(25), nullable=False)
    sub_brand = Column(String(25), nullable=False)
    dsp = Column(String(25), nullable=False)
    ad_type = Column(String(25), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

# Create an index to not allow reapeated values on these dimensions
Index('dcm_index', DCM.date, DCM.brand, DCM.sub_brand, DCM.campaign_id, DCM.campaign, \
    DCM.placement_id, DCM.placement, DCM.dsp, DCM.ad_type, unique=True)


class DSP(Base):
    """
    Create a DSP table
    """

    __tablename__ = 'dsp_raw'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    campaign_id = Column(Integer, nullable=False)
    campaign = Column(String(75), nullable=False)
    impressions = Column(Float, nullable=False)
    clicks = Column(Integer, nullable=False)
    cost = Column(Float, nullable=False)
    brand = Column(String(25), nullable=False)
    sub_brand = Column(String(25), nullable=False)
    dsp = Column(String(25), nullable=False)
    ad_type = Column(String(25), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

# Create an index to not allow reapeated values on these dimensions
Index('dsp_index', DSP.date, DSP.brand, DSP.sub_brand, DSP.campaign_id, DSP.campaign, DSP.dsp, DSP.ad_type, unique=True)


class Report(Base):
    """
    Create a Report table
    """

    __tablename__ = 'report'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    brand = Column(String(25), nullable=False)
    sub_brand = Column(String(25), nullable=False)
    ad_campaign_id = Column(Integer, nullable=False)
    ad_campaign = Column(String(75), nullable=False)
    dsp = Column(String(25), nullable=False)
    dsp_campaign_id = Column(Integer, nullable=False)
    dsp_campaign = Column(String(75), nullable=False)
    ad_impressions = Column(Float, nullable=False)
    ad_clicks = Column(Integer, nullable=False)
    ad_reach = Column(Float, nullable=False)
    dsp_impressions = Column(Float, nullable=False)
    dsp_clicks = Column(Integer, nullable=False)
    dsp_cost = Column(Float, nullable=False)    
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

# Create an index to not allow reapeated values on these dimensions
Index('report_index', Report.date, Report.brand, Report.sub_brand, Report.ad_campaign_id, \
    Report.ad_campaign, Report.dsp, Report.dsp_campaign_id, Report.dsp_campaign, unique=True)


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
        self.dbtest = os.getenv("DB_TEST") or config.get_config("DB_TEST")

        # all these values are necessary
        if not all([self.dbhost, self.dbport, self.dbuser, self.dbpass, self.dbname]):
            raise Exception("""It is not possible to connect in the MySQL database
                            All of the following environment variables must be set
                            - DB_HOST
                            - DB_PORT
                            - DB_USER
                            - DB_PASS
                            - DB_NAME
                            - DB_TEST (for development only)
                            another possibility woul be the .dspreview.json ;)""")
        
        # connection string for operational use
        self.con_str = "mysql+mysqldb://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}".format(\
            dbuser=self.dbuser, dbpass=self.dbpass, dbhost=self.dbhost, \
            dbport=self.dbport, dbname=self.dbname)
        self.flask_str = "mysql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}".format(\
            dbuser=self.dbuser, dbpass=self.dbpass, dbhost=self.dbhost, \
            dbport=self.dbport, dbname=self.dbname)
        self.test_str = "mysql://{dbuser}:{dbpass}@{dbhost}:{dbport}/{dbname}".format(\
            dbuser=self.dbuser, dbpass=self.dbpass, dbhost=self.dbhost, \
            dbport=self.dbport, dbname=self.dbtest)
        
    def initialize_database(self):
        """
        Initialize the whole database
        """
        # set the SQLALCHEMY_DATABASE_URI for flask
        config_file = "src/webapp/instance/config.py"
        config_lines = []
        sql_config_line = "SQLALCHEMY_DATABASE_URI = '{constr}'\n".format(constr=self.flask_str)
        with open(config_file, "r") as f:
            config_lines = f.readlines()
        has_uri_cfg = False
        for i in range(len(config_lines)):
            if re.search(".*SQLALCHEMY_DATABASE_URI.*", config_lines[i]):
                config_lines[i] = sql_config_line
                has_uri_cfg = True
        if not has_uri_cfg:
            config_lines.append(sql_config_line)
        with open(config_file, "w") as f:
            for l in config_lines:
                f.write(l)
        # First initialize things related to Flask
        #sys.path.append("src/webapp")


        # Then initialize things related to the operational
        engine = create_engine(self.con_str)
        Base.metadata.bind = engine
        Base.metadata.create_all(engine)
    
    def get_connection(self):
        """
        Create a connection to the MySQL database
        """
        return create_engine(self.con_str, pool_recycle=1, pool_timeout=57600).connect()

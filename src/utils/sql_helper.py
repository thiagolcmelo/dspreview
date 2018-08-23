# -*- coding: utf-8 -*-

import os
import json

from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, validates
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, Index
from sqlalchemy.sql import func

Base = declarative_base()

class DCM(Base):
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

Index('dcm_index', DCM.date, DCM.brand, DCM.sub_brand, DCM.campaign_id, DCM.campaign, \
    DCM.placement_id, DCM.placement, DCM.dsp, DCM.ad_type, unique=True)


class DSP(Base):
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

Index('dsp_index', DSP.date, DSP.brand, DSP.sub_brand, DSP.campaign_id, DSP.campaign, DSP.dsp, DSP.ad_type, unique=True)


class Report(Base):
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

Index('report_index', Report.date, Report.brand, Report.sub_brand, Report.ad_campaign_id, \
    Report.ad_campaign, Report.dsp, Report.dsp_campaign_id, Report.dsp_campaign, unique=True)


class SqlHelper(object):
    def __init__(self):
        self.dbhost = os.getenv("DB_HOST")
        self.dbport = os.getenv("DB_PORT")
        self.dbuser = os.getenv("DB_USER")
        self.dbpass = os.getenv("DB_PASS")
        self.dbname = os.getenv("DB_NAME")

        user_home = os.path.expanduser("~")
        config_file = "{}/.dspreview.json".format(user_home)
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                try:
                    data = json.loads(f.read())
                    self.dbhost = self.dbhost or data.get("DB_HOST")
                    self.dbport = self.dbport or data.get("DB_PORT")
                    self.dbuser = self.dbuser or data.get("DB_USER")
                    self.dbpass = self.dbpass or data.get("DB_PASS")
                    self.dbname = self.dbname or data.get("DB_NAME")
                except:
                    raise Exception("Invalid .dspreview.json file.")
    
        if not all([self.dbhost, self.dbport, self.dbuser, self.dbpass, self.dbname]):
            raise Exception("""It is not possible to connect in the MySQL database
                            All of the following environment variables must be set
                            - DB_HOST
                            - DB_PORT
                            - DB_USER
                            - DB_PASS
                            - DB_NAME
                            another possibility woul be the .dspreview.json ;)""")
        
        self.con_str = 'mysql+mysqldb://%s:%s@%s:%s/%s' % (self.dbuser, \
            self.dbpass, self.dbhost, self.dbport, self.dbname)
        
    def initialize_database(self):
        engine = create_engine(self.con_str)
        Base.metadata.bind = engine
        Base.metadata.create_all(engine)
    
    def get_connection(self):
        return create_engine(self.con_str, pool_recycle=1, pool_timeout=57600).connect()

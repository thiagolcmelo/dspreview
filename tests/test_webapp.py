# -*- coding: utf-8 -*-

# python standard
import sys
from datetime import datetime
sys.path.insert(0, "./src")
sys.path.insert(0, "./src/webapp")

# third-party imports
from flask_testing import TestCase
from flask import abort, url_for

# local imports
from utils.sql_helper import get_connection_strs
from webapp.app import create_app, db
from webapp.app.models import User, Classification
from webapp.app.models import DCMRaw, DCM, DSPRaw, DSP, Report


class TestBase(TestCase):

    def create_app(self):
        # pass in test configurations
        config_name = 'testing'
        app = create_app(config_name)
        app.config.update(
            SQLALCHEMY_DATABASE_URI=get_connection_strs().test_str
        )

        return app

    def setUp(self):
        # it needs to be here
        """
        Will be called before every test
        """

        db.create_all()

        # create test admin user
        admin = User(username="admin", password="admin2018", is_admin=True)

        # create test non-admin user
        normal_user = User(username="normal_user", password="test2018")

        # create test for classification
        classification = Classification(pattern=".*", brand="some brand",
                                        sub_brand="some sub brand",
                                        dsp="some dsp",
                                        use_campaign_id=True,
                                        use_campaign=False,
                                        use_placement_id=False,
                                        use_placement=True)

        # create a test for dcm
        dcm = DCM(date=datetime.now(), campaign_id=85989,
                  campaign="some campaign", placement_id=54786,
                  placement="some placement", impressions=87884.8,
                  clicks=874, reach=7581.5, brand="some brand",
                  sub_brand="some sub brand", dsp="some dsp")

        # create a test for dcm raw
        dcm_raw = DCMRaw(date=datetime.now(), campaign_id=85989,
                         campaign="some campaign", placement_id=54786,
                         placement="some placement", impressions=87884.8,
                         clicks=874, reach=7581.5)

        # create a test for dsp
        dsp = DSP(date=datetime.now(), campaign_id=85989,
                  campaign="some campaign", impressions=87884.8,
                  clicks=874, cost=7581.5, brand="some brand",
                  sub_brand="some sub brand", dsp="some dsp")

        # create a test for dsp raw
        dsp_raw = DSPRaw(date=datetime.now(), campaign_id=85989,
                         campaign="some campaign", impressions=87884.8,
                         clicks=874, cost=7581.5)

        report = Report(date=datetime.now(), brand="some brand",
                        sub_brand="some sub brand", ad_campaign_id=89865,
                        ad_campaign="some campaign", dsp="some dsp",
                        dsp_campaign_id=87897, dsp_campaign="some campaign",
                        ad_impressions=783546.8, ad_clicks=578698,
                        ad_reach=879.9, dsp_impressions=7987.9,
                        dsp_clicks=7897, dsp_cost=578979.8)

        # save users to database
        db.session.add(admin)
        db.session.add(normal_user)
        db.session.add(classification)
        db.session.add(dcm)
        db.session.add(dcm_raw)
        db.session.add(dsp)
        db.session.add(dsp_raw)
        db.session.add(report)
        db.session.commit()

    def tearDown(self):
        """
        Will be called after every test
        """

        db.session.remove()
        db.drop_all()


class TestModels(TestBase):

    def test_user_model(self):
        """
        Test number of records in User table
        """
        self.assertEqual(User.query.count(), 2)

    def test_classification_model(self):
        """
        Test number of records in Classification table
        """
        self.assertEqual(Classification.query.count(), 1)

    def test_dsp_model(self):
        """
        Test number of records in DSP table
        """
        self.assertEqual(DSP.query.count(), 1)

    def test_dsp_raw_model(self):
        """
        Test number of records in DSPRaw table
        """
        self.assertEqual(DSPRaw.query.count(), 1)

    def test_dcm_model(self):
        """
        Test number of records in DCM table
        """
        self.assertEqual(DCM.query.count(), 1)

    def test_dcm_raw_model(self):
        """
        Test number of records in DCMRaw table
        """
        self.assertEqual(DCMRaw.query.count(), 1)

    def test_report_model(self):
        """
        Test number of records in DCMRaw table
        """
        self.assertEqual(Report.query.count(), 1)

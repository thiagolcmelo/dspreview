# -*- coding: utf-8 -*-
"""
"""

# python standard
import sys
sys.path.insert(0, "./src")
sys.path.insert(0, "./src/webapp")

# third-party imports
from flask_testing import TestCase
from flask import abort, url_for

# local imports
from utils.sql_helper import SqlHelper

from webapp.app import create_app, db
from webapp.app.models import User


class TestBase(TestCase):

    def create_app(self):
        sqlhelper = SqlHelper()

        # pass in test configurations
        config_name = 'testing'
        app = create_app(config_name)
        app.config.update(
            SQLALCHEMY_DATABASE_URI=sqlhelper.test_str
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

        # save users to database
        db.session.add(admin)
        db.session.add(normal_user)
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

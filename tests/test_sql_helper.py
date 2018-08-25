# -*- coding: utf-8 -*-

# python standard
import os
import unittest
from unittest.mock import patch
import logging

# local imports
from utils.sql_helper import rand_word, get_connection_strs
from utils.config_helper import ConfigHelper

logging.disable(logging.CRITICAL)


class TestSqlHelper(unittest.TestCase):

    def test_rand_word(self):
        self.assertEqual(len(rand_word(20)), 20, "Error in secret size")

    @patch.dict(os.environ, {
        "DB_HOST": "v1",
        "DB_PORT": "v2",
        "DB_USER": "v3",
        "DB_PASS": "v4",
        "DB_NAME": "v5",
        "DB_TEST_NAME": "v6",
        "DB_TEST_USER": "v7",
        "DB_TEST_PASS": "v8"
    })
    def test_connection_strs_normal(self):
        strs = get_connection_strs()
        full = "mysql+mysqldb://v3:v4@v1:v2/v5"
        simple = "mysql://v3:v4@v1:v2/v5"
        simple_test = "mysql://v7:v8@v1:v2/v6"
        self.assertEqual(strs.con_str, full,
                         "Error in main connection string")
        self.assertEqual(strs.flask_str, simple,
                         "Error in flask connection string")
        self.assertEqual(strs.test_str, simple_test,
                         "Error in test connection string")

    @patch.dict(os.environ, {
        "DB_HOST": "",
        "DB_PORT": "",
        "DB_USER": "",
        "DB_PASS": "",
        "DB_NAME": "",
        "DB_TEST_NAME": "",
        "DB_TEST_USER": "",
        "DB_TEST_PASS": ""
    })
    @patch.object(ConfigHelper, 'get_config')
    def test_search_in_file_valid(self, mock_get_config):
        mock_get_config.return_value = "sv"
        strs = get_connection_strs()
        full = "mysql+mysqldb://sv:sv@sv:sv/sv"
        self.assertEqual(strs.con_str, full,
                         "Error in main connection string")

    @patch.dict(os.environ, {
        "DB_HOST": "",
        "DB_PORT": "",
        "DB_USER": "",
        "DB_PASS": "",
        "DB_NAME": "",
        "DB_TEST_NAME": "",
        "DB_TEST_USER": "",
        "DB_TEST_PASS": ""
    })
    @patch.object(ConfigHelper, 'get_config')
    def test_search_in_file_invalid(self, mock_get_config):
        mock_get_config.return_value = ""
        with self.assertRaises(Exception):
            get_connection_strs()


if __name__ == "__main__":
    unittest.main()

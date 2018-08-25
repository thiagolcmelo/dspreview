# -*- coding: utf-8 -*-

# python standard
import unittest
import logging
import unittest.mock as mock
from unittest.mock import patch, mock_open

# local imports
from utils.config_helper import ConfigHelper

logging.disable(logging.CRITICAL)


class TestConfigHelper(unittest.TestCase):

    @mock.patch('utils.config_helper.os.path.exists')
    def test_with_invalid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data = "{..."
        mo = mock_open(read_data=read_data)
        with patch('utils.config_helper.open', mo):
            with self.assertRaises(Exception):
                ConfigHelper()

    @mock.patch('utils.config_helper.os.path.exists')
    def test_with_valid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data = """{"key":"value"}"""
        mo = mock_open(read_data=read_data)
        with patch('utils.config_helper.open', mo):
            try:
                ConfigHelper()
            except Exception:
                self.fail("""It shouldn't raise an exception since
                we are feeding a valid file""")

    @mock.patch('utils.config_helper.os.path.exists')
    def test_get_value(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data = """{"key":"value"}"""
        mo = mock_open(read_data=read_data)
        with patch('utils.config_helper.open', mo):
            config = ConfigHelper()
            self.assertEqual(config.get_config("key"),
                             "value", """Is is not finding the correct
                                        key-value config""")

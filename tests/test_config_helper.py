# -*- coding: utf-8 -*-
"""
"""

# python standard
import os
import unittest
import unittest.mock as mock
from unittest.mock import patch, mock_open

# local imports
from src.utils.bucket_helper import BucketHelper
from src.utils.config_helper import ConfigHelper

class TestConfigHelper(unittest.TestCase):

    @mock.patch('src.utils.config_helper.os.path.exists')
    def test_with_invalid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data="{..."
        mo = mock_open(read_data=read_data)
        with patch('src.utils.config_helper.open', mo) as m:
            # very bad approach...
            working = True
            try:
                config = ConfigHelper()
                working = False
            except:
                pass

            if not working:
                self.assertFalse(True, """It should raise an exception since 
                we are feeding a invalid file""")
    
    @mock.patch('src.utils.config_helper.os.path.exists')
    def test_with_valid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data="""{"key":"value"}"""
        mo = mock_open(read_data=read_data)
        with patch('src.utils.config_helper.open', mo) as m:
            try:
                config = ConfigHelper()
            except:
                self.assertFalse(True, """It shouldn't raise an exception since 
                we are feeding a valid file""")
    
    @mock.patch('src.utils.config_helper.os.path.exists')
    def test_get_value(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data="""{"key":"value"}"""
        mo = mock_open(read_data=read_data)
        with patch('src.utils.config_helper.open', mo) as m:
            config = ConfigHelper()
            self.assertTrue(config.get_config("key")=="value", """Is is not
                finding the correct key-value config""")

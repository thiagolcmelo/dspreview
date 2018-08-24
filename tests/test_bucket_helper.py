# -*- coding: utf-8 -*-

# python standard
import os
import unittest
import unittest.mock as mock
from unittest.mock import patch, mock_open

# local imports
from utils.bucket_helper import BucketHelper
from utils.config_helper import ConfigHelper


class TestBucketHelper(unittest.TestCase):

    @mock.patch.dict(os.environ, {
        "GCP_BUCKET": "some_value",
        "GOOGLE_APPLICATION_CREDENTIALS": "some_value"
    })
    def test_instance_with_envs(self):
        bh = BucketHelper()
        self.assertIsInstance(bh, BucketHelper)

    @mock.patch.dict(os.environ, {
        "GCP_BUCKET": '',
        "GOOGLE_APPLICATION_CREDENTIALS": ''
    })
    @mock.patch('utils.bucket_helper.os.path.exists')
    def test_instance_with_valid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data = """{
                    "GOOGLE_APPLICATION_CREDENTIALS": "some_value",
                    "GCP_BUCKET": "dspreview"
                    }"""
        mo = mock_open(read_data=read_data)
        with patch('utils.bucket_helper.open', mo):
            bh = BucketHelper()
            self.assertIsInstance(bh, BucketHelper)

    @mock.patch.dict(os.environ, {
        "GCP_BUCKET": '',
        "GOOGLE_APPLICATION_CREDENTIALS": ''
    })
    @patch.object(ConfigHelper, 'get_config')
    def test_instance_with_invalid_file(self, mock_config):
        mock_config.return_value = None

        # very bad approach...
        working = True
        try:
            BucketHelper()
            working = False
        except Exception:
            pass

        if not working:
            self.assertFalse(True, """It should raise an exception since
            we are feeding a invalid configuration""")

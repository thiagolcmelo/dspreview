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
    @mock.patch('src.utils.bucket_helper.os.path.exists')
    def test_instance_with_valid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data="""{
                    "GOOGLE_APPLICATION_CREDENTIALS": "some_value",
                    "GCP_BUCKET": "dspreview"
                    }"""
        mo = mock_open(read_data=read_data)
        with patch('src.utils.bucket_helper.open', mo) as m:
            bh = BucketHelper()
            self.assertIsInstance(bh, BucketHelper)
    
    @mock.patch.dict(os.environ, {
        "GCP_BUCKET": '',
        "GOOGLE_APPLICATION_CREDENTIALS": ''
    })
    @mock.patch('src.utils.bucket_helper.os.path.exists')
    def test_instance_with_invalid_file(self, mock_path_exists):
        mock_path_exists.return_value = True
        read_data=""
        mo = mock_open(read_data=read_data)
        with patch('src.utils.bucket_helper.open', mo) as m:
            with self.assertRaises(Exception) as context:
                bh = BucketHelper()
        
    # def test_list_files(self):
    #     """ it relies deeply in google's api """

    # def test_get_csv_file(self):
    #     """ it relies deeply in google's api """

    # def test_archive_csv_file(self):
    #     """ it relies deeply in google's api """

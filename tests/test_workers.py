# -*- coding: utf-8 -*-
"""
"""

import os
import unittest
import unittest.mock as mock
from unittest.mock import patch, mock_open

import pandas as pd

from src.utils.bucket_helper import BucketHelper
from src.workers.worker import Worker, DcmWorker, DspWorker

class TestWorkers(unittest.TestCase):
    def setUp(self):
        self.worker = Worker()

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_download(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = [
            {
                'name': 'archive/', 
                'contentType': 'application/x-www-form-urlencoded;charset=UTF-8', 
                'size': '0'
            }, {
                'name': 'dbm.csv', 
                'contentType': 'text/csv', 
                'size': '21645'
            }, {
                'name': 'dcm.csv', 
                'contentType': 'text/csv', 
                'size': '48297'
            }, {
                'name': 'mediamath.csv', 
                'contentType': 'text/csv', 
                'size': '22989'
            }
        ]
        mock_get_file.return_value = pd.DataFrame([{'a':1,'b':3}])
        
        worker = Worker()
        worker.download('^dcm.*')
        mock_get_file.assert_called_with('dcm.csv')

        worker = Worker()
        worker.download('^dbm.*')
        mock_get_file.assert_called_with('dbm.csv')

        worker = Worker()
        worker.download('^mediamath.*')
        mock_get_file.assert_called_with('mediamath.csv')

    # def test_parse(self):

    # def test_upload(self):
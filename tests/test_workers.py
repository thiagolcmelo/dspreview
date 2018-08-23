# -*- coding: utf-8 -*-
"""
Test workers, how they should download stuff, how they should parse .csv
files, and how they should store it in the database.
"""

# python standard
import os
import unittest
import unittest.mock as mock
from unittest.mock import patch, mock_open

# third-party imports
import pandas as pd
import numpy as np

# local imports
from utils.bucket_helper import BucketHelper
from workers.worker import Worker, DcmWorker, DspWorker, generate_report


class TestWorkers(unittest.TestCase):
    def setUp(self):
        """
        Some examples of information that could be found in the bucket
        """

        # a fake list of files
        self.fake_list = [
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

        # first two lines of a good dcm's file
        self.good_dcm = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "48988.5",
            "clicks": "118",
            "reach": "48.24"
        }, {
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "198706",
            "placement": "dbm_youtube",
            "impressions": "55509.0",
            "clicks": "116",
            "reach": "42.49"
        }])

        # first two line of a file with data missing, will be filled with zero
        self.missing_fine_dcm = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": np.nan,
            "clicks": "",
            "reach": ""
        }, {
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "198706",
            "placement": "dbm_youtube",
            "impressions": "",
            "clicks": "116",
            "reach": "42.49"
        }])

        # first two line of a file with data missing, in this case, dimensions
        # are missing
        self.missing_bad_dcm = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "48988.5",
            "clicks": "118",
            "reach": "48.24"
        }, {
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "198706",
            "placement": "",
            "impressions": "55509.0",
            "clicks": "116",
            "reach": "42.49"
        }])

        # a file with a line repeated, it is expected to be deduplicated
        self.duplicate_dcm = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "48988.5",
            "clicks": "118",
            "reach": "48.24"
        }, {
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "48988.5",
            "clicks": "118",
            "reach": "48.24"
        }])

        # a file with same dimensions values and different metrics values
        self.bad_dcm = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "48988.5",
            "clicks": "118",
            "reach": "48.24"
        }, {
            "date": "2018-01-01",
            "campaign_id": "86394",
            "campaign": "acme_asprin",
            "placement_id": "267821",
            "placement": "mediamath_programmatic",
            "impressions": "648712.9",
            "clicks": "876",
            "reach": "87.54"
        }])

        # first two lines of a good dsp's file
        self.good_dsp = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6011070",
            "clicks": "11889",
            "cost": "40334.2797",
        }, {
            "date": "2018-01-01",
            "campaign_id": "111493",
            "campaign": "acme_car_youtube",
            "impressions": "6585720",
            "clicks": "29843",
            "cost": "58547.0508",
        }])

        # a file with a line repeated, it is expected to be deduplicated
        self.duplicate_dsp = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6011070",
            "clicks": "11889",
            "cost": "40334.2797",
        }, {
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6011070",
            "clicks": "11889",
            "cost": "40334.2797",
        }])

        # first two line of a file with data missing, will be filled with zero
        self.missing_fine_dsp = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6011070",
            "clicks": "",
            "cost": "40334.2797",
        }, {
            "date": "2018-01-01",
            "campaign_id": "111493",
            "campaign": "acme_car_youtube",
            "impressions": "",
            "clicks": "29843",
            "cost": "",
        }])

        # first two line of a file with data missing, in this case, dimensions
        # are missing
        self.missing_bad_dsp = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "",
            "impressions": "6011070",
            "clicks": "11889",
            "cost": "40334.2797",
        }, {
            "date": "2018-01-01",
            "campaign_id": "111493",
            "campaign": "acme_car_youtube",
            "impressions": "6585720",
            "clicks": "29843",
            "cost": "58547.0508",
        }])

        # a file with same dimensions values and different metrics values
        self.bad_dsp = pd.DataFrame([{
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6011070",
            "clicks": "11889",
            "cost": "40334.2797",
        }, {
            "date": "2018-01-01",
            "campaign_id": "128115",
            "campaign": "acme_asprin_youtube",
            "impressions": "6585720",
            "clicks": "29843",
            "cost": "58547.0508",
        }])

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_download(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = pd.DataFrame([{'a': 1, 'b': 3}])

        worker = Worker()
        worker.download('^dcm.*')
        mock_get_file.assert_called_with('dcm.csv')

        worker = Worker()
        worker.download('^dbm.*')
        mock_get_file.assert_called_with('dbm.csv')

        worker = Worker()
        worker.download('^mediamath.*')
        mock_get_file.assert_called_with('mediamath.csv')

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_good(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.good_dcm

        worker = DcmWorker()
        worker.download('^dcm.*')
        worker.parse()
        self.assertTrue(len(worker.dfs) == 1, "There should be one DataFrame!")

        parsed_df = worker.dfs[0]
        columns_has = parsed_df.columns
        columns_should = [
            'date',
            'campaign_id',
            'campaign',
            'placement_id',
            'placement',
            'impressions',
            'clicks',
            'reach',
            'brand',
            'sub_brand',
            'dsp',
            'ad_type',
        ]

        self.assertTrue(sorted(columns_has) == sorted(columns_should), """
            Missing some column! They should be:
                - date
                - campaign_id
                - campaign
                - placement_id
                - placement
                - impressions
                - clicks
                - reach
                - brand
                - sub_brand
                - dsp
                - ad_type
            """)

        self.assertTrue(parsed_df.date.dtype == np.dtype('datetime64[ns]'),
                        """date should be parsed to np.datetime64""")
        self.assertTrue(parsed_df.campaign_id.dtype == np.dtype('int64'),
                        """campaign_id should be parsed to np.int64""")
        self.assertTrue(parsed_df.placement_id.dtype == np.dtype('int64'),
                        """placement_id should be parsed to np.int64""")
        self.assertTrue(parsed_df.impressions.dtype == np.dtype('float64'),
                        """impressions should be parsed to np.float64""")
        self.assertTrue(parsed_df.clicks.dtype == np.dtype('int64'),
                        """clicks should be parsed to np.int64""")
        self.assertTrue(parsed_df.reach.dtype == np.dtype('float64'),
                        """reach should be parsed to np.float64""")

        self.assertTrue(parsed_df.loc[0].brand == "acme", "Invalid brand")
        self.assertTrue(parsed_df.loc[0].sub_brand ==
                        "asprin", "Invalid sub_brand")
        self.assertTrue(parsed_df.loc[0].dsp == "mediamath", "Invalid dsp")
        self.assertTrue(parsed_df.loc[0].ad_type ==
                        "programmatic", "Invalid dsp")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_missing(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.missing_fine_dcm
        worker = DcmWorker()
        worker.download()
        worker.parse()
        self.assertTrue(len(worker.dfs) == 1, "There should be on DataFrame!")
        self.assertTrue(worker.dfs[0].loc[0].clicks ==
                        0, "Clicks should be zero")
        self.assertTrue(worker.dfs[0].loc[0].reach ==
                        0, "Reach should be zero")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_missing_bad(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.missing_bad_dcm
        worker = DcmWorker()
        worker.download()

        # very bad approach...
        working = True
        try:
            worker.parse()
            working = False
        except:
            pass

        if not working:
            self.assertFalse(True, """It should raise an exception since we 
                are feeding the worker with a bad dataframe. This dataframe 
                has rows with missing placement or campaign""")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_duplicate(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.duplicate_dcm
        worker = DcmWorker()
        worker.download()
        worker.parse()
        self.assertTrue(worker.dfs[0].shape[0] == 1, """The duplicate rows should
            be removed!""")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_bad_values(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.bad_dcm
        worker = DcmWorker()
        worker.download()

        # very bad approach...
        working = True
        try:
            worker.parse()
            working = False
        except:
            pass

        if not working:
            self.assertFalse(True, """It should raise an exception since we 
                are feeding the worker with a bad dataframe. This dataframe 
                has rows with the same dimensions' combination and
                some difference in the metrics values, this combination should be
                unique""")
    
    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dsp_good(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.good_dsp

        worker = DspWorker('dbm')
        worker.download()
        worker.parse()
        self.assertTrue(len(worker.dfs) == 1, "There should be one DataFrame!")

        parsed_df = worker.dfs[0]
        columns_has = parsed_df.columns
        columns_should = [
            'date',
            'campaign_id',
            'campaign',
            'impressions',
            'clicks',
            'cost',
            'brand',
            'sub_brand',
            'dsp',
            'ad_type',
        ]

        self.assertTrue(sorted(columns_has) == sorted(columns_should), """
            Missing some column! They should be:
                - date
                - campaign_id
                - campaign
                - impressions
                - clicks
                - cost
                - brand
                - sub_brand
                - dsp
                - ad_type
            """)

        self.assertTrue(parsed_df.date.dtype == np.dtype('datetime64[ns]'),
                        """date should be parsed to np.datetime64""")
        self.assertTrue(parsed_df.campaign_id.dtype == np.dtype('int64'),
                        """campaign_id should be parsed to np.int64""")
        self.assertTrue(parsed_df.impressions.dtype == np.dtype('float64'),
                        """impressions should be parsed to np.float64""")
        self.assertTrue(parsed_df.clicks.dtype == np.dtype('int64'),
                        """clicks should be parsed to np.int64""")
        self.assertTrue(parsed_df.cost.dtype == np.dtype('float64'),
                        """reach should be parsed to np.float64""")

        self.assertTrue(parsed_df.loc[0].brand == "acme", "Invalid brand")
        self.assertTrue(parsed_df.loc[0].sub_brand ==
                        "asprin", "Invalid sub_brand")
        self.assertTrue(parsed_df.loc[0].dsp == "dbm", "Invalid dsp")
        self.assertTrue(parsed_df.loc[0].ad_type ==
                        "youtube", "Invalid dsp")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dsp_missing(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.missing_fine_dsp
        worker = DspWorker('dbm')
        worker.download()
        worker.parse()
        self.assertTrue(len(worker.dfs) == 1, "There should be on DataFrame!")
        self.assertTrue(worker.dfs[0].loc[0].clicks ==
                        0, "Clicks should be zero")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dsp_missing_bad(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.missing_bad_dsp
        worker = DspWorker('dbm')
        worker.download()

        # very bad approach...
        working = True
        try:
            worker.parse()
            working = False
        except:
            pass

        if not working:
            self.assertFalse(True, """It should raise an exception since we 
                are feeding the worker with a bad dataframe. This dataframe 
                has rows with missing placement or campaign""")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dcm_duplicate(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.duplicate_dsp
        worker = DspWorker('dbm')
        worker.download()
        worker.parse()
        self.assertTrue(worker.dfs[0].shape[0] == 1, """The duplicate rows should
            be removed!""")

    @patch.object(BucketHelper, 'list_files')
    @patch.object(BucketHelper, 'get_csv_file')
    def test_parse_dsp_bad_values(self, mock_get_file, mock_list_files):
        mock_list_files.return_value = self.fake_list
        mock_get_file.return_value = self.bad_dsp
        worker = DspWorker('dbm')
        worker.download()

        # very bad approach...
        working = True
        try:
            worker.parse()
            working = False
        except:
            pass

        if not working:
            self.assertFalse(True, """It should raise an exception since we 
                are feeding the worker with a bad dataframe. This dataframe 
                has rows with the same dimensions' combination and
                some difference in the metrics values, this combination should be
                unique""")

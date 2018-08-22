# -*- coding: utf-8 -*-
"""
"""

import re

import pandas as pd
import numpy as np
from pandas.io import sql
import MySQLdb

from src.utils.bucket_helper import BucketHelper
from src.utils.sql_helper import SqlHelper


class Worker(object):
    def __init__(self):
        self.bucket = BucketHelper()
        self.dfs = []
        self.pattern = None
        self.dsp = None

    def download(self, pattern=None):
        """this function checks whether there are files in the GCP bucket 
        with the given `pattern`. If there are, those files will be 
        downloaded

        Params
        ------
        pattern : string
            it is a regex patter for searching in the bucket's files, in case
            of none, it will try to use a pre configured patter, but if it
            was not set rpeviouly, an exception will raise

        Returns
        -------
        The object instace for use in chain calls
        """
        pattern = pattern or self.pattern
        self.dfs = []
        files = self.bucket.list_files()
        for f in files:
            if re.search(pattern, f['name']):
                self.dfs.append(self.bucket.get_csv_file(f['name']))
        return self

    def upload(self):
        sqlhelper = SqlHelper()
        con = sqlhelper.get_connection()
        table = "%s_raw" % ('dsp' if self.dsp else 'dcm')
        table_temp = "%s_temp" % table
        for df in self.dfs:
            df.to_sql(con=con, name=table_temp,
                      if_exists='replace', index=False)
            update_part = []
            for c in self.metrics:
                update_part.append("{table}.{fld}={temp}.{fld}".format(
                    table=table, temp=table_temp, fld=c))
            update_part.append(
                "{table}.updated_at=CURRENT_TIMESTAMP()".format(table=table))
            update_part = ",".join(update_part)
            all_columns = ",".join(self.dimensions+self.metrics)
            connection = con.connect()
            connection.execute("""INSERT INTO {table} ({all_cols})
                                    SELECT {all_cols}
                                    FROM {temp} ON DUPLICATE KEY
                                    UPDATE
                                    {updates}
                                    """.format(table=table, temp=table_temp, \
                                    updates=update_part, all_cols=all_columns))
            connection.execute("DROP TABLE {temp}".format(temp=table_temp))
        return self


class DcmWorker(Worker):
    def __init__(self):
        super(DcmWorker, self).__init__()
        self.pattern = '.*dcm.*'
        self.dimensions = [
            'date', 
            'brand', 
            'sub_brand', 
            'campaign_id',
            'campaign',
            'placement_id',
            'placement',
            'dsp', 
            'ad_type'
        ]
        self.metrics = [
            'impressions',
            'clicks',
            'reach'
        ]

    def find_brand(self, campaign):
        return campaign.split('_')[0]

    def find_sub_brand(self, campaign):
        return campaign.split('_')[1]

    def find_dsp(self, placement):
        return placement.split('_')[0]

    def find_ad_type(self, placement):
        return placement.split('_')[1]

    def parse(self):
        """this function should apply all the proper parsing operations in the
        downloaded dataframes, setting values as dates, integers, or floats

        it is also responsible for figuring out the name of the brand, sub_brand,
        the dsp and the ad_type names

        Returns
        -------
        The object instace for use in chain calls
        """
        for i, df in enumerate(self.dfs):
            df.date = pd.to_datetime(df.date)
            df.campaign_id = df.campaign_id.replace(
                '', '0', regex=True).fillna(0).astype(int)
            df.placement_id = df.placement_id.replace(
                '', '0', regex=True).fillna(0).astype(int)
            df.impressions = df.impressions.replace(
                '', '0', regex=True).fillna(0).astype(float)
            df.clicks = df.clicks.replace(
                '', '0', regex=True).fillna(0).astype(int)
            df.reach = df.reach.replace(
                '', '0', regex=True).fillna(0).astype(float)
            df = df.assign(brand=df.campaign.apply(self.find_brand)) \
                .assign(sub_brand=df.campaign.apply(self.find_sub_brand)) \
                .assign(dsp=df.placement.apply(self.find_dsp)) \
                .assign(ad_type=df.placement.apply(self.find_ad_type))
            df.drop_duplicates(inplace=True)

            prob_df = df.groupby(self.dimensions).agg({
                "impressions": "sum",
                "clicks": "sum",
                "reach": "sum", # WARNING! CALCULATED METRIC!!!
            }).reset_index()

            if prob_df.shape[0] != df.shape[0]:
                raise Exception("""Problematic rows!
                    The dimensions combinations should be unique
                    It is not a problem of suplicate rows, it is a problem
                    of some combination having different values
                    of impressions, clicks, or cost.""")

            self.dfs[i] = df.drop_duplicates()
        return self


class DspWorker(Worker):
    def __init__(self, dsp):
        """
        Params
        ------
        dsp : string
            a DSP name, withou formating, spaces or special chars. this will
            be used as pattern for searching advertisenments of this DSP in the
            csv files
        """
        super(DspWorker, self).__init__()
        self.dsp = dsp
        self.pattern = '.*%s.*' % dsp
        self.dimensions = [
            'date', 
            'brand', 
            'sub_brand', 
            'dsp', 
            'campaign', 
            'campaign_id', 
            'ad_type'
        ]
        self.metrics = [
            'impressions',
            'clicks',
            'cost'
        ]

    def find_brand(self, campaign):
        return campaign.split('_')[0]

    def find_sub_brand(self, campaign):
        return campaign.split('_')[1]

    def find_ad_type(self, campaign):
        return campaign.split('_')[2]

    def parse(self):
        """this function should apply all the proper parsing operations in the
        downloaded dataframes, setting values as dates, integers, or floats

        it is also responsible for figuring out the name of the brand, sub_brand,
        the dsp and the ad_type names

        Returns
        -------
        The object instace for use in chain calls
        """

        for i, df in enumerate(self.dfs):
            df.date = pd.to_datetime(df.date)
            df.campaign_id = df.campaign_id.replace(
                '', '0', regex=True).fillna(0).astype(int)
            df.impressions = df.impressions.replace(
                '', '0', regex=True).fillna(0).astype(float)
            df.clicks = df.clicks.replace(
                '', '0', regex=True).fillna(0).astype(int)
            df.cost = df.cost.replace(
                '', '0', regex=True).fillna(0).astype(float)
            df = df.assign(brand=df.campaign.apply(self.find_brand)) \
                .assign(sub_brand=df.campaign.apply(self.find_sub_brand)) \
                .assign(dsp=self.dsp) \
                .assign(ad_type=df.campaign.apply(self.find_ad_type))
            df.drop_duplicates(inplace=True)

            prob_df = df.groupby(self.dimensions).agg({
                "impressions": "sum",
                "clicks": "sum",
                "cost": "sum",
            }).reset_index()

            if prob_df.shape[0] != df.shape[0]:
                raise Exception("""Problematic rows!
                    The dimensions combinations should be unique
                    It is not a problem of suplicate rows, it is a problem
                    of some combination having different values
                    of impressions, clicks, or cost.""")

            self.dfs[i] = df.drop_duplicates()

        return self

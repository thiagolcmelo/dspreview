# -*- coding: utf-8 -*-
"""
"""

# python standard
import re

# third-party imports
import pandas as pd
from sqlalchemy import and_

# local imports
from utils.bucket_helper import BucketHelper
from utils.sql_helper import SqlHelper
from webapp.app.models import Classification
from webapp.app.queries import GENERATE_REPORT


class Worker(object):
    """
    This class provides capability for downloading .csv files from a bucket
    and save it to the sql database.
    It classifies their information regarded to brand, sub brand, and dsp.
    """

    def __init__(self):
        self.bucket = BucketHelper()
        self.sqlhelper = SqlHelper()
        self.dfs = []
        self.pattern = None
        self.dsp = None
        self.load_classifications()

    def download(self, pattern=None):
        """
        this function checks whether there are files in the GCP bucket
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
        print("Downloading...")
        pattern = pattern or self.pattern
        self.dfs = []
        self.dfs_classified = []
        files = self.bucket.list_files()
        for f in files:
            if re.search(pattern, f['name']):
                self.dfs.append(self.bucket.get_csv_file(f['name']))
        return self

    def upload(self, raw=False):
        """this is a basic upload to the mysql database, since the schema
        is defined in src.utils.sql_helper, the DataFrames are expect to be
        in a very specific format, which must be assured by the parsers
        implementation

        Params
        ------
        raw : boolean
            if True, it will upload only the raw data, without classifications

        Returns
        -------
        The object instace for use in chain calls
        """
        con = self.sqlhelper.get_connection()

        if raw:
            print("Uploading [raw]...")
            dims = self.dimensions_raw
            table_type = 'raw'
            dfs = self.dfs
        else:
            print("Uploading [classified]...")
            dims = self.dimensions
            table_type = 'classified'
            dfs = self.dfs_classified

        table = "{}_{}".format('dsp' if self.dsp else 'dcm', table_type)
        table_temp = "{}_temp".format(table)

        for df in dfs:
            df.to_sql(con=con, name=table_temp,
                      if_exists='replace', index=False)
            update_part = []
            for c in self.metrics:
                update_part.append("{table}.{fld}={temp}.{fld}".format(
                    table=table, temp=table_temp, fld=c))
            update_part.append(
                "{table}.updated_at=CURRENT_TIMESTAMP()".format(table=table))
            update_part = ",".join(update_part)
            all_columns = ",".join(dims+self.metrics)
            connection = con.connect()
            connection.execute("""INSERT INTO {table} ({all_cols})
                                    SELECT {all_cols}
                                    FROM {temp} ON DUPLICATE KEY
                                    UPDATE
                                    {updates}
                                    """.format(table=table, temp=table_temp,
                                               updates=update_part,
                                               all_cols=all_columns))
            connection.execute("DROP TABLE {temp}".format(temp=table_temp))
        return self

    def parse(self):
        raise NotImplementedError("""Implemented by children, for specific
                                  purposes.""")

    def find_by_patter(self, ad_line, classifiers, classifying):
        for c in classifiers:
            composed = ""
            if c.use_campaign_id:
                composed += str(ad_line.get("campaign_id") or 0)
            if c.use_campaign:
                composed += ad_line.get("campaign") or ""
            if c.use_placement_id:
                composed += str(ad_line.get("placement_id") or 0)
            if c.use_placement:
                composed += ad_line.get("placement") or ""
            if re.search(c.pattern, composed, re.IGNORECASE):
                return getattr(c, classifying)
        return "unidentified " + classifying.replace("_", "")

    def find_brand(self, ad_line):
        return self.find_by_patter(ad_line, self.for_brand, "brand")

    def find_sub_brand(self, ad_line):
        return self.find_by_patter(ad_line, self.for_sub_brand, "sub_brand")

    def find_dsp(self, ad_line):
        return self.find_by_patter(ad_line, self.for_dsp, "dsp")

    def classify(self):
        """
        try to classify brand, sub brand, and dsp according to fields values
        it should not be called before parse!

        Returns
        -------
        The object instace for use in chain calls
        """
        print("Applying classification...")
        self.dfs_classified = []
        for i, df in enumerate(self.dfs):
            df = df.assign(brand=df.apply(self.find_brand, axis=1)) \
                .assign(sub_brand=df.apply(self.find_sub_brand, axis=1)) \
                .assign(dsp=df.apply(self.find_dsp, axis=1))

            df = df.groupby(self.dimensions) \
                .agg(self.metrics_agg).reset_index()
            self.dfs_classified.append(df.copy())
        return self

    def load_classifications(self):
        """
        Load the classifications for figuring out the brand, sub brand, and
        dsp according to the information in campaign and placement fields
        """
        with self.sqlhelper.get_context():
            self.for_brand = Classification.query \
                .filter(and_(
                            Classification.brand.isnot(None),
                            Classification.brand != "")).all()
            self.for_sub_brand = Classification.query \
                .filter(and_(
                            Classification.sub_brand.isnot(None),
                            Classification.sub_brand != "")).all()
            self.for_dsp = Classification.query \
                .filter(and_(
                            Classification.dsp.isnot(None),
                            Classification.dsp != "")).all()


class DcmWorker(Worker):
    def __init__(self):
        super(DcmWorker, self).__init__()
        self.pattern = ".*dcm.*"
        self.dimensions_raw = [
            "date",
            "campaign_id",
            "campaign",
            "placement_id",
            "placement"
        ]
        self.dimensions = [
            "date",
            "brand",
            "sub_brand",
            "campaign_id",
            "campaign",
            "placement_id",
            "placement",
            "dsp"
        ]
        self.metrics_agg = {
            "impressions": "sum",
            "clicks": "sum",
            "reach": "sum"  # WARNING! CALCULATED METRIC!!!
        }
        self.metrics = list(self.metrics_agg.keys())

    def parse(self):
        """this function should apply all the proper parsing operations in the
        downloaded dataframes, setting values as dates, integers, or floats

        Returns
        -------
        The object instace for use in chain calls
        """
        print("Parsing...")
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
            df.drop_duplicates(inplace=True)

            prob_df = df.groupby(self.dimensions_raw)\
                .agg(self.metrics_agg).reset_index()

            if prob_df.shape[0] != df.shape[0]:
                raise Exception("""Problematic rows!
                    The dimensions combinations should be unique
                    It is not a problem of suplicate rows, it is a problem
                    of some combination having different values
                    of impressions, clicks, or cost.""")

            self.dfs[i] = df.copy()
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
        self.pattern = ".*%s.*" % dsp
        self.dimensions_raw = [
            "date",
            "campaign",
            "campaign_id",
        ]
        self.dimensions = [
            "date",
            "brand",
            "sub_brand",
            "dsp",
            "campaign",
            "campaign_id"
        ]
        self.metrics_agg = {
            "impressions": "sum",
            "clicks": "sum",
            "cost": "sum"
        }
        self.metrics = list(self.metrics_agg.keys())

    def parse(self):
        """this function should apply all the proper parsing operations in the
        downloaded dataframes, setting values as dates, integers, or floats

        Returns
        -------
        The object instace for use in chain calls
        """
        print("Parsing...")
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
            df.drop_duplicates(inplace=True)

            prob_df = df.groupby(self.dimensions_raw)\
                .agg(self.metrics_agg).reset_index()

            if prob_df.shape[0] != df.shape[0]:
                raise Exception("""Problematic rows!
                    The dimensions combinations should be unique
                    It is not a problem of suplicate rows, it is a problem
                    of some combination having different values
                    of impressions, clicks, or cost.""")

            self.dfs[i] = df.copy()

        return self


def generate_report():
    """ this function generates the full report, joining data from DCM and
    the DSPs. Since the files might have arbitrary dates, the option is to
    generate the whole report table again. It is not that bad though
    """
    sqlhelper = SqlHelper()
    con = sqlhelper.get_connection()
    connection = con.connect()
    connection.execute(GENERATE_REPORT)

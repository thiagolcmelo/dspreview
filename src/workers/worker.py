# -*- coding: utf-8 -*-

# python standard
import re
import logging

# third-party imports
import pandas as pd
from sqlalchemy import and_
import pika

# local imports
from utils.bucket_helper import BucketHelper
from utils.config_helper import ConfigHelper
from utils.sql_helper import get_connection, get_context
from webapp.app.models import Classification
from webapp.app.queries import GENERATE_REPORT

############################################################################
logger = logging.getLogger('dspreview_application')
con = get_connection()
############################################################################


class Manager(object):
    def __enter__(self):
        try:
            config = ConfigHelper()
            credentials = pika.PlainCredentials(config.get_config("MQ_USER"),
                                                config.get_config("MQ_PASS"))
            host = config.get_config("MQ_HOST")
            port = config.get_config("MQ_PORT")
            vhost = config.get_config("MQ_VHOST")
            self.queue = config.get_config("MQ_QUEUE")
            parameters = pika.ConnectionParameters(host=host, port=port,
                                                   virtual_host=vhost,
                                                   credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue, durable=True)
        except Exception as err:
            logger.exception(err)
            raise Exception("""It was not possible to connect to the MQ,
                            please check the connection information""")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def schedule_task(self, task):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue,
                                   body=task,
                                   properties=pika.BasicProperties(
                                           delivery_mode=2,))

    def check_schedule(self):
        self.channel.queue_declare(queue=self.queue, durable=True)

        def callback(ch, method, properties, body):
            body = body.decode("utf-8").lower()
            logger.info(">>> Received {}".format(body))

            if body == "report":
                generate_report
            else:
                workers = []
                if body == 'dcm':
                    logger.info("Triggering DCM worker")
                    workers.append(DcmWorker())
                elif "." in body:
                    _, dsp = body.split(".")
                    logger.info("Triggering DSP worker [{}]".format(dsp))
                    workers.append(DspWorker(dsp))
                else:
                    logger.info("Triggering DSP workers")
                    dsp_opts = BucketHelper.dsp_available()
                    logger.info("Found [{}]".format(", ".join(dsp_opts)))
                    for opt in dsp_opts:
                        logger.info("Creating structure for [{}]".format(opt))
                        workers.append(DspWorker(opt))
                for w in workers:
                    w.extract().transform().load()
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("<<< Success :)")

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(callback, queue=self.queue)
        self.channel.start_consuming()

    def route_task(self):
        pass


class Worker(object):
    """
    This class provides capability for downloading .csv files from a bucket
    and save it to the sql database.
    It classifies their information regarded to brand, sub brand, and dsp.
    """

    def __init__(self):
        self.dfs = []
        self.dfs_classified = []
        self.pattern = None
        self.dsp = None
        self.load_classifications()

    def extract(self, pattern=None):
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
        bucket = BucketHelper()
        pattern = pattern or self.pattern
        self.dfs = []
        files = bucket.list_files()
        for f in files:
            fname = f['name']
            if re.search(pattern, fname):
                logger.info("Extracting file [{}]".format(fname))
                self.dfs.append(bucket.get_csv_file(fname))
        return self

    def transform(self):
        """
        Execute all transformation that apply

        Returns
        -------
        The object instace for use in chain calls
        """
        return self.parse().classify()

    def load(self):
        """
        Save data to database

        Returns
        -------
        The object instace for use in chain calls
        """
        return self.upload(raw=True).upload()

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
        if raw:
            logmsg = "Uploading [{}] [raw]".format(self.dsp or "DCM")
            dims = self.dimensions_raw
            table_type = 'raw'
            dfs = self.dfs
        else:
            logmsg = "Uploading [{}] [classified]".format(self.dsp or "DCM")
            dims = self.dimensions
            table_type = 'classified'
            dfs = self.dfs_classified

        logger.info(logmsg)
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
        logger.info("Applying classification")
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
        with get_context():
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
        logger.info("Parsing DCM file")
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

            if not all(df.campaign.values) or not all(df.placement.values):
                raise Exception("""There are missing values in campaign or
                                placement fields""")

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
        logger.info("Parsing DSP file [{}]".format(self.dsp))
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

            if not all(df.campaign.values):
                raise Exception("There are missing values in campaign field")

            self.dfs[i] = df.copy()

        return self


def generate_report():
    """ this function generates the full report, joining data from DCM and
    the DSPs. Since the files might have arbitrary dates, the option is to
    generate the whole report table again.
    """
    con = get_connection()
    connection = con.connect()
    connection.execute(GENERATE_REPORT)

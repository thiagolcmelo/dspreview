# -*- coding: utf-8 -*-
"""
this module has a friedly interface for dealing with buckets
currently it works only for GCP Storage, but it can be
expanded to work with AWS S3 as well
"""

import json
import os
import tempfile
import datetime

import pandas as pd

import googleapiclient.discovery

class BucketHelper(object):
    """ This is a very simple class for listing, downloading and archiving
    csv files.
    """
    def __init__(self):
        self.bucket = os.environ.get("GCP_BUCKET")
        self.account = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        user_home = os.path.expanduser("~")
        config_file = "{}/.dspreview.json".format(user_home)
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                try:
                    data = json.loads(f.read())
                    self.bucket = self.bucket or data.get("GCP_BUCKET")
                    self.account = self.account or \
                            data.get("GOOGLE_APPLICATION_CREDENTIALS")
                except:
                    raise Exception("Invalid .dspreview.json file.")
                
        if not self.bucket or not self.account:
            raise Exception("""The GCP_BUCKET or GOOGLE_APPLICATION_CREDENTIALS
            are not set and there is not a .dspreview.json file in the user's
            home folder, please provide one of them.""")
        
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.account
        self._service = None
    
    @property
    def service(self):
        """it avoids the creation of multiple service"""
        if not self._service:
            self._service = googleapiclient.discovery.build('storage', 'v1')
        return self._service

    def list_files(self):
        """lists all files inside the project's bucket

        Returns
        -------
        files : array_like
           an array of dictionaies like:
           `[{'name': 'dbm.csv', 'contentType': 'text/csv', 'size': '21645'}]`
        """
        fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
        req = self.service.objects().list(bucket=self.bucket, fields=fields_to_return)
        all_objects = []
        while req:
            resp = req.execute()
            all_objects.extend(resp.get('items', []))
            req = self.service.objects().list_next(req, resp)
        return all_objects

    def get_csv_file(self, filename):
        """downloads a file that exists!

        Params
        ------
        filename : string
            the name of a existing file, it might be found through `list_files`

        Returns
        -------
        Pandas dataframe with the parsed .csv file or None if it does not exist
        """
        req = self.service.objects().get_media(bucket=self.bucket, object=filename)
        with tempfile.TemporaryFile(mode='w+b') as tmpfile:
            downloader = googleapiclient.http.MediaIoBaseDownload(tmpfile, req)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print("Download {}%.".format(int(status.progress() * 100)))
            tmpfile.seek(0)
            return pd.read_csv(tmpfile)
        return None

    def archive_csv_file(self, filename):
        """archive a file that exists!

        Params
        ------
        filename : string
            the name of a existing file, it might be found through `list_files`

        Returns
        -------
        A boolean indicating if the file was archived successfully
        """
        try:
            # it will copy and delete for now
            ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            new_filename = "archive/{0}___{1}".format(filename, ts)
            req = self.service.objects().copy(sourceBucket=self.bucket, \
                sourceObject=filename, destinationBucket=self.bucket, \
                destinationObject=new_filename,body={})
            resp = req.execute()
            if resp.get('name') and resp.get('name') == new_filename:
                self.service.objects().delete(bucket=self.bucket, object=filename).execute()
        except:
            return False
        return True
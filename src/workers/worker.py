# -*- coding: utf-8 -*-
"""
"""

import re

from src.utils.bucket_helper import BucketHelper

class Worker(object):
    def __init__(self):
        self.bucket = BucketHelper()

    def download(self, pattern):
        files = self.bucket.list_files()
        for f in files:
            if re.search(pattern, f['name']):
                self.bucket.get_csv_file(f['name'])
    
    def upload(self):
        pass


class DcmWorker(Worker):
    def __init__(self):
        super(DcmWorker, self).__init__()
    
    def parse(self):
        pass


class DspWorker(Worker):
    def __init__(self):
        super(DspWorker, self).__init__()
    
    def parse(self):
        pass


# -*- coding: utf-8 -*-
"""
Config file manager
"""

# python standard
import os
import json


class ConfigHelper(object):
    """
    Manage the config file
    """
    def __init__(self):
        self.config_info = {}

        # check for the config file even if the env vars are found
        self.user_home = os.path.expanduser("~")
        self.config_file = "{}/.dspreview.json".format(self.user_home)

        # create a config file if it does not exist
        if not os.path.exists(self.config_file):
            with open(self.config_file, 'a') as f:
                f.write("")
        else:
            with open(self.config_file, 'r') as f:
                try:
                    self.config_info = json.loads(f.read())
                except Exception as err:
                    print(str(err))
                    raise Exception("Invalid .dspreview.json file.")

    def get_config(self, name):
        """
        Params
        -----
        """
        return self.config_info.get(name)

    def set_config(self, name, value):
        self.config_info[name] = value
        content = json.dumps(self.config_info)
        with open(self.config_file, 'w') as f:
            f.write(content)

        # reload contents
        self.__init__()

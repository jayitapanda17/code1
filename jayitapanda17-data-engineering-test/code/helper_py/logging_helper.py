#!/usr/bin/python3

import sys, getopt
import logging
import datetime

class TaskLogger:

    logger = logging.getLogger(__name__)

    def __init__(self, application_name = None, process_name = None, inputfilename=None):

        ch = logging.StreamHandler()

        current_datetime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        LogHome="../logs"
        if inputfilename is None:
            inputfilename="{}/{}.{}.{}.log".format(LogHome, application_name, process_name , current_datetime)

        formatter_string='%(asctime)s %(module)s:%(funcName)s: %(levelname)-8s: %(message)s'
        date_format_string='%Y-%m-%d %H:%M'
        formatter = logging.Formatter(formatter_string, datefmt=date_format_string)
        logger = logging.basicConfig(filename=inputfilename, format=formatter_string, datefmt=date_format_string)

        #Setting log level
        self.logger.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        print("Log file Name : " + inputfilename)

    def getLogger(self):
        return self.logger
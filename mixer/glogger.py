"""This module create the .log for the steps normalized."""
import time
import logging

from utilities.logger import Logger
from mixer.settings import DEBUG


global logger
logfile = (
    "_".join([
        "log_mixer",
        time.strftime("%Y%m%d%H%M%S", time.gmtime()),
        ".log"
    ])
)
logger = Logger(logfile, logging.DEBUG, DEBUG)

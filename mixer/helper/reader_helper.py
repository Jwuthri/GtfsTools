"""Here are the db connection."""
import importlib
import logging

from mixer.settings import db_type
from mixer.glogger import logger


class Reader(object):
    """Helper to gen the reader class."""

    def __init__(self, db_name):
        """Constructor."""
        DB = getattr(
            importlib.import_module(
                "utilities.database.{}".format(db_type)
            ), "DB"
        )
        logger.log(logging.INFO, "Initialize DB connection")
        self.db = DB(db_name)
        self.db_name = db_name

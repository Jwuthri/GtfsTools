"""Read the table in the database."""
import logging

from mixer.helper.reader_helper import Reader
from mixer.gtfs.mixer.settings import _TABLE_IN_DATABASE
from mixer.glogger import logger


class ReaderETL(Reader):
    """Read an etl db."""

    def __init__(self, db_name):
        """Constructor."""
        super().__init__(db_name)

    def is_the_db_clean(self):
        """Read if it's the first gtfs."""
        query = """
            SELECT Top 1 Id
            FROM Gtfs
        """
        try:
            self.db.execute_and_fetch_all(query)[0][0]
        except:
            return True
        else:
            return False

    def read_database(self):
        """Read all table in db."""
        dict_df = dict()
        query = """
            SELECT * FROM {}
        """
        for table in _TABLE_IN_DATABASE:
            cquery = query.format(table)
            logger.log(logging.INFO, cquery)
            dict_df[table] = self.db.get_query_frame(cquery)

        return dict_df

"""Persist a GTFS in the db."""
import logging

from mixer.helper.writer_helper import Writer
from utilities.decorator import logged
from mixer.glogger import logger
from mixer.settings import safe_mode


class WriterGTFS(Writer):
    """Persist the df into the table."""

    def __init__(self, db_name):
        """Constructor."""
        super().__init__(db_name)
        self.nb_errors = 0
        self.dict_df = None

    def log_error(self, e, table):
        """Log the exception."""
        self.nb_errors += 1
        logger.log(logging.ERROR, table)
        logger.log(logging.ERROR, e)

    def insert_table(self, table):
        """Insert df."""
        if table == "Shape":
            return self.insert_shapes(table)

        df = self.dict_df[table]
        try:
            self.db.insert_dataframe(df, table)
        except Exception as e:
            self.log_error(e, table)
        else:
            logger.log(logging.INFO, "{} insert".format(table))

    def insert_shapes(self, table):
        """Update a table."""
        df = self.dict_df[table]
        try:
            self.db.insert_geography(df)
        except Exception as e:
            self.log_error(e, table)
        else:
            logger.log(logging.INFO, "{} insert".format(table))

    def update_table(self, table, set_col, where_col):
        """Update a table."""
        df = self.dict_df[table]
        try:
            self.db.update_table(df, table, set_col, where_col)
        except Exception as e:
            self.log_error(e, table)
        else:
            logger.log(logging.INFO, "{} update".format(table))

    def strat_commit(self):
        """Should we commit or raise if error."""
        msg = "There are {} errors".format(self.nb_errors)
        if safe_mode:
            if self.nb_errors == 0:
                self.db.commit()
            else:
                self.db.rollback()
                raise WriterException(msg)
        else:
            self.db.commit()
            logger.log(logging.WARNING, msg)

    @logged(level=logging.INFO, name=logger)
    def insert_gtfs(self, dict_df):
        """Insert all df into the table."""
        self.dict_df = dict_df
        order_insertion = [
            "Gtfs", "Agency", "Calendar", "CalendarDate", "Stop",
            "Route", "Trip", "StopTime", "Shape", "TripToGtfs",
            "StopToGtfs", "RouteToTrip", "RouteToGtfs",
            "TripToShape", "TripToDate", "TransferTimesNDistances"
        ]
        for table in order_insertion:
            self.insert_table(table)

        self.strat_commit()

    @logged(level=logging.INFO, name=logger)
    def up_gtfs(self, dict_df):
        """Update the entities in db."""
        self.dict_df = dict_df
        order_update = ["Stop", "Route"]
        for table in order_update:
            self.update_table(table, ["EndDate"], ["Id"])

        self.strat_commit()

    @logged(level=logging.INFO, name=logger)
    def end_gtfs(self, dict_df):
        """Close the entities in the db."""
        self.dict_df = dict_df
        order_update = ["Stop", "Route"]
        for table in order_update:
            self.update_table(table, ["EndDate"], ["Id"])

        self.strat_commit()


class WriterException(Exception):
    """Class to generate the raise exception."""

    def __init__(self, message):
        """Constructor."""
        self.message = message

    def __str__(self):
        """Print message."""
        return self.message

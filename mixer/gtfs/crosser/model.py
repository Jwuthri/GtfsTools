"""Are GTFS crossing ?"""
import logging
import datetime

from mixer.glogger import logger
from mixer.gtfs.crosser.writer import WriterIntersection
from mixer.gtfs.crosser.settings import dict_nt
from utilities.datetime_time import range_date
from utilities.utils import change_list_type
from utilities.pandas_tools import remove_rows_contains_null


class Model(object):
    """Define and remove shuffle data."""

    def __init__(self, dict_gtfs_in_base, dict_df, db_name):
        """Constructor."""
        self.dict_df = dict_df
        self.db_name = db_name
        self.dict_gtfs_in_base = dict_gtfs_in_base
        self.set_last_date_in_base
        self.set_new_start_date
        self.new_end_date = self.start_date - datetime.timedelta(days=1)
        self.writer = WriterIntersection(db_name)
        self.set_last_gtfs

    @property
    def set_new_start_date(self):
        """Define the new start date ine gtfs."""
        self.start_date = self.dict_df["Gtfs"]["StartDate"].min()

    @property
    def set_last_date_in_base(self):
        """Define last enddate in base."""
        self.end_date = self.dict_gtfs_in_base["Gtfs"]["EndDate"].max().date()

    @property
    def set_last_gtfs(self):
        """Find last gtfs in base."""
        df = self.dict_gtfs_in_base["Gtfs"]
        df = df.sort_values(by="EndDate")
        self.gtfs = df[:1]
        self.gtfs = self.gtfs.copy()
        self.gtfs["EndDate"] = self.new_end_date

    def crossing(self):
        """Data are crossing?"""
        if self.start_date < self.end_date:
            logger.log(logging.WARNING, "The gtfs are crossing.")
            return True
        else:
            return False

    def change_new_end_date(self, df, col):
        """Change the col, to new enddate."""

        def change_date(val):
            """Change date to new max_enddate."""
            val = val.date()
            if val > self.new_end_date:
                return self.new_end_date
            else:
                return None

        return df[col].map(change_date)

    def update_end_date(self, table):
        """Change the end_date to new for StopTime and TripToShape."""
        df = self.dict_gtfs_in_base[table]
        df["EndDate"] = self.change_new_end_date(df, "EndDate")
        df["StartDate"] = self.change_new_end_date(df, "StartDate")
        df = remove_rows_contains_null(df, "EndDate")
        df = remove_rows_contains_null(df, "StartDate")

        return df

    def gtfs_intersection(self):
        """Remove TripToDate during the range date,
        change the Gtfs end_date, and the stop_time and
        trip_to_shape end_date."""
        if self.crossing():
            lst_dates = range_date(self.start_date, self.end_date)
            msg = "There are {} trip_to_date to remove".format(len(lst_dates))
            logger.log(logging.INFO, msg)
            lst_dates = change_list_type(lst_dates, str)
            self.writer.remove_trip_to_date(lst_dates)
            msg = 'New gtfs EndDate is {}'.format(self.new_end_date)
            logger.log(logging.INFO, msg)
            msg = 'Insteed of {}'.format(self.end_date)
            logger.log(logging.INFO, msg)
            self.writer.change_gtfs_end_date(self.gtfs)
            for table in dict_nt.keys():
                df = self.update_end_date(table)
                logger.log(logging.INFO, "Update table {}".format(table))
                self.writer.update_end_date(
                    df, table, dict_nt[table].set, dict_nt[table].where)

            self.writer.db.commit()

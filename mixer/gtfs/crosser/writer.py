"""Remove and update all data cause of intersection."""
import logging

from mixer.helper.writer_helper import Writer
from utilities.decorator import logged
from mixer.glogger import logger


class WriterIntersection(Writer):
    """Remove or update data."""

    def __init__(self, db_name):
        """Constructor."""
        super().__init__(db_name)

    @logged(level=logging.INFO, name=logger)
    def remove_trip_to_date(self, lst_day):
        """Delete all trip_to_date of a period."""
        cquery = """
            DELETE
            FROM TripToDate
            WHERE Date in {}
        """.format(tuple(lst_day))
        self.db.execute(cquery, False)

    def change_gtfs_end_date(self, gtfs):
        """Change Gtfs EndDate."""
        self.db.update_table(gtfs, "Gtfs", ["EndDate"], ["Id"])

    @logged(level=logging.INFO, name=logger)
    def update_end_date(self, df, table, set, where):
        """Change the end & start date with new one."""
        self.db.update_table(df, table, set, where)

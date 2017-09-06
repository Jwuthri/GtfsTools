"""Create a zip file (gtfs)."""
from mixer.gtfs.constructer.model import GtfsConstructer
from mixer.gtfs.constructer.writer import Writer


class Controller(object):
    """For a given GTFS zip return a dict of df."""

    def __init__(self, schedule_type, key_type, db_name, date):
        """Constructor."""
        self.schedule_type = schedule_type
        self.key_type = key_type
        self.db_name = db_name
        self.date = date

    def main(self):
        """Gen the df normalized."""
        model = GtfsConstructer(
            self.schedule_type, self.key_type, self.db_name, self.date)
        dict_df = model.build()
        writer = Writer(dict_df, self.date, self.db_name, self.schedule_type)

        return writer.gtfs_writer()

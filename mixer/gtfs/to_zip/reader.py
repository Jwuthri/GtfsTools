"""Read a df from a dict of df."""
from mixer.gtfs.to_zip.settings import dict_subset


class Reader(object):

    def __init__(self, dict_df):
        self.dict_df = dict_df
        self.key = dict_df.keys()

    def read(self, table):
        as_gtfs = dict_subset[table].as_gtfs
        as_db = dict_subset[table].as_db

        if as_gtfs in self.key:
            return self.dict_df[as_gtfs]
        elif as_db in self.key:
            return self.dict_df[as_db]
        else:
            raise TableMiss("We don't have this file")


class TableMiss(Exception):
    """Class to generate the raise exception."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

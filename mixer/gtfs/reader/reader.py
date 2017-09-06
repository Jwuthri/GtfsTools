"""Transform a file into df."""
import pandas as pd


class Reader(object):
    """Read a gtfs file."""

    def __init__(self, gtfs, dict_files):
        """Constructor."""
        self.gtfs = gtfs
        self.dict_files = dict_files

    def read_file(self, table):
        """Read a file in a zip dir.

        :params dict_files: the type of the columns
        :params table: the file to read
        """
        dt = self.dict_files[table].as_type

        return pd.read_csv(
            self.gtfs.open(table), encoding='utf-8-sig', dtype=dt
        )

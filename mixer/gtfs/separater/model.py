"""Find the difference between 2 gtfs."""
import datetime

import mixer.gtfs.separater.settings as SETTINGS
from utilities.pandas_tools import compare_data_frame


class Model(object):
    """Find difference entities end and new."""

    def __init__(self, dict_new_gtfs, dict_gtfs_in_base):
        """Constructor."""
        self.dict_new_gtfs = dict_new_gtfs
        self.dict_gtfs_in_base = dict_gtfs_in_base
        self.start_date = self.dict_new_gtfs["Gtfs"]["StartDate"].min()
        self.end_date = self.start_date - datetime.timedelta(days=1)

    def compare_data(self, table):
        """Determine if we have new data."""
        return compare_data_frame(
            self.dict_new_gtfs[table],
            self.dict_gtfs_in_base[table],
            SETTINGS.dict_row[table].compare_id
        )

    def compare_reup_data(self, table, new_data):
        """Determine if we have data reused after been closed."""
        df = compare_data_frame(
            self.dict_new_gtfs[table],
            new_data,
            SETTINGS.dict_row[table].compare_id
        )

        return df

    def compare_close_data(self, table):
        """Determine if we have data reused after been closed."""
        df = compare_data_frame(
            self.dict_gtfs_in_base[table],
            self.dict_new_gtfs[table],
            SETTINGS.dict_row[table].compare_id
        )

        return df

    def whats_new(self):
        """Define the new entities in this new gtfs."""
        new_df = dict()
        for table in SETTINGS.dict_row.keys():
            new_df[table] = self.compare_data(table)
        for table in SETTINGS._UNCHANGED_TABLE:
            new_df[table] = self.dict_new_gtfs[table]

        return new_df

    def whats_up(self, new_df):
        """Define the entities reused."""
        up_df = dict()
        for table in ["Stop", "Route"]:
            df = self.compare_reup_data(table, new_df[table])
            df = df.copy()
            df["EndDate"] = None
            up_df[table] = df

        return up_df

    def whats_end(self):
        """Define the entities no more used."""
        end_df = dict()
        for table in ["Stop", "Route"]:
            df = self.compare_close_data(table)
            df = df.copy()
            df["EndDate"] = self.close_entities(df["EndDate"])
            end_df[table] = df

        return end_df

    def close_entities(self, serie):
        """Add an EndDate to closed entities."""
        return serie.map(self.add_end_date)

    def add_end_date(self, row):
        """Add EndDate if not."""
        if row is None:
            return self.end_date
        else:
            return row

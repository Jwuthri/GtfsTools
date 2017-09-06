"""Select of subset of the GTFS files to our need."""
import logging

from mixer.gtfs.subseter.settings import dict_subset
from mixer.glogger import logger
from utilities.decorator import unique
from utilities.decorator import logged


class Model(object):
    """Create the sub df thx to the settings.py."""

    def __init__(self, dict_df_versionned):
        """Constructor."""
        self.dict_df_versionned = dict_df_versionned
        self.dict_subset = dict_subset
        self.set_subset_df

    @property
    def set_subset_df(self):
        """List of all df which will be create."""
        self.lst_df = list(self.dict_subset.keys())

    @unique
    def select_sub_cols(self, table):
        """Select a subset of the initial df."""
        cols = self.dict_subset[table].sub_cols
        df_name = self.dict_subset[table].initial
        df = self.dict_df_versionned[df_name]
        new_df = df[cols]
        rename = self.dict_subset[table].rename
        if rename:
            new_df = new_df.rename(columns={"Id": rename})

        return new_df

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Generated all the table needed for the database."""
        dict_subset = dict()
        for df in self.lst_df:
            msg = "Subset the df {}".format(df)
            logger.log(logging.INFO, msg)
            dict_subset[df] = self.select_sub_cols(df)

        return dict_subset

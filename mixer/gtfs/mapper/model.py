"""Map the columns btw gtfs and data base."""
import logging

from mixer.glogger import logger
from utilities.decorator import logged
import mixer.gtfs.mapper.settings as SETTINGS


class Model(object):
    """Change the name of the dataframe columns."""

    def __init__(self, dict_df_normalized):
        """Constructor."""
        self.dict_mapper = SETTINGS.dict_mapper
        self.dict_df_normalized = dict_df_normalized

    def remap_df(self, table):
        """Remap the df columns to db columns."""
        msg = "Remap df {}".format(table)
        logger.log(logging.INFO, msg)
        df = self.dict_df_normalized[table]
        nt = self.dict_mapper[table]
        df = df.rename(columns=nt.mapping)

        return df[nt.sub_cols]

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Map each df columns."""
        dict_df_mapped = dict()
        dict_df_mapped["Gtfs"] = self.remap_df("Gtfs")
        dict_df_mapped["Agency"] = self.remap_df("Agency")
        dict_df_mapped["Calendar"] = self.remap_df("Calendar")
        dict_df_mapped["CalendarDate"] = self.remap_df("CalendarDate")
        dict_df_mapped["Route"] = self.remap_df("Route")
        dict_df_mapped["Stop"] = self.remap_df("Stop")
        dict_df_mapped["Trip"] = self.remap_df("Trip")
        dict_df_mapped["StopTime"] = self.remap_df("StopTime")
        dict_df_mapped["Shape"] = self.remap_df("Shape")
        dict_df_mapped["TripToDate"] = self.remap_df("TripToDate")

        return dict_df_mapped

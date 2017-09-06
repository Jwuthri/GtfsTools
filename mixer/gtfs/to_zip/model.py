"""Transform a dict of df to a zipfile."""
import logging
import datetime

import mixer.gtfs.constructer.settings as SETTINGS
from mixer.gtfs.reader.controller import Controller as CR
from mixer.gtfs.normalizer.controller import Controller as CN
from mixer.gtfs.mapper.controller import Controller as CM
from mixer.gtfs.versioner.controller import Controller as CV
from mixer.gtfs.to_zip.reader import Reader
from mixer.gtfs.mixer.controller import EventLog
from mixer.glogger import logger


class Model(object):

    def __init__(self, gtfs_path):
        self.gtfs_path = gtfs_path

    def normalize_gtfs(self):
        logger.log(logging.INFO, EventLog.log_read_zip)
        dict_reader = CR(self.gtfs_path).main()
        logger.log(logging.INFO, EventLog.log_normalize_gtfs)
        dict_norm = CN(self.gtfs_path, dict_reader).main()
        logger.log(logging.INFO, EventLog.log_mapping_gtfs)
        dict_map = CM(dict_norm).main()
        logger.log(logging.INFO, EventLog.log_versioning_gtfs)
        dict_vers = CV(dict_map).main()
        logger.log(logging.INFO, EventLog.log_subset_gtfs)
        self.file_name = dict_vers["Gtfs"]["Id"].iloc[0]

        return dict_vers

    def remap_df(self, table, df):
        msg = "Remap df {}".format(table)
        logger.log(logging.INFO, msg)
        nt = SETTINGS.dict_mapper[table]
        df = df.rename(columns=nt.mapping)

        return df[nt.sub_cols]

    def remove_date_sep(self, row):
        return row.replace("-", "")

    def format_date(self, row):
        return row.date().strftime("%Y%m%d")

    def format_time(self, sec):
        if sec >= 86400:
            sec -= 86400

        return str(datetime.timedelta(seconds=int(sec)))

    def format_bool(self, df, cols):
        for col in cols:
            df[col] = df[col].apply(lambda bb: bb * 1)

        return df

    def main(self):
        dict_vers = self.normalize_gtfs()
        reader = Reader(dict_vers)
        agency = reader.read("agency.txt")
        calendar = reader.read("calendar.txt")
        calendar_dates = reader.read("calendar_dates.txt")
        routes = reader.read("routes.txt")
        stops = reader.read("stops.txt")
        stoptimes = reader.read("stop_times.txt")
        shapes = reader.read("shapes.txt")
        trips = reader.read("trips.txt")

        agency["Timezone"] = agency["Timezone"].str.title()
        calendar = self.format_bool(calendar, [
            "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday", "Sunday"])
        stoptimes["ArrivalTimeSeconds"] = stoptimes["ArrivalTimeSeconds"].map(
            self.format_time)
        stoptimes["DepartureTimeSeconds"] = stoptimes["DepartureTimeSeconds"].map(
            self.format_time)

        dict_df_mapped = dict()
        dict_df_mapped["agency"] = self.remap_df("Agency", agency)
        dict_df_mapped["calendar"] = self.remap_df("Calendar", calendar)
        dict_df_mapped["calendar_dates"] = self.remap_df(
            "CalendarDate", calendar_dates)
        dict_df_mapped["routes"] = self.remap_df("Route", routes)
        dict_df_mapped["stops"] = self.remap_df("Stop", stops)
        dict_df_mapped["trips"] = self.remap_df("Trip", trips)
        dict_df_mapped["stoptimes"] = self.remap_df("StopTime", stoptimes)
        dict_df_mapped["shapes"] = self.remap_df("Shape", shapes)

        return dict_df_mapped, self.file_name

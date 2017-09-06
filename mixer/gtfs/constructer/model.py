"""Generate the Gtfs from ETL Database."""
import datetime
import logging
import time

import pandas as pd

from mixer.glogger import logger
from mixer.gtfs.constructer.reader import ReaderFactory
import mixer.gtfs.constructer.settings as SETTINGS


class GtfsConstructer(object):
    """Create the archive zip, with all requiered files."""

    def __init__(self, schedule_type, key_type, db_name, date):
        """Constructor."""
        self.db_name = db_name
        self.date = date
        self.time = time.mktime(date.timetuple())
        cls = ReaderFactory(db_name, date)
        self.reader = cls.reader(schedule_type, key_type)
        self.dict_mapper = SETTINGS.dict_mapper

    def get_calendar_dates(self, trips):
        df = trips[["ServiceId", "TripDate"]].copy()
        df = df.drop_duplicates()
        df = df.rename(columns={"TripDate": "Date"})
        df["Exception"] = 1

        return df

    def get_shapes(self, lst_shapes):
        shapes = self.reader.shapes(lst_shapes)
        shapes_df = pd.DataFrame()
        lat = list()
        lon = list()
        shape_id = list()
        for idx, row in shapes.iterrows():
            lst_points = row["Shape"].split('(')[1]
            lst_points = lst_points.replace(")", "")
            lst_tuple_points = lst_points.split(", ")
            del lat[:]
            del lon[:]
            del shape_id[:]
            for pt in lst_tuple_points:
                LAT, LON = pt.split(" ")
                lat.append(LAT)
                lon.append(LON)
                shape_id.append(row["Id"])

            df = pd.DataFrame([lon, lat, shape_id]).transpose()
            df.columns = ["Longitude", "Latitude", "Id"]
            df["Sequence"] = df.index

            shapes_df = pd.concat([df, shapes_df])
        shapes_df.reset_index(drop=True, inplace=True)

        return shapes_df

    def get_df(self):
        dict_df = dict()
        tables = [
            "agency", "calendar", "routes",
            "stops", "stoptimes", "trips"
        ]
        for table in tables:
            dict_df[table] = getattr(self.reader, table)()

        lst_shapes = dict_df["trips"]["ShapeId"].unique()
        dict_df["shapes"] = self.get_shapes(lst_shapes)
        dict_df["calendar_dates"] = self.get_calendar_dates(dict_df["trips"])

        return dict_df

    def format_date(self, row):
        try:
            return row.strftime("%Y%m%d")
        except:
            return row.replace("-", "")

    def format_time(self, sec):
        mins, secs = divmod(sec, 60)
        hours, mins = divmod(mins, 60)

        return '%02d:%02d:%02d' % (hours, mins, secs)

    def format_bool(self, df, cols):
        for col in cols:
            df[col] = df[col].apply(lambda bb: bb * 1)

        return df

    def remap_df(self, table, df):
        """Remap the df columns to db columns."""
        msg = "Remap df {}".format(table)
        logger.log(logging.INFO, msg)
        nt = self.dict_mapper[table]
        df = df.rename(columns=nt.mapping)

        return df[nt.sub_cols]

    def build(self):
        dict_df = self.get_df()
        agency = dict_df["agency"]
        calendar = dict_df["calendar"]
        calendar_dates = dict_df["calendar_dates"]
        routes = dict_df["routes"]
        stops = dict_df["stops"]
        stoptimes = dict_df["stoptimes"]
        shapes = dict_df["shapes"]
        trips = dict_df["trips"]

        calendar["StartDate"] = calendar["StartDate"].map(self.format_date)
        calendar["EndDate"] = calendar["EndDate"].map(self.format_date)
        calendar = self.format_bool(calendar, [
            "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday", "Sunday"])
        calendar_dates["Date"] = calendar_dates[
            "Date"].map(self.format_date)
        stoptimes["ArrivalTimeSeconds"] = stoptimes["ArrivalTimeSeconds"] - self.time
        stoptimes["DepartureTimeSeconds"] = stoptimes["DepartureTimeSeconds"] - self.time
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

        return dict_df_mapped

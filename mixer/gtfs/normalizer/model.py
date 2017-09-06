"""Normalize a gtfs to agree with our needed."""
import logging

import pandas as pd

import utilities.utils as tools
import utilities.datetime_time as dt
from utilities.decorator import logged
from utilities.file_dir import hashfile
from utilities.pandas_tools import change_nan_value
from mixer.glogger import logger
from mixer.gtfs.generater.stimes import EstimateStopTimes, EstimateShapeDistTrav
from mixer.gtfs.generater.shapes import EstimateShapes
from mixer.gtfs.generater.date_trips import DateTrips
from mixer.gtfs.normalizer.settings import dict_normalize


class Model(object):
    """Cast and format columns to our need."""

    def __init__(self, gtfs_path, dict_gtfs):
        """Constructor."""
        self.gtfs_path = gtfs_path
        self.dict_gtfs = dict_gtfs
        start_date = min(self.dict_gtfs["calendar.txt"]["start_date"])
        self.start_date = dt.value2date(start_date)
        end_date = max(self.dict_gtfs["calendar.txt"]["end_date"])
        self.end_date = dt.value2date(end_date)
        self.gtfs_id = hashfile(self.gtfs_path)
        self.dict_normalize = dict_normalize

    def normalize_a_column(self, serie):
        """Clear the column values."""
        return serie.map(tools.fmt_str)

    def format_df(self, df, requiered):
        """Gen miss columns, and normalize columns values.

        :params requiered: list of columns
        :params df: the dataframe to normalize
        :return: dataframe normalized
        """
        for col in requiered:
            if col not in df.columns:
                df[col] = None
            else:
                df[col] = self.normalize_a_column(df[col])

        return df

    @logged(level=logging.INFO, name=logger)
    def set_gtfs(self):
        """Gen the table GTFS."""
        data = {
            "gtfs_id": self.gtfs_id,
            "start_date": self.start_date,
            "end_date": self.end_date
        }
        msg = "The Id of this gtfs zip is {}"
        logger.log(logging.INFO, msg.format(self.gtfs_id))

        return pd.DataFrame([data])

    @logged(level=logging.INFO, name=logger)
    def set_agency(self):
        """Gen the table AGENCY."""
        df = self.dict_gtfs["agency.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, dict_normalize["Agency"].requiered)

    @logged(level=logging.INFO, name=logger)
    def set_calendar(self):
        """Gen the table CALENDAR."""
        df = self.dict_gtfs["calendar.txt"]
        df["start_date"] = df["start_date"].map(dt.value2date)
        df["end_date"] = df["end_date"].map(dt.value2date)
        df["gtfs_id"] = self.gtfs_id
        df = change_nan_value(df, None)

        return self.format_df(df, dict_normalize["Calendar"].requiered)

    @logged(level=logging.INFO, name=logger)
    def gen_calendar_dates(self):
        """Gen or normalize calendar_dates.txt."""

        def remove_weird_service_id(calendar_dates, calendar):
            serv_cal_dates = set(calendar_dates["service_id"].unique())
            serv_cal = set(calendar["service_id"].unique())
            weird_services = list(serv_cal_dates - serv_cal)

            return calendar_dates[~calendar_dates["service_id"].isin(weird_services)]

        if 'calendar_dates.txt' in tools.list_zip_files(self.gtfs_path):
            calendar = self.dict_gtfs["calendar.txt"]
            calendar_dates = self.set_calendar_dates()
            calendar_dates = remove_weird_service_id(calendar_dates, calendar)
        else:
            msg = "We don't have calendar_dates.txt in GTFS zip."""
            logger.log(logging.WARNING, msg)
            calendar_dates = pd.DataFrame()
            calendar_dates.columns = ["date", "service_id", "exception_type"]

        return calendar_dates

    def set_calendar_dates(self):
        """Gen the table CALENDAR_DATES."""
        df = self.dict_gtfs["calendar_dates.txt"]
        df["date"] = df["date"].map(dt.value2date)
        df["gtfs_id"] = self.gtfs_id
        df = change_nan_value(df, None)

        return self.format_df(df, dict_normalize["CalendarDate"].requiered)

    @logged(level=logging.INFO, name=logger)
    def set_routes(self):
        """Gen the table ROUTE."""
        df = self.dict_gtfs["routes.txt"]
        df = change_nan_value(df, None)

        df["sha"] = (
            df["route_long_name"].map(str) +
            df["route_short_name"].map(str)
        )
        df = df.sort_values(by="sha")
        df["start_date"] = self.start_date
        df["end_date"] = None
        df["gtfs_id"] = self.gtfs_id

        def route_direction(serie):
            """Change the name of 2 routes with the same name,
            in opposite direction.
            """
            lst_index = list(serie.loc[serie.shift(-1) == serie].index)
            msg = "We have {} routes with the same name for 2 directions"
            logger.log(logging.WARNING, msg.format(len(lst_index)))
            for idx in lst_index:
                serie.loc[idx] += "_other_dire"

        # route_direction(df["sha"])

        return self.format_df(df, dict_normalize["Route"].requiered)

    @logged(level=logging.INFO, name=logger)
    def set_stops(self):
        """Gen the table STOPS."""
        df = self.dict_gtfs["stops.txt"]
        df = change_nan_value(df, None)
        df["start_date"] = self.start_date
        df["end_date"] = None
        df["gtfs_id"] = self.gtfs_id

        def gen_unallow_null(series):
            """Gen 1 if no type in the stop."""
            if series is None:
                return 0
            else:
                return series

        df = self.format_df(df, dict_normalize["Stop"].requiered)
        df["location_type"] = df["location_type"].map(gen_unallow_null)

        return df

    def format_date(self, row):
        try:
            return dt.time2seconds(row)
        except:
            return row

    def t2s(self, t):
        """Transform time in seconds."""
        hour, minute, second = map(int, str(t).split(":"))
        sod = hour * 3600 + minute * 60 + second
        return sod

    @logged(level=logging.INFO, name=logger)
    def set_stoptimes(self, trips):
        """Gen the table STOPTIMES."""
        df = self.dict_gtfs["stop_times.txt"]

        def prepare_stop_times(df):
            """Gen the arrival and departure time."""
            try:
                df["arrival_time"] = df["arrival_time"].map(self.t2s)
                df["departure_time"] = df["departure_time"].map(self.t2s)
                msg = "We got all the stoptimes"
                logger.log(logging.INFO, msg)
            except:
                msg = "Some stoptimes are generating..."
                logger.log(logging.WARNING, msg)
                df = EstimateStopTimes.main(df)
            return df

        def fill_shape_dist_trav(df, stops):
            """Gen shape_dist_traveled if doesn't exist."""
            if df["shape_dist_traveled"].isnull().values.any():
                return EstimateShapeDistTrav(df, stops).main()
            else:
                return df

        def add_service_id(df, trips):
            return pd.merge(df, trips[["trip_id", "service_id"]], on="trip_id")

        stops = self.dict_gtfs["stops.txt"]
        df = self.format_df(df, dict_normalize["StopTime"].requiered)
        df = fill_shape_dist_trav(df, stops)
        df = prepare_stop_times(df)
        df = add_service_id(df, trips)
        df["gtfs_id"] = self.gtfs_id
        df = change_nan_value(df, None)

        def gen_unallow_null(series):
            """."""
            if series is None:
                return 1
            else:
                return series

        df["pickup_type"] = df["pickup_type"].map(gen_unallow_null)
        df["drop_off_type"] = df["drop_off_type"].map(gen_unallow_null)

        return df

    @logged(level=logging.INFO, name=logger)
    def set_trips(self):
        """Gen the table TRIPS."""
        df = self.dict_gtfs["trips.txt"]
        df_calendar = self.dict_gtfs["calendar.txt"]
        df = change_nan_value(df, None)
        df = self.format_df(df, dict_normalize["Trip"].requiered)
        sub_cols = ["service_id", "start_date", "end_date"]
        df = pd.merge(df, df_calendar[sub_cols], on="service_id")
        df["gtfs_id"] = self.gtfs_id

        return df

    def fill_shapes(self, trips, graph):
        """Fill the missed shapes."""
        if len(trips[trips["shape_id"].isnull()]) > 0:
            shapes = self.dict_gtfs["shapes.txt"]
            true_shapes_trips = trips[~trips["shape_id"].isnull()]
            false_shapes_trips = trips[trips["shape_id"].isnull()]
            msg = "{} shapes are generating...".format(len(false_shapes_trips))
            logger.log(logging.WARNING, msg)
            gen_shapes, gen_shape_trips = graph.main(false_shapes_trips)
            logger.log(logging.INFO, "They have been generated")
            shapes = pd.concat([shapes, gen_shapes])
            shapes["gtfs_id"] = self.gtfs_id
            trips = pd.concat([true_shapes_trips, gen_shape_trips])
        else:
            msg = "We got all shapes"
            logger.log(logging.INFO, msg)
            shapes = self.set_shapes()

        shapes = self.format_df(shapes, dict_normalize["Shape"].requiered)

        return shapes, trips

    @logged(level=logging.INFO, name=logger)
    def gen_shapes(self, trips):
        """Gen or normalize the shapes.txt."""
        graph = EstimateShapes(self.gtfs_path)
        if 'shapes.txt' in tools.list_zip_files(self.gtfs_path):
            shapes, trips = self.fill_shapes(trips, graph)
        else:
            msg = "We don't have shapes.txt in GTFS zip, we are generating..."
            logger.log(logging.WARNING, msg)
            shapes, trips = graph.main(trips)
            shapes["gtfs_id"] = self.gtfs_id

        return shapes, trips

    def set_shapes(self):
        """Gen the table SHAPES."""
        df = self.dict_gtfs["shapes.txt"]
        df = change_nan_value(df, None)
        df["gtfs_id"] = self.gtfs_id

        return self.format_df(df, dict_normalize["Shape"].requiered)

    def gen_date_trips(self, trips, cal, cal_dates, gtfs):
        """Gen the table TRIPTODATE."""
        logger.log(logging.INFO, "Generating the TripToDate...")
        dt = DateTrips(trips, cal, cal_dates, gtfs)

        return dt.main()

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Gen all df normalized."""
        dict_df_normalized = dict()
        gtfs = self.set_gtfs()
        dict_df_normalized["Gtfs"] = gtfs
        dict_df_normalized["Agency"] = self.set_agency()
        calendar = self.set_calendar()
        dict_df_normalized["Calendar"] = calendar
        cal_dates = self.gen_calendar_dates()
        dict_df_normalized["CalendarDate"] = cal_dates
        dict_df_normalized["Route"] = self.set_routes()
        dict_df_normalized["Stop"] = self.set_stops()
        trips = self.set_trips()
        shapes, trips = self.gen_shapes(trips)
        dict_df_normalized["Trip"] = trips
        dict_df_normalized["StopTime"] = self.set_stoptimes(trips)
        dict_df_normalized["Shape"] = shapes
        dict_df_normalized["TripToDate"] = self.gen_date_trips(
            trips, calendar, cal_dates, gtfs)

        return dict_df_normalized

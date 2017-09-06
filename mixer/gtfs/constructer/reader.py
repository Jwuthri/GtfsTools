"""Can create a Gtfs from ETL database."""
import logging
import datetime
import time

import pandas as pd

from utilities.datetime_time import time2seconds
from mixer.helper.reader_helper import Reader
from mixer.glogger import logger


class ReaderETL(Reader):
    """Read an Etl Database."""

    def __init__(self, db_name, date):
        """Constructor."""
        super().__init__(db_name)
        self.date = date
        self.gtfs_id = self.get_gtfs_id()

    def agency(self):
        query = """
            SELECT Id, Name, Timezone, Url, Language, Phone
            FROM Agency
        """
        agency = self.db.get_query_frame(query)
        agency["Timezone"] = agency["Timezone"].str.title()

        return agency

    def get_gtfs_id(self):
        query = """
            SELECT Id
            FROM Gtfs
            WHERE StartDate <= '{}'
                AND EndDate >= '{}'
        """
        cquery = query.format(self.date, self.date)

        return self.db.get_query_frame(cquery).iloc[0]["Id"]

    def calendar(self):
        query = """
            SELECT ServiceId, StartDate, EndDate, Monday,
            Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday
            FROM Calendar
            WHERE GtfsId = '{}'
        """
        cquery = query.format(self.gtfs_id)
        df = self.db.get_query_frame(cquery)
        df["StartDate"] = self.date
        df["EndDate"] = self.date + datetime.timedelta(days=1)

        return df

    def routes(self):
        raise NotImplementedError

    def stops(self):
        raise NotImplementedError

    def shapes(self, lst_shapes):
        query = """
            SELECT Id, shape.STAsText() as Shape
            FROM Shape
            WHERE Id in {}
        """
        cquery = query.format(tuple(lst_shapes))

        return self.db.get_query_frame(cquery)

    def stoptimes(self):
        raise NotImplementedError

    def trips(self):
        raise NotImplementedError


class GtfsTheoHashKey(ReaderETL):
    """Read the theoritical gtfs in the database, and use sha1 id."""

    def __init__(self, db_name, date):
        """Constructor."""
        super().__init__(db_name, date)

    def routes(self):
        query = """
            SELECT Id, Type, Route.ShortName,
                Route.LongName, Description,
                Url, Color, TextColor, AgencyId
            FROM Route
        """
        cquery = query.format(self.date, self.date)

        return self.db.get_query_frame(cquery)

    def stops(self):
        query = """
            SELECT Id, Latitude, Longitude, Description, Url,
                Stop.Name, Type, ParentStopId, Code, ZoneId
            FROM Stop
        """
        cquery = query.format(self.date, self.date)

        return self.db.get_query_frame(cquery)

    def stoptimes(self):
        query = """
            SELECT ST.*
            FROM StopTime as ST
                JOIN TripToDate as TDD on TDD.TripId = ST.TripId
            WHERE Date = '{}'
            ORDER BY ST.TripId, StopSequence
        """
        cquery = query.format(self.date)

        return self.db.get_query_frame(cquery)

    def trips(self):
        query = """
            SELECT TDD.Date as TripDate, T.*, TDD.ServiceId, RTT.RouteId, TTS.ShapeId
            FROM TripToDate as TDD
                JOIN RouteToTrip as RTT on RTT.TripId = TDD.TripId
                JOIN Trip as T on T.Id = TDD.TripId
                JOIN TripToShape as TTS on TTS.TripId = TDD.TripId
            WHERE Date = '{}'
        """
        cquery = query.format(self.date)

        return self.db.get_query_frame(cquery)


class GtfsTheoRealKey(ReaderETL):
    """Read the theoritical gtfs in the database, and use real id."""

    def __init__(self, db_name, date):
        """Constructor."""
        super().__init__(db_name, date)

    def routes(self):
        raise NotImplementedError

    def stops(self):
        raise NotImplementedError

    def stoptimes(self):
        raise NotImplementedError

    def trips(self):
        raise NotImplementedError


class GtfsObsHashKey(ReaderETL):
    """Read the observed gtfs in the database, and use sha1 id."""

    def __init__(self, db_name, date):
        """Constructor."""
        super().__init__(db_name, date)

    def routes(self):
        query = """
            SELECT Id, Type, Route.ShortName,
                Route.LongName, Description,
                Url, Color, TextColor, AgencyId
            FROM Route
        """
        cquery = query.format(self.date, self.date)

        return self.db.get_query_frame(cquery)

    def stops(self):
        query = """
            SELECT Id, Latitude, Longitude, ZoneId, ParentStopId,
                Stop.Name, Type, Code, Description, Url
            FROM Stop
        """

        cquery = query.format(self.date, self.date)

        return self.db.get_query_frame(cquery)

    def stoptimes(self):
        query = """
            SELECT AST.StopId, DST.TripId,
                AST.ArrivalDatetime, AST.DepartureDatetime
            FROM AlignedStopTime as AST
                JOIN DateStampedTrip as DST on DST.Id = AST.DateStampedTripId
            WHERE DST.TripDate = '{}'
        """
        cquery = query.format(self.date)
        df = self.db.get_query_frame(cquery)

        def fill_columns(df):
            df["StopHeadsign"] = None
            df["DropOffType"] = 1
            df["PickupType"] = 1
            df["DistanceTraveled"] = None
            group = df.groupby("TripId")
            final_df = pd.DataFrame()
            for trip, df in group:
                df = df.copy()
                df.sort_values(by="ArrivalDatetime", inplace=True)
                df.reset_index(drop=True, inplace=True)
                df["StopSequence"] = df.index
                final_df = pd.concat([final_df, df])
            final_df["ArrivalTimeSeconds"] = final_df["ArrivalDatetime"].apply(lambda t: time.mktime(t.timetuple()))
            final_df["DepartureTimeSeconds"] = final_df["ArrivalDatetime"].apply(lambda t: time.mktime(t.timetuple()))

            return final_df

        df = fill_columns(df)

        return df

    def trips(self):
        query = """
            SELECT Distinct DT.TripId, DT.TripDate, T.*,
                RTT.RouteId, TTS.ShapeId, TDD.ServiceId
            FROM DateStampedTrip as DT
                JOIN RouteToTrip as RTT on RTT.TripId = DT.TripId
                JOIN Trip as T on T.Id = DT.TripId
                JOIN TripToShape as TTS on TTS.TripId = DT.TripId
                JOIN TripToDate as TDD on TDD.TripId = DT.TripId
            WHERE TripDate = '{}'
        """
        cquery = query.format(self.date)

        return self.db.get_query_frame(cquery)


class GtfsObsRealKey(ReaderETL):
    """Read the observed gtfs in the database, and use real id."""

    def __init__(self, db_name, date):
        """Constructor."""
        super().__init__(db_name, date)

    def routes(self):
        raise NotImplementedError

    def stops(self):
        raise NotImplementedError

    def stoptimes(self):
        raise NotImplementedError

    def trips(self):
        raise NotImplementedError


class ReaderFactory(object):
    """Reader Factory."""

    def __init__(self, db_name, date):
        """Constructor."""
        self.db_name = db_name
        self.date = date

    def reader(self, schedule_type, key_type):
        """Create an instance of the right reader class,
        depending on the schedule_type & key_type."""
        msg = "Instance of the class {}"
        if schedule_type == "Theo":
            if key_type == "Real":
                cls = GtfsTheoRealKey(self.db_name, self.date)
                logger.log(logging.INFO, msg.format(cls.__class__.__name__))
                return cls
            else:
                cls = GtfsTheoHashKey(self.db_name, self.date)
                logger.log(logging.INFO, msg.format(cls.__class__.__name__))
                return cls
        elif key_type == "Real":
            cls = GtfsObsRealKey(self.db_name, self.date)
            logger.log(logging.INFO, msg.format(cls.__class__.__name__))
            return cls
        else:
            cls = GtfsObsHashKey(self.db_name, self.date)
            logger.log(logging.INFO, msg.format(cls.__class__.__name__))
            return cls

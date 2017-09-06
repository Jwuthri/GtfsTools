"""Version the GTFS by changing all the Id of each table."""
import logging

import pandas as pd

import utilities.pandas_tools as pt
from utilities.decorator import logged
from mixer.glogger import logger


class Model(object):
    """Transform Id of each entitie into sha1."""

    def __init__(self, dict_df_mapped):
        """Constructor."""
        self.dict_df_mapped = dict_df_mapped
        self.set_agency_id
        self.set_gtfs_id

    @property
    def set_agency_id(self):
        """Extract the id agency."""
        self.agency_id = self.dict_df_mapped["Agency"]["Id"].iloc[0]

    @property
    def set_gtfs_id(self):
        """Extract gtfs id."""
        self.gtfs_id = self.dict_df_mapped["Gtfs"]["Id"].iloc[0]

    def gen_args(self, _id, seq, cols):
        """Gen args for mp."""
        return [
            (
                uid,
                _id,
                seq,
                cols
            )
            for uid in self.df[_id].unique()
        ]

    def gen_sha1(self, uid, _id, seq, cols):
        """Gen sha1 based on a sequence of ."""
        data = self.df[self.df[_id] == uid]
        df_uid = pt.hash_frame(data.sort_values(by=seq)[cols])

        return [uid, df_uid]

    def mp_sha(self, args):
        """Mp for gen sha1."""
        return self.gen_sha1(*args)

    @logged(level=logging.INFO, name=logger)
    def gen_new_id_on_seq(self, table, cols, seq, _id):
        """Generate a sha1 for a sequence of stops."""
        logger.log(logging.INFO, "generate sha1 for {}".format(table))
        df = self.dict_df_mapped[table].copy()
        self.df = df
        lst = []

        # If we want to multiprocess the generation of the sha1 use next one,
        # else still use the code row 72 => 75
        # import multiprocessing as mp
        # from tqdm import tqdm
        # l_args = self.gen_args(_id, seq, cols)
        # pool = mp.Pool()
        # ln = len(l_args)
        # for sha in tqdm(pool.imap_unordered(self.mp_sha, l_args), total=ln):
        #     lst.append(sha)
        # pool.close()

        for sdl_id, uid in df.groupby(_id):
            df_uid = pt.hash_frame(uid.sort_values(by=seq)[cols])
            lst.append([sdl_id, df_uid])

        on_cols = ["Id", table + "ScheduleId"]

        return pd.DataFrame(lst, columns=on_cols)

    @logged(level=logging.INFO, name=logger)
    def gen_new_id_on_cols(self, table, cols, _id):
        """Generate a sha1 for a serie."""
        logger.log(logging.INFO, "generate sha1 for {}".format(table))
        df = self.dict_df_mapped[table].copy()
        sdl_id = table + "ScheduleId"
        df[sdl_id] = pt.sha1_for_named_columns(df, cols)
        on_cols = [_id, sdl_id]

        return df[on_cols]

    @logged(level=logging.INFO, name=logger)
    def new_id(self):
        """Generate the table of correspondance for new id."""
        vstops = self.gen_new_id_on_cols(
            "Stop", ["Latitude", "Longitude"], "Id")
        vroutes = self.gen_new_id_on_cols(
            "Route", ["ShortName", "LongName"], "Id")
        vshapes = self.gen_new_id_on_seq(
            "Shape", ["Latitude", "Longitude", "GtfsId"],
            "ShapeSequence", "Id")
        vtrips = self.gen_new_id_on_seq(
            "StopTime", ["ArrivalTimeSeconds", "StopId", "GtfsId", "ServiceId"],
            "StopSequence", "TripId")
        vtrips = vtrips.rename(
            columns={"StopTimeScheduleId": "TripScheduleId"})

        return vstops, vroutes, vshapes, vtrips

    @logged(level=logging.INFO, name=logger)
    def merge_stops(self, vstops):
        """Change stop_id to new sha1."""
        stops = self.dict_df_mapped["Stop"].copy()
        stops = pd.merge(stops, vstops, on="Id")

        def merge_parent_station(stops, vstops):
            """Change the parent stop id to sha1."""
            vstops = vstops.copy()
            vstops = vstops.rename(columns={
                "Id": "ParentStopId", "StopScheduleId": "ParentScheduleId"})
            stops = pd.merge(stops, vstops, on="ParentStopId", how="left")

            return stops

        stops = merge_parent_station(stops, vstops)
        stops["Id"] = stops["StopScheduleId"]
        stops["ParentStopId"] = stops["ParentScheduleId"]
        stops = pt.change_nan_value(stops, None)

        return stops.drop_duplicates(subset="Id")

    @logged(level=logging.INFO, name=logger)
    def merge_routes(self, vroutes):
        """Change the route_id to new sha1."""
        routes = self.dict_df_mapped["Route"].copy()
        routes = pd.merge(routes, vroutes, on="Id")
        routes["Id"] = routes["RouteScheduleId"]
        routes["AgencyId"] = pt.change_nan_value(routes["AgencyId"], self.agency_id)

        return routes.drop_duplicates(subset="Id")

    @logged(level=logging.INFO, name=logger)
    def merge_stoptimes(self, vtrips, vstops):
        """Change the trip_id and stop_id with the new sha1."""
        stimes = self.dict_df_mapped["StopTime"].copy()
        vtrips = vtrips.rename(columns={"Id": "TripId"})
        vstops = vstops.rename(columns={"Id": "StopId"})
        stimes = pd.merge(stimes, vtrips, on="TripId")
        stimes = pd.merge(stimes, vstops, on="StopId")
        stimes["TripId"] = stimes["TripScheduleId"]
        stimes["StopId"] = stimes["StopScheduleId"]

        return stimes

    @logged(level=logging.INFO, name=logger)
    def merge_shapes(self, vshapes):
        """Change the shape_id to new sha1."""
        shapes = self.dict_df_mapped["Shape"].copy()
        shapes = pd.merge(shapes, vshapes, on="Id")
        shapes["Id"] = shapes["ShapeScheduleId"]
        shapes = pt.change_nan_value(shapes, None)

        return shapes

    @logged(level=logging.INFO, name=logger)
    def merge_trips(self, vtrips, vroutes, vshapes):
        """Change the trip_id and route_id  and shape_id to sha1."""
        trips = self.dict_df_mapped["Trip"].copy()
        vroutes = vroutes.rename(columns={"Id": "RouteId"})
        vshapes = vshapes.rename(columns={"Id": "ShapeId"})
        trips = pd.merge(trips, vtrips, on="Id", how="left")
        trips = pd.merge(trips, vroutes, on="RouteId")
        trips = pd.merge(trips, vshapes, on="ShapeId")
        trips["Id"] = trips["TripScheduleId"]
        trips["RouteId"] = trips["RouteScheduleId"]
        trips["ShapeId"] = trips["ShapeScheduleId"]

        return trips

    def merge_date_trips(self, vtrips):
        """Change the trip_id in trip2date."""
        ttd = self.dict_df_mapped["TripToDate"].copy()
        trips = vtrips[["Id", "TripScheduleId"]]
        ttd = pd.merge(ttd, trips, left_on="TripId", right_on="Id")
        ttd["TripId"] = ttd["TripScheduleId"]

        return ttd

    def set_trip_gtfs(self, vtrips, trips):
        """Create Trip2Gtfs."""
        vtrips["GtfsId"] = self.gtfs_id
        trips = trips[["Headsign", "ShortName", "TripScheduleId"]]
        vtrips = pd.merge(vtrips, trips, on="TripScheduleId")
        _id = vtrips["TripScheduleId"].copy()
        vtrips["TripScheduleId"] = vtrips["Id"]
        vtrips["Id"] = _id

        return vtrips

    def set_route_gtfs(self, vroutes, routes):
        """Create Route2Gtfs."""
        vroutes["GtfsId"] = self.gtfs_id
        routes = routes[["ShortName", "LongName", "RouteScheduleId"]]
        vroutes = pd.merge(vroutes, routes, on="RouteScheduleId")
        _id = vroutes["RouteScheduleId"].copy()
        vroutes["RouteScheduleId"] = vroutes["Id"]
        vroutes["Id"] = _id

        return vroutes

    def set_stop_gtfs(self, vstops, stops):
        """Create Stop2Gtfs."""
        vstops["GtfsId"] = self.gtfs_id
        stops = stops[["Name", "ZoneId", "StopScheduleId"]]
        vstops = pd.merge(vstops, stops, on="StopScheduleId")
        _id = vstops["StopScheduleId"].copy()
        vstops["StopScheduleId"] = vstops["Id"]
        vstops["Id"] = _id

        return vstops

    def set_shape_gtfs(self, vshapes):
        """Create Shape2Gtfs."""
        vshapes["GtfsId"] = self.gtfs_id

        return vshapes

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Generate all new entities id."""
        dict_df_version = dict()
        vstops, vroutes, vshapes, vtrips = self.new_id()
        dict_df_version["Gtfs"] = self.dict_df_mapped["Gtfs"]
        dict_df_version["Agency"] = self.dict_df_mapped["Agency"]
        dict_df_version["Calendar"] = self.dict_df_mapped["Calendar"]
        dict_df_version["CalendarDate"] = self.dict_df_mapped["CalendarDate"]
        routes = self.merge_routes(vroutes)
        stops = self.merge_stops(vstops)
        trips = self.merge_trips(vtrips, vroutes, vshapes)
        dict_df_version["StopTime"] = self.merge_stoptimes(vtrips, vstops)
        dict_df_version["TripToDate"] = self.merge_date_trips(vtrips)
        dict_df_version["Shape"] = self.merge_shapes(vshapes)
        dict_df_version["Route"] = routes
        dict_df_version["Stop"] = stops
        dict_df_version["Trip"] = trips
        dict_df_version["TripToGtfs"] = self.set_trip_gtfs(vtrips, trips)
        dict_df_version["RouteToGtfs"] = self.set_route_gtfs(vroutes, routes)
        dict_df_version["StopToGtfs"] = self.set_stop_gtfs(vstops, stops)
        dict_df_version["ShapeToGtfs"] = self.set_shape_gtfs(vshapes)

        return dict_df_version

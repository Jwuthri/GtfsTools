"""Generate the table TimeNDistances."""
import multiprocessing as mp
import itertools
import pandas as pd
import grequests
import urllib
from tqdm import tqdm
import logging
# from memory_profiler import profile

from utilities.distance import Distance
from mixer.settings import osrm, max_dist, speed
from utilities.decorator import logged
from mixer.glogger import logger


class TimeDist(object):
    """Generate the table TimeNDistances.

    The time and the distance btw 2 stops."""

    def __init__(self, df1, df2):
        """Constructor."""
        self.max_dist = max_dist
        self.osrm = osrm
        self.df1 = df1
        self.df2 = df2

    def fmt_loc(self, latlon):
        """Format the latlon."""
        return str(latlon)[1:-1].replace(" ", "")

    def extract_timedistance(self, response):
        """Extract value from json."""
        summary = response.json()["route_summary"]

        return round(summary["total_time"]), summary["total_distance"]

    def gen_timedistance_params(self, loc1, loc2):
        """Create the request."""
        params = {
            "loc": [self.fmt_loc(loc1), self.fmt_loc(loc2)],
            "z": 3
        }

        return params

    def is_less_than_x_meters(self, latlon1, latlon2, stop1, stop2):
        """Generate the TimeDist for all stops close enough."""
        distance = Distance().haversine(latlon1, latlon2)

        return (0 < distance < self.max_dist, latlon1, latlon2, stop1, stop2)

    def mp_is_less_than_x_meters(self, args):
        """Multiprocess t =he function."""
        return self.is_less_than_x_meters(*args)

    def compute_locs_timedistance(self, locs_seq, matrix={}):
        """Compute queries."""
        queries = [
            grequests.get(
                self.osrm,
                params=self.gen_timedistance_params(loc1, loc2),
                proxies={"http": None}
            )
            for loc1, loc2, stop1, stop2 in locs_seq
        ]

        responses = grequests.map(queries)
        times_dists = map(self.extract_timedistance, responses)

        return times_dists

    def time_dist_osrm(self, close_coords):
        """Compute all time dist thx to OSRM."""
        time_dist = self.compute_locs_timedistance(close_coords)

        df_stop = pd.DataFrame(
            close_coords, columns=["latlon1", "latlon2", "from_id", "to_id"])
        time_ndistance = pd.concat(
            [df_stop[["from_id", "to_id"]], pd.DataFrame(time_dist)], axis=1)
        if len(time_ndistance.columns) == 4:
            time_ndistance.columns = [
                "FromStopId", "ToStopId", "TimeSeconds", "DistanceMeters"
            ]
        else:
            time_ndistance = pd.DataFrame()

        return time_ndistance

    def _haversine_time_dist(self, from_coord, to_coord, from_stop, to_stop):
        """Compute time and dist btw 2 pts."""
        distance = Distance().haversine(from_coord, to_coord)
        time = distance / (speed / 3.6)
        df = pd.DataFrame(
            [{
                "FromStopId": from_stop, "ToStopId": to_stop,
                "TimeSeconds": int(time), "DistanceMeters": int(distance)
            }]
        )

        return df

    def mp_h_t_d(self, args):
        """Multiprocess haversine dist."""
        return self._haversine_time_dist(*args)

    def stops_dict(self, df1, df2):
        """Gen dict of stops."""
        df = pd.concat([df1, df2])
        df = df.drop_duplicates()

        return dict(
            (row["Id"], (row["Latitude"], row["Longitude"]))
            for idx, row in df.iterrows()
        )

    def gen_stops_matrix(self, df1, df2):
        """Gen matrix combination of stops."""
        return list(
            itertools.product(
                list(df1["Id"]),
                list(df2["Id"])
            )
        )

    def gen_args(self, stp_dict, stops_matrix):
        """Gen list of args."""
        return [
            (
                stp_dict.get(s1),
                stp_dict.get(s2),
                s1, s2
            )
            for s1, s2 in stops_matrix
        ]

    def gen_time_dist_args(self, close_coords):
        """."""
        return [
            (
                coords[0],
                coords[1],
                coords[2],
                coords[3]
            )
            for coords in close_coords
        ]

    def time_dist_haversine(self, close_coords):
        """Build table with haversine and random speed."""
        l_args = self.gen_time_dist_args(close_coords)
        msg = "Can't access to {}".format(self.osrm)
        logger.log(logging.WARNING, msg)
        logger.log(logging.INFO, "Using haversine and speed in settings")
        pool = mp.Pool()

        res = list()
        ln = len(l_args)
        for lst in tqdm(pool.imap_unordered(self.mp_h_t_d, l_args), total=ln):
            res.append(lst)
        pool.close()

        logger.log(logging.INFO, "Extracting the dataframe ...")

        return pd.concat(res)

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Gen TimeDist."""
        stp_dict = self.stops_dict(self.df1, self.df2)
        stops_matrix = self.gen_stops_matrix(self.df1, self.df2)
        logger.log(logging.INFO, "Define all stops pair close")
        l_args = self.gen_args(stp_dict, stops_matrix)
        pool = mp.Pool()
        close = pool.map(self.mp_is_less_than_x_meters, l_args)
        pool.close()

        close_coords_true = [x for x in close if x[0]]
        close_coords = list(map(lambda x: x[1:], close_coords_true))

        logger.log(logging.INFO, "Generate all the matrice Time/Dist")
        try:
            urllib.request.urlopen(self.osrm).getcode()
        except:
            if len(close_coords) > 0:
                return self.time_dist_haversine(close_coords)
            else:
                return pd.DataFrame()
        else:
            return self.time_dist_osrm(close_coords)

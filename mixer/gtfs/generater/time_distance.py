"""Generate the table TimeNDistances."""
import multiprocessing as mp
import itertools
import pandas as pd
from tqdm import tqdm
import logging

from utilities.distance import Distance
from mixer.settings import osrm, max_dist, speed
from utilities.decorator import logged
from mixer.glogger import logger


class TimeDist(object):
    """Generate the table TimeNDistances.

    The time and the distance btw 2 stops."""

    def __init__(self, from_stops, to_stops):
        """Constructor."""
        self.max_dist = max_dist
        self.osrm = osrm
        self.from_stops = from_stops
        self.to_stops = to_stops
        self.lst_fstops = list(from_stops["Id"].unique())
        self.lst_tstops = list(to_stops["Id"].unique())
        self.location = self.set_location()

    def set_location(self):
        df = pd.concat([self.from_stops, self.to_stops]).drop_duplicates()

        return dict(
            (row["Id"], (row["Latitude"], row["Longitude"]))
            for idx, row in df.iterrows()
        )

    def distance_btw_2_stops(self, fromstop, tostop):
        latlon1 = self.location.get(fromstop)
        latlon2 = self.location.get(tostop)
        distance = Distance().haversine(latlon1, latlon2)
        if 0 < distance < self.max_dist:
            return [fromstop, tostop]

    def one_to_n(self, fstop):
        l_possible = list()
        for tstop in self.lst_tstops:
            l_possible.append(self.distance_btw_2_stops(fstop, tstop))

        return list(filter(lambda x: x != None, l_possible))

    def _haversine_time_dist(self, fromstop, tostop):
        latlon1 = self.location.get(fromstop)
        latlon2 = self.location.get(tostop)
        distance = Distance().haversine(latlon1, latlon2)
        time = distance / (speed / 3.6)

        return fromstop, tostop, int(time), int(distance)

    def mp_h_t_d(self, args):
        return self._haversine_time_dist(*args)

    def get_close_stops(self):
        pool = mp.Pool()
        res = list()
        ln = len(self.lst_tstops)
        logger.log(logging.INFO, "Define all stops pair close")
        for lst in tqdm(pool.imap_unordered(self.one_to_n, self.lst_tstops), total=ln):
            res.append(lst)
        pool.close()

        return res

    def compute_time_dist_pair_stops(self, pair_stops):
        pool = mp.Pool()
        fs, ts, tms, dm = [], [], [], []
        ln = len(pair_stops)
        logger.log(logging.INFO, "Compute time & distance stops pair close")
        for lst in tqdm(pool.imap_unordered(self.mp_h_t_d, pair_stops), total=ln):
            fs.append(lst[0])
            ts.append(lst[1])
            tms.append(lst[2])
            dm.append(lst[3])

        return pd.DataFrame(data={
                "FromStopId": fs, "ToStopId": ts,
                "TimeSeconds": tms, "DistanceMeters": dm
            }
        )

    @logged(level=logging.INFO, name=logger)
    def main(self):
        close_stops = self.get_close_stops()
        close_stops = list(filter(lambda x: x != [], close_stops))
        pair_close_stops = list(itertools.chain(*close_stops))

        if len(pair_close_stops) > 0:
            return self.compute_time_dist_pair_stops(pair_close_stops)
        else:
            return pd.DataFrame()

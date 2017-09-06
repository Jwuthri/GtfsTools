"""Generate the stoptimes missed."""
from scipy.interpolate import UnivariateSpline
import numpy as np
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp

from utilities.distance import Distance

import warnings
warnings.filterwarnings("ignore")


class EstimateStopTimes(object):
    """Module to fill the stop times if we only have some of them."""

    def __init__(self):
        """Constructor."""
        pass

    @staticmethod
    def get_interpolator(x, y):
        """Gen the function of interpolation."""
        fxy = [
            (x, y) for x, y in zip(x, y)
            if not (np.isnan(y) or np.isnan(x))
        ]
        fx, fy = zip(*fxy)
        ip = UnivariateSpline(fx, fy, k=1, s=0)

        def interpolator(x):
            return float(ip(x))

        return interpolator

    @staticmethod
    def fill_times(group, x_column, y_column):
        """Fill the stop times missing."""
        estimator = EstimateStopTimes.get_interpolator(
            group[x_column], group[y_column])
        estimate_time = lambda row: estimator(
            row[x_column]) if np.isnan(row[y_column]) else row[y_column]
        group[y_column] = group.apply(estimate_time, axis=1)

        return group

    @staticmethod
    def time2seconds(t):
        """Transform time in seconds."""
        try:
            hour, minute, second = map(int, str(t).split(":"))
            sod = hour * 3600 + minute * 60 + second
            return sod
        except:
            return t

    @staticmethod
    def main(table):
        """Fill all the stop times missing."""
        x_column = "shape_dist_traveled"
        y_column = "arrival_time"
        tqdm.pandas()
        at_least_2 = (lambda g: g[y_column].dropna().size > 1)
        has_mintimes = table.groupby("trip_id").apply(at_least_2)
        bad_ones = list(has_mintimes[has_mintimes == False].index)

        if bad_ones:
            raise Exception("Unable to estimate trips")

        table[y_column] = table[y_column].apply(EstimateStopTimes.time2seconds)

        estimate_arrival_times = (
            lambda x: EstimateStopTimes.fill_times(x, x_column, y_column))
        table = table.groupby("trip_id").progress_apply(estimate_arrival_times)
        table["departure_time"] = table[y_column]

        return table


class EstimateShapeDistTrav(object):

    def __init__(self, df, stops):
        stops = stops[["stop_id", "stop_lat", "stop_lon"]]
        self.df = pd.merge(df, stops, on="stop_id")

    def compute_dist_btw_2_stops(self, origin, dest):
        l1 = [origin["stop_lat"], origin["stop_lon"]]
        l2 = [dest["stop_lat"], dest["stop_lon"]]

        return Distance().euclidean(l1, l2)

    def build_trip_shape_dist(self, trip_id):
        df = self.df[self.df["trip_id"] == trip_id]
        l_shape_dist_trav = list()
        dist = 0
        df.sort_values("stop_sequence", inplace=True)
        _, origin = next(df.iterrows())
        for _, dest in df.iterrows():
            dist += self.compute_dist_btw_2_stops(origin, dest)
            l_shape_dist_trav.append(dist)
            origin = dest
        df["shape_dist_traveled"] = l_shape_dist_trav

        return df

    def main(self):
        l_args = list(self.df["trip_id"].unique())
        pool = mp.Pool()
        ln = len(l_args)
        res = []
        print("Building the shape dist traveled...")
        # pbar = tqdm(total=ln)
        # for df in l_args:
        #    res.append(self.build_trip_shape_dist(df))
        #    pbar.update(1)
        # pbar.close()
        for stimes in tqdm(pool.imap_unordered(self.build_trip_shape_dist, l_args), total=ln):
            res.append(stimes)
        final_df = pd.concat(res)
        del final_df["stop_lat"]
        del final_df["stop_lon"]

        return final_df

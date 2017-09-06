"""All relative work about distance."""
import numpy as np

from scipy.interpolate import UnivariateSpline


class Distance(object):
    """Module to determine the euclidean distance between 2 points."""

    def __init__(self):
        self.EARTH_RADIUS_IN_METERS = 6372797.560856

    def loc2array(self, loc):
        lat, lon = loc

        return np.array([lat, lon])

    def _haversine(self, o, d):
        o_rad = np.radians(o)
        d_rad = np.radians(d)

        lat_arc, lon_arc = abs(o_rad - d_rad)
        a = np.sin(lat_arc * 0.5)**2 + (
            np.cos(o_rad[0]) * np.cos(d_rad[0]) * np.sin(lon_arc * 0.5)**2
        )
        c = 2 * np.arcsin(np.sqrt(a))

        return self.EARTH_RADIUS_IN_METERS * c

    def haversine(self, origin, destination):
        o = self.loc2array(origin)
        d = self.loc2array(destination)

        return self._haversine(o, d)

    def euclidean(self, origin, destination):
        o = self.loc2array(origin)
        d = self.loc2array(destination)

        return np.linalg.norm(d - o)


def get_interpolator(x, y):
    """A linear interpolator, with extrapolation outside bbox."""
    ip = UnivariateSpline(x, y, k=1, s=0)

    def interpolator(x):
        return float(ip(x))

    return interpolator

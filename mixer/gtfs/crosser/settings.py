"""Settings for gtfs intersection."""
from collections import namedtuple

_NT = namedtuple("Table", ["set", "where"])

dict_nt = dict()

# dict_nt["StopTime"] = _NT(["EndDate", "StartDate"], ["TripId"])
dict_nt["TripToShape"] = _NT(["EndDate", "StartDate"], ["TripId"])

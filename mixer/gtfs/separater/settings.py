"""Settings to know which table to read and update."""
from collections import namedtuple

_NT_NEW_ROW = namedtuple("Gtfs", ["table", "compare_id"])
dict_row = dict()

_TABLE_UPDATE = [
    "Stop", "Route",
    "StopTime", "TripToShape"
]

_UNCHANGED_TABLE = [
    "Gtfs", "Calendar", "CalendarDate",
    "TripToGtfs", "RouteToGtfs", "StopToGtfs", "TripToDate",
]

dict_row["Stop"] = _NT_NEW_ROW("Stop", "Id")
dict_row["Route"] = _NT_NEW_ROW("Route", "Id")
dict_row["Agency"] = _NT_NEW_ROW("Agency", "Id")
dict_row["Trip"] = _NT_NEW_ROW("Trip", "Id")
dict_row["Shape"] = _NT_NEW_ROW("Shape", "Id")
dict_row["TripToShape"] = _NT_NEW_ROW("TripToShape", "TripId")
dict_row["RouteToTrip"] = _NT_NEW_ROW("RouteToTrip", "TripId")
dict_row["StopTime"] = _NT_NEW_ROW("StopTime", "TripId")

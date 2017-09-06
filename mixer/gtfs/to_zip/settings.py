"""Table Settings."""
from collections import namedtuple

NT_TABLE = namedtuple("Table", ["as_gtfs", "as_db"])
dict_subset = dict()

dict_subset["stops.txt"] = NT_TABLE("stops.txt", "Stop")
dict_subset["routes.txt"] = NT_TABLE("routes.txt", "Route")
dict_subset["agency.txt"] = NT_TABLE("agency.txt", "Agency")
dict_subset["calendar.txt"] = NT_TABLE("calendar.txt", "Calendar")
dict_subset["stop_times.txt"] = NT_TABLE("stop_times.txt", "StopTime")
dict_subset["trips.txt"] = NT_TABLE("trips.txt", "Trip")
dict_subset["shapes.txt"] = NT_TABLE("shapes.txt", "Shape")
dict_subset["calendar_dates.txt"] = NT_TABLE(
    "calendar_dates.txt", "CalendarDate")

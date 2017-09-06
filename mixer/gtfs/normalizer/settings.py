"""Settings for the gtfs columns requiered and optional."""
from collections import namedtuple

NT_NORM = namedtuple("Normalize", ["optional", "requiered"])
dict_normalize = dict()

##########
# AGENCY #
##########

agency = [
    "agency_id", "agency_lang", "agency_phone", "agency_fare_url"
]
req_agency = agency + [
    "agency_name", "agency_url", "agency_timezone"
]

############
# CALENDAR #
############

calendar = []
req_calendar = calendar + [
    "service_id", "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday", "start_date", "end_date"
]

##################
# CALENDAR DATES #
##################

calendar_dates = []
req_calendar_dates = calendar_dates + [
    "date", "service_id", "exception_type"
]

##########
# ROUTES #
##########

routes = [
    "agency_id", "route_desc", "route_color", "route_text_color"
]
req_routes = routes + [
    "route_id", "route_short_name", "route_long_name",
    "route_type", "route_url"
]

#########
# STOPS #
#########

stops = [
    "stop_code", "stop_desc", "zone_id", "stop_url",
    "location_type", "parent_station",
    "stop_timezone", "wheelchair_boarding"
]
req_stops = stops + [
    "stop_id", "stop_name", "stop_lat", "stop_lon"
]

#############
# STOPTIMES #
#############

stimes = [
    "stop_headsign", "pickup_type", "drop_off_type",
    "shape_dist_traveled", "timepoint"
]
req_stimes = stimes + [
    "trip_id", "arrival_time", "departure_time",
    "stop_id", "stop_sequence"
]

##########
# SHAPES #
##########

shapes = [
    "shape_dist_traveled"
]

req_shapes = shapes + [
    "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
]

#########
# TRIPS #
#########

trips = [
    "trip_headsign", "trip_short_name",
    "direction_id", "block_id", "shape_id",
    "wheelchair_accessible", "bikes_allowed"
]
req_trips = trips + [
    "route_id", "service_id", "trip_id"
]

dict_normalize["Agency"] = NT_NORM(agency, req_agency)
dict_normalize["Calendar"] = NT_NORM(calendar, req_calendar)
dict_normalize["CalendarDate"] = NT_NORM(calendar_dates, req_calendar_dates)
dict_normalize["Route"] = NT_NORM(routes, req_routes)
dict_normalize["Stop"] = NT_NORM(stops, req_stops)
dict_normalize["StopTime"] = NT_NORM(stimes, req_stimes)
dict_normalize["Shape"] = NT_NORM(shapes, req_shapes)
dict_normalize["Trip"] = NT_NORM(trips, req_trips)

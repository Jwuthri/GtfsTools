"""Settings for the GTFS reader."""
from collections import namedtuple

NT_FILES_ = namedtuple("File", ["as_type", "id_field"])
dict_files = dict()

##########
# AGENCY #
##########

agency_type = {
    'agency_id': str, 'agency_name': str,
    'agency_url': str, 'agency_timezone': str
}
agency_id_field = ["agency_id"]

############
# CALENDAR #
############

calendar_type = {
    'service_id': str, 'monday': int, 'tuesday': int, 'wednesday': int,
    'thursday': int, 'friday': int, 'saturday': int, 'sunday': int,
    'start_date': int, 'end_date': int
}
calendar_id_field = ["service_id"]

##################
# CALENDAR DATES #
##################

cal_date_type = {
    'service_id': str, 'date': int, "exception_type": int
}
cal_date_id_field = ["service_id"]

##########
# ROUTES #
##########

routes_type = {
    'route_id': str, 'route_short_name': str, 'route_long_name': str,
    'route_type': int, 'route_color': str, 'route_text_color': str
}
routes_id_field = ["route_id"]

#########
# STOPS #
#########

stops_type = {
    'stop_id': str, 'stop_code': str, 'stop_name': str,
    'stop_lat': float, 'stop_lon': float
}
stops_id_field = ["stop_id"]

#############
# STOPTIMES #
#############

stop_times_type = {
    'trip_id': str, 'arrival_time': str, 'departure_time': str,
    'stop_id': str, 'stop_sequence': int
}
stop_times_id_field = ["stop_id", "trip_id"]

##########
# SHAPES #
##########

shapes_type = {
    'shape_id': str, 'shape_pt_lat': float,
    'shape_pt_lon': float, 'shape_pt_sequence': int
}
shapes_id_field = ["shape_id"]

#########
# TRIPS #
#########

trips_type = {
    'route_id': str, 'service_id': str, 'trip_id': str,
    'trip_headsign': str
}
trips_id_field = ["trip_id"]

dict_files["agency.txt"] = NT_FILES_(agency_type, agency_id_field)
dict_files["calendar.txt"] = NT_FILES_(calendar_type, calendar_id_field)
dict_files["routes.txt"] = NT_FILES_(routes_type, routes_id_field)
dict_files["stops.txt"] = NT_FILES_(stops_type, stops_id_field)
dict_files["stop_times.txt"] = NT_FILES_(stop_times_type, stop_times_id_field)
dict_files["trips.txt"] = NT_FILES_(trips_type, trips_id_field)
optional_shapes = NT_FILES_(shapes_type, shapes_id_field)
optional_cal_dates = NT_FILES_(cal_date_type, cal_date_id_field)

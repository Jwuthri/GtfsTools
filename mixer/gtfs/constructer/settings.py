"""Settings for the gtfs columns mapper btw GTFS and DB."""
from collections import namedtuple


NT_MAPPER_ = namedtuple("Map", ["mapping", "sub_cols"])
dict_mapper = dict()

##########
# AGENCY #
##########

agency_map = {
    "Id": "agency_id",
    "Name": "agency_name",
    "Url": "agency_url",
    "Timezone": "agency_timezone",
    "Language": "agency_lang",
    "Phone": "agency_phone"
}
agency_cols = [
    'agency_id', 'agency_name', 'agency_url',
    'agency_timezone', 'agency_lang', 'agency_phone'
]

############
# CALENDAR #
############

calendar_map = {
    "ServiceId": "service_id",
    "StartDate": "start_date",
    "EndDate": "end_date",
    "Monday": "monday",
    "Tuesday": "tuesday",
    "Wednesday": "wednesday",
    "Thursday": "thursday",
    "Friday": "friday",
    "Saturday": "saturday",
    "Sunday": "sunday"
}
calendar_cols = [
    'service_id', 'start_date', 'end_date',
    'monday', 'tuesday', 'wednesday',
    'thursday', 'friday', 'saturday', 'sunday'
]

##################
# CALENDAR DATES #
##################

cal_dates_map = {
    "ServiceId": "service_id",
    "Date": "date",
    "Exception": "exception_type",
    "ExceptionType": "exception_type"
}
cal_dates_cols = ['service_id', 'date', 'exception_type']

##########
# ROUTES #
##########

routes_map = {
    "Id": "route_id",
    "AgencyId": "agency_id",
    "ShortName": "route_short_name",
    "LongName": "route_long_name",
    "Description": "route_desc",
    "Type": "route_type",
    "Url": "route_url",
    "Color": "route_color",
    "TextColor": "route_text_color"
}
routes_cols = [
    'route_id', 'agency_id', 'route_short_name', 'route_long_name',
    'route_desc', 'route_type', 'route_url', 'route_color', 'route_text_color'
]

#########
# STOPS #
#########

stops_map = {
    "Id": "stop_id",
    "Code": "stop_code",
    "Latitude": "stop_lat",
    "Longitude": "stop_lon",
    "Name": "stop_name",
    "Type": "location_type",
    "ParentStopId": "parent_station",
    "Description": "stop_description",
    "Url": "stop_url",
    "ZoneId": "zone_id"
}
stops_cols = [
    'stop_id', 'stop_code', 'stop_lat', 'stop_lon',
    'stop_name', 'location_type', 'parent_station',
    'stop_description', 'stop_url', 'zone_id',
]

#############
# STOPTIMES #
#############

stimes_map = {
    "TripId": "trip_id",
    "StopId": "stop_id",
    "StopSequence": "stop_sequence",
    "ArrivalTimeSeconds": "arrival_time",
    "DepartureTimeSeconds": "departure_time",
    "DistanceTraveled": "shape_dist_traveled",
    "StopHeadsign": "stop_headsign",
    "DropOffType": "drop_off_type",
    "PickupType": "pickup_type"
}
stimes_cols = [
    'trip_id', 'stop_id', 'stop_sequence',
    'arrival_time', 'departure_time',
    'shape_dist_traveled', 'stop_headsign',
    'drop_off_type', 'pickup_type'
]
##########
# SHAPES #
##########

shapes_map = {
    "Id": "shape_id",
    "Longitude": "shape_pt_lon",
    "Latitude": "shape_pt_lat",
    "Sequence": "shape_pt_sequence",
    "ShapeSequence": "shape_pt_sequence"
}
shapes_cols = [
    'shape_id', 'shape_pt_lon', 'shape_pt_lat', 'shape_pt_sequence'
]

#########
# TRIPS #
#########

trips_map = {
    "Id": "trip_id",
    "ServiceId": "service_id",
    "RouteId": "route_id",
    "ShapeId": "shape_id",
    "Direction": "direction_id",
    "Headsign": "trip_headsign",
    "BlockId": "block_id"
}
trips_cols = [
    'trip_id', 'service_id', 'route_id', 'shape_id',
    'direction_id', 'trip_headsign', 'block_id'
]


dict_mapper["Agency"] = NT_MAPPER_(agency_map, agency_cols)
dict_mapper["Calendar"] = NT_MAPPER_(calendar_map, calendar_cols)
dict_mapper["CalendarDate"] = NT_MAPPER_(cal_dates_map, cal_dates_cols)
dict_mapper["Route"] = NT_MAPPER_(routes_map, routes_cols)
dict_mapper["Stop"] = NT_MAPPER_(stops_map, stops_cols)
dict_mapper["StopTime"] = NT_MAPPER_(stimes_map, stimes_cols)
dict_mapper["Shape"] = NT_MAPPER_(shapes_map, shapes_cols)
dict_mapper["Trip"] = NT_MAPPER_(trips_map, trips_cols)

"""Settings for the gtfs columns mapper btw GTFS and DB."""
from collections import namedtuple


NT_MAPPER_ = namedtuple("Map", ["mapping", "sub_cols"])
dict_mapper = dict()

########
# GTFS #
########

gtfs_map = {
    'gtfs_id': 'Id',
    'start_date': 'StartDate',
    'end_date': 'EndDate'
}
gtfs_cols = ['StartDate', 'Id', 'EndDate']

##########
# AGENCY #
##########

agency_map = {
    'agency_lang': 'Language',
    'agency_id': 'Id',
    'agency_name': 'Name',
    'agency_url': 'Url',
    'agency_phone': 'Phone',
    'agency_timezone': 'Timezone'
}
agency_cols = ['Id', 'Name', 'Timezone', 'Url', 'Language', 'Phone']

############
# CALENDAR #
############

calendar_map = {
    'service_id': 'ServiceId',
    'monday': 'Monday',
    'tuesday': 'Tuesday',
    'wednesday': 'Wednesday',
    'thursday': 'Thursday',
    'friday': 'Friday',
    'saturday': 'Saturday',
    'sunday': 'Sunday',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'gtfs_id': 'GtfsId'
}
calendar_cols = [
    'ServiceId', 'StartDate', 'EndDate',
    'Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday',
    'Sunday', 'GtfsId'
]

##################
# CALENDAR DATES #
##################

cal_dates_map = {
    'service_id': 'ServiceId',
    'date': 'Date',
    'exception_type': 'ExceptionType',
    'gtfs_id': 'GtfsId'
}
cal_dates_cols = ['Date', 'ServiceId', 'ExceptionType', 'GtfsId']

##########
# ROUTES #
##########

routes_map = {
    'route_id': 'Id',
    'route_type': 'Type',
    'route_short_name': 'ShortName',
    'route_long_name': 'LongName',
    'route_desc': 'Description',
    'route_url': 'Url',
    'route_color': 'Color',
    'route_text_color': 'TextColor',
    'agency_id': 'AgencyId',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'gtfs_id': 'GtfsId'
}
routes_cols = [
    'Id', 'StartDate', 'EndDate', 'Type',
    'ShortName', 'LongName', 'Description', 'Url',
    'Color', 'TextColor', 'AgencyId', 'GtfsId'
]

#########
# STOPS #
#########

stops_map = {
    'stop_id': 'Id',
    'stop_name': 'Name',
    'stop_lat': 'Latitude',
    'stop_lon': 'Longitude',
    'stop_code': 'Code',
    'location_type': 'Type',
    'stop_url': 'Url',
    'stop_desc': 'Description',
    'zone_id': 'ZoneId',
    'parent_station': 'ParentStopId',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'gtfs_id': 'GtfsId'
}
stops_cols = [
    'Id', 'Name', 'StartDate', 'EndDate',
    'Latitude', 'Longitude', 'Code',
    'Type', 'Url', 'Description',
    'ZoneId', 'ParentStopId', 'GtfsId'
]

#############
# STOPTIMES #
#############

stimes_map = {
    'stop_id': 'StopId',
    'trip_id': 'TripId',
    'stop_sequence': 'StopSequence',
    'arrival_time': 'ArrivalTimeSeconds',
    'departure_time': 'DepartureTimeSeconds',
    'stop_headsign': 'StopHeadsign',
    'drop_off_type': 'DropOffType',
    'pickup_type': 'PickupType',
    'shape_dist_traveled': 'DistanceTraveled',
    'gtfs_id': 'GtfsId',
    'service_id': 'ServiceId'
}
stimes_cols = [
    'StopId', 'TripId', 'GtfsId',
    'StopSequence', 'ArrivalTimeSeconds',
    'DepartureTimeSeconds', 'StopHeadsign',
    'DropOffType', 'PickupType', 'DistanceTraveled', 'ServiceId'
]
##########
# SHAPES #
##########

shapes_map = {
    'shape_id': 'Id',
    'shape_pt_lat': 'Latitude',
    'shape_pt_lon': 'Longitude',
    'shape_pt_sequence': 'ShapeSequence',
    'shape_dist_traveled': 'DistanceTraveled',
    'gtfs_id': 'GtfsId'
}
shapes_cols = [
    'Id', 'Latitude', 'Longitude',
    'ShapeSequence', 'DistanceTraveled', 'GtfsId'
]

#########
# TRIPS #
#########

trips_map = {
    'trip_id': 'Id',
    'service_id': 'ServiceId',
    'route_id': 'RouteId',
    'trip_headsign': 'Headsign',
    'direction_id': 'Direction',
    'block_id': 'BlockId',
    'shape_id': 'ShapeId',
    'trip_short_name': 'ShortName',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'gtfs_id': 'GtfsId'
}
trips_cols = [
    'Id', 'ServiceId', 'RouteId', 'Headsign',
    'Direction', 'BlockId', 'ShapeId', 'ShortName',
    'StartDate', 'EndDate', 'GtfsId'
]

##############
# TRIPTODATE #
##############

trpdate_map = {
    "trip_id": "TripId",
    "service_id": "ServiceId",
    "date": "Date",
    "gtfs_id": "GtfsId"
}
trpdate_cols = ["TripId", "ServiceId", "Date", "GtfsId"]


dict_mapper["Gtfs"] = NT_MAPPER_(gtfs_map, gtfs_cols)
dict_mapper["Agency"] = NT_MAPPER_(agency_map, agency_cols)
dict_mapper["Calendar"] = NT_MAPPER_(calendar_map, calendar_cols)
dict_mapper["CalendarDate"] = NT_MAPPER_(cal_dates_map, cal_dates_cols)
dict_mapper["Route"] = NT_MAPPER_(routes_map, routes_cols)
dict_mapper["Stop"] = NT_MAPPER_(stops_map, stops_cols)
dict_mapper["StopTime"] = NT_MAPPER_(stimes_map, stimes_cols)
dict_mapper["Shape"] = NT_MAPPER_(shapes_map, shapes_cols)
dict_mapper["Trip"] = NT_MAPPER_(trips_map, trips_cols)
dict_mapper["TripToDate"] = NT_MAPPER_(trpdate_map, trpdate_cols)

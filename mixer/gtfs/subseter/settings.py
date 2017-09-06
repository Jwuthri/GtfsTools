"""Select the cols in the GTFS files to generate out db s."""
from collections import namedtuple

NT_SUBSET_ = namedtuple("Subset", ["initial", "sub_cols", "rename"])
dict_subset = dict()

########
# GTFS #
########

_gtfs = ['StartDate', 'Id', 'EndDate']

##########
# AGENCY #
##########

_agency = ['Id', 'Name', 'Timezone', 'Url', 'Language', 'Phone']

############
# CALENDAR #
############

_calendar = [
    'ServiceId', 'StartDate', 'EndDate', 'Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday', 'Sunday', 'GtfsId'
]

##################
# CALENDAR DATES #
##################

_cal_dates = ['Date', 'ServiceId', 'ExceptionType', 'GtfsId']

##########
# ROUTES #
##########

_routes = [
    'Id', 'StartDate', 'EndDate', 'Type', 'ShortName', 'LongName',
    'Description', 'Url', 'Color', 'TextColor', 'AgencyId'
]

#########
# STOPS #
#########

_stops = [
    'Id', 'Name', 'StartDate', 'EndDate', 'Latitude', 'Longitude',
    'Code', 'Type', 'Url', 'Description', 'ZoneId', 'ParentStopId'
]

##########
# SHAPES #
##########

_shapes = [
    'Id', 'Latitude', 'Longitude', 'ShapeSequence', 'DistanceTraveled'
]

#############
# STOPTIMES #
#############

_stimes = [
    'StopId', 'TripId', 'StopSequence', 'ArrivalTimeSeconds',
    'DepartureTimeSeconds', 'StopHeadsign',
    'DropOffType', 'PickupType', 'DistanceTraveled'
]

#########
# TRIPS #
#########

_trips = ['Id', 'Headsign', 'Direction', 'BlockId', 'ShortName']

###############
# TRIP 2 GTFS #
###############

_trip_to_gtfs = [
    "GtfsId", "Id", "TripScheduleId", "Headsign", "ShortName"
]

#################
# TRIP 2 SHAPES #
#################

_trip_to_shape = ["Id", "ShapeId", "StartDate", "EndDate"]

###############
# TRIP 2 DATE #
###############

_trip_to_date = ["TripId", "ServiceId", "Date", "GtfsId"]

################
# STOP TO GTFS #
################

_stop_to_gtfs = ["GtfsId", "Id", "StopScheduleId", "Name", "ZoneId"]

#################
# ROUTE TO GTFS #
#################

_route_to_gtfs = [
    "GtfsId", "Id", "RouteScheduleId", "ShortName", "LongName"
]

#################
# ROUTE TO TRIP #
#################

_route_to_trip = ["RouteId", "Id"]

dict_subset["Gtfs"] = NT_SUBSET_("Gtfs", _gtfs, None)
dict_subset["Agency"] = NT_SUBSET_("Agency", _agency, None)
dict_subset["Calendar"] = NT_SUBSET_("Calendar", _calendar, None)
dict_subset["CalendarDate"] = NT_SUBSET_("CalendarDate", _cal_dates, None)
dict_subset["Route"] = NT_SUBSET_("Route", _routes, None)
dict_subset["Stop"] = NT_SUBSET_("Stop", _stops, None)
dict_subset["StopTime"] = NT_SUBSET_("StopTime", _stimes, None)
dict_subset["Shape"] = NT_SUBSET_("Shape", _shapes, None)
dict_subset["Trip"] = NT_SUBSET_("Trip", _trips, None)
dict_subset["TripToDate"] = NT_SUBSET_("TripToDate", _trip_to_date, None)
dict_subset["TripToGtfs"] = NT_SUBSET_("TripToGtfs", _trip_to_gtfs, "TripId")
dict_subset["StopToGtfs"] = NT_SUBSET_("StopToGtfs", _stop_to_gtfs, "StopId")
dict_subset["RouteToTrip"] = NT_SUBSET_("Trip", _route_to_trip, "TripId")
dict_subset["TripToShape"] = NT_SUBSET_("Trip", _trip_to_shape, "TripId")
dict_subset["RouteToGtfs"] = NT_SUBSET_(
    "RouteToGtfs", _route_to_gtfs, "RouteId")

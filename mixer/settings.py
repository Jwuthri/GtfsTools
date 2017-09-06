"""Settings for the mixer."""
import os

# print the log in the console or not
DEBUG = True

# database ["sql_server", "oracle", ...]
db_type = "sql_server"

# osrm url
osrm = "http://vigie:5000/viaroute"

# time dist table, max dist in meters
max_dist = 2000

# speed of walker km/h
speed = 5

# safe mode (commit only if, we don't have error during the insertion)
safe_mode = True

########
# PATH #
########

ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = os.path.join(ABSOLUTE_PATH, 'data', 'gtfs_nancy.zip')

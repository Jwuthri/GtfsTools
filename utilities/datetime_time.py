"""Function relative to the time."""
import datetime
import math
import time
from dateutil import parser


def range_date(start, end):
    liste_date = list()
    delta = datetime.timedelta(days=1)
    while(end >= start):
        liste_date.append(start)
        start = start + delta

    return liste_date


def time2seconds(dt):
    if isinstance(dt, float) and math.isnan(dt):
        return dt
    if isinstance(dt, datetime.datetime):
        times = dt.time()
        seconds = 3600 * times.hour + 60 * times.minute + times.second
    if isinstance(dt, str):
        dt = parser.parse(dt)
        seconds = time2seconds(dt)

    return seconds


def as_sod(x):
    if isinstance(x, float) and math.isnan(x):
        return x

    hour, minute, second = map(int, str(x).split(":"))
    sod = hour * 3600 + minute * 60 + second

    return sod


def value2date(str_date):
    return parser.parse(str(str_date)).date()


def date_scd2date(date, sod):
    if isinstance(date, str):
        date = value2date(date)
    if isinstance(sod, str):
        sod = int(float(sod))
    ts = time.mktime(date.timetuple()) + int(sod)

    return datetime.datetime.fromtimestamp(ts)

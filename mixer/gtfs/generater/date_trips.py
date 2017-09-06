"""Generate a df for all trips available for each day."""
import datetime
from tqdm import tqdm

import pandas as pd


class DateTrips(object):
    """Trips of a day."""

    def __init__(self, trips, calendar, cal_dates, gtfs):
        """Constructor."""
        self.trips = trips
        self.gtfs = gtfs
        self.calendar = calendar
        self.cal_dates = cal_dates

    def get_services_trips(self):

        service_trips = dict(
            [
                (serv, set(group["trip_id"]))
                for serv, group in self.trips.groupby("service_id")
            ]
        )

        return service_trips

    def get_calendars_date_services(self):
        day_services = {}
        first = min(self.calendar["start_date"])
        end = max(self.calendar["end_date"])
        day_range = range((end - first).days + 1)

        for day_idx in day_range:
            day = first + datetime.timedelta(days=day_idx)

            weekday = day.strftime("%A").lower()
            serv_selection = self.calendar[
                (self.calendar[weekday] == 1) &
                (self.calendar["start_date"] <= day) &
                (self.calendar["end_date"] >= day)
            ]
            services = set(serv_selection["service_id"])

            day_eserv = self.cal_dates[self.cal_dates["date"] == day]

            add_services = set(
                day_eserv[day_eserv["exception_type"] == 1]["service_id"])
            del_services = set(
                day_eserv[day_eserv["exception_type"] == 2]["service_id"])

            services.update(add_services)
            services.discard(del_services)

            day_services[day] = services

        return day_services

    def main(self):
        date_trips_cols = ["trip_id", "service_id", "date", "gtfs_id"]

        day_services = self.get_calendars_date_services()
        service_trips = self.get_services_trips()

        day_trips = []
        pbar = tqdm(total=len(day_services))
        for day, services in day_services.items():
            for service in services:
                for trip in service_trips.get(service, []):
                    day_trips.append(
                        (trip, service, day, self.gtfs["gtfs_id"][0])
                    )
            pbar.update(1)
        pbar.close()

        return pd.DataFrame(day_trips, columns=date_trips_cols)

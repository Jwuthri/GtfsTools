"""Transform the df into csv, and then build the zip."""
import os
import shutil
import logging

from mixer.glogger import logger


class Writer(object):

    def __init__(self, gtfs_file, name, path=os.getcwd() + "/", xtp=False):
        self.gtfs_file = gtfs_file
        self.xtp = xtp
        self.path = path
        self.name = name

    def calendar_xtp(self, row):
        return row.strftime("%Y%m%d")

    def gtfs_writer(self):
        _path = self.path + self.name
        if not os.path.exists(_path):
            logger.log(logging.INFO, "Creating zip")
            os.makedirs(_path)
        else:
            logger.log(logging.WARNING, "This zip already exist")

        path = _path + "/"
        cal = self.gtfs_file["calendar"]
        if self.xtp:
            cal["start_date"] = cal["start_date"].map(self.calendar_xtp)
            cal["end_date"] = cal["end_date"].map(self.calendar_xtp)
        self.gtfs_file["agency"].to_csv(path + "agency.txt", index=False)
        cal.to_csv(path + "calendar.txt", index=False)
        self.gtfs_file["calendar_dates"].to_csv(
            path + "calendar_dates.txt", index=False)
        self.gtfs_file["routes"].to_csv(path + "routes.txt", index=False)
        self.gtfs_file["stops"].to_csv(path + "stops.txt", index=False)
        self.gtfs_file["stoptimes"].to_csv(
            path + "stop_times.txt", index=False)
        self.gtfs_file["shapes"].to_csv(path + "shapes.txt", index=False)
        self.gtfs_file["trips"].to_csv(path + "trips.txt", index=False)

        os.makedirs(_path + '.zip')
        shutil.rmtree(_path + '.zip')
        shutil.make_archive(path, 'zip', self.name)

        return self.path, self.name + ".zip"

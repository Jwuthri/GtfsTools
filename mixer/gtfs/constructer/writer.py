"""Create the zip file which contains all the files."""
import os
import shutil


class Writer(object):

    def __init__(self, gtfs_file, date, db_name, schedule_type, path=os.getcwd() + "/"):
        self.gtfs_file = gtfs_file
        self.path = path
        self.date = date
        self.db_name = db_name
        self.schedule_type = schedule_type

    def gtfs_writer(self):
        gtfs_name = self.schedule_type.lower() + "_" + self.db_name.lower() + "_" + str(self.date)

        if not os.path.exists(self.path + gtfs_name):
            os.makedirs(self.path + gtfs_name)

        path = self.path + gtfs_name + "/"

        self.gtfs_file["agency"].to_csv(path + "agency.txt", index=False)
        self.gtfs_file["calendar"].to_csv(path + "calendar.txt", index=False)
        self.gtfs_file["calendar_dates"].to_csv(
            path + "calendar_dates.txt", index=False)
        self.gtfs_file["routes"].to_csv(path + "routes.txt", index=False)
        self.gtfs_file["stops"].to_csv(path + "stops.txt", index=False)
        self.gtfs_file["stoptimes"].to_csv(
            path + "stop_times.txt", index=False)
        self.gtfs_file["shapes"].to_csv(path + "shapes.txt", index=False)
        self.gtfs_file["trips"].to_csv(path + "trips.txt", index=False)

        archive_name = os.path.expanduser(os.path.join(self.path + gtfs_name))
        root_dir = os.path.expanduser(os.path.join(path))
        shutil.make_archive(archive_name, 'zip', root_dir)

        return archive_name + '.zip'

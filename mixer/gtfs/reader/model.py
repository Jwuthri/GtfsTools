"""Read a GTFS with the constraintes in settings.py."""
import logging
from zipfile import ZipFile

from utilities.utils import list_zip_files
from mixer.gtfs.reader.reader import Reader
import mixer.gtfs.reader.settings as SETTINGS
from utilities.decorator import logged
from mixer.glogger import logger


class Model(object):
    """Read a gtfs zip."""

    def __init__(self, gtfs_path):
        """Constructor.

        :params gtfs_path: the gtfs path
        :type gtfs_path: str
        """
        self.gtfs_path = gtfs_path
        self.dict_files = SETTINGS.dict_files
        self._shapes = SETTINGS.optional_shapes
        self._cal_date = SETTINGS.optional_cal_dates

    @property
    def set_optional_file(self):
        """Shapes and Calendar dates are optional."""
        if "shapes.txt" in list_zip_files(self.gtfs_path):
            self.dict_files["shapes.txt"] = self._shapes

        if "calendar_dates.txt" in list_zip_files(self.gtfs_path):
            self.dict_files["calendar_dates.txt"] = self._cal_date

    @logged(level=logging.INFO, name=logger)
    def main(self):
        """Transform a GTFS zip into dict of df."""
        gtfs_zipped = ZipFile(self.gtfs_path)
        dict_gtfs = dict()
        self.set_optional_file
        tables = self.dict_files.keys()
        reader = Reader(gtfs_zipped, self.dict_files)
        for table in tables:
            dict_gtfs[table] = reader.read_file(table)

        return dict_gtfs

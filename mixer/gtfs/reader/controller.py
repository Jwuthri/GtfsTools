"""Transform a GTFS to a dict of df"""
import os

from mixer.gtfs.reader.model import Model


class Controller(object):
    """For a given GTFS zip return a dict of df."""

    def __init__(self, gtfs_path):
        """Constructor.

        :params gtfs_path: the gtfs path
        :type gtfs_path: str
        """
        if not os.path.exists(gtfs_path):
            raise '{} is not a path'.format(gtfs_path)
        self.gtfs_path = gtfs_path

    def main(self):
        """Read a gtfs as a dict of df."""
        model = Model(self.gtfs_path)

        return model.main()

"""Transform a dict of df to new dict of df normalized."""
from mixer.gtfs.normalizer.model import Model


class Controller(object):
    """For a given GTFS zip return a dict of df."""

    def __init__(self, gtfs_path, dict_gtfs):
        """Constructor."""
        self.gtfs_path = gtfs_path
        self.dict_gtfs = dict_gtfs

    def main(self):
        """Gen the df normalized."""
        model = Model(self.gtfs_path, self.dict_gtfs)

        return model.main()

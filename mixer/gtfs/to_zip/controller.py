"""Controller."""
from mixer.gtfs.to_zip.model import Model
from mixer.gtfs.to_zip.writer import Writer
from mixer.settings import DATA_PATH

from mixer import settings


class Controller(object):
    """For a given GTFS zip return a dict of df."""

    def __init__(self, gtfs_path=settings.DATA_PATH, xtp=False):
        """Constructor."""
        self.gtfs_path = gtfs_path
        self.xtp = xtp

    def main(self):
        """Gen the df normalized."""
        model = Model(self.gtfs_path)
        dict_df, filename = model.main()
        writer = Writer(dict_df, filename, xtp=self.xtp)

        return writer.gtfs_writer()


if __name__ == "__main__":
    Controller(gtfs_path=DATA_PATH, xtp=False).main()

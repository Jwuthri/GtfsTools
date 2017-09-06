"""Select all columns on the files to generate all our tables."""
from mixer.gtfs.subseter.model import Model


class Controller(object):
    """For a dict of df, select a subset of df."""

    def __init__(self, dict_df_versioned):
        """Constructor."""
        self.dict_df_versioned = dict_df_versioned

    def main(self):
        """Gen the sub df."""
        model = Model(self.dict_df_versioned)

        return model.main()

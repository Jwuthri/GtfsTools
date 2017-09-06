"""Map the columns df with the one in database."""
from mixer.gtfs.mapper.model import Model


class Controller(object):
    """For a dict of df, map each df cols."""

    def __init__(self, dict_df_normalized):
        """Constructor."""
        self.dict_df_normalized = dict_df_normalized

    def main(self):
        """Gen the df normalized."""
        model = Model(self.dict_df_normalized)

        return model.main()

"""Create Sha1 for all entities."""
from mixer.gtfs.versioner.model import Model


class Controller(object):
    """For a dict of df, gen the id of the entities."""

    def __init__(self, dict_df_mapped):
        """Constructor."""
        self.dict_df_mapped = dict_df_mapped

    def main(self):
        """Gen the new id."""
        model = Model(self.dict_df_mapped)

        return model.main()

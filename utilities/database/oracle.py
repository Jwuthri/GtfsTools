"""Oracle connection."""
from utilities.database.db_connector import DBConnector
from utilities.database.settings import db_settings
from utilities.map_exception import DbException


class DB(DBConnector):

    def __init__(self, db_name):
        self.db_settings = db_settings
        super().__init__(db_name, self.db_settings)
        self.set_shapes_query
        self.get_connection()

    def get_connection(self):
        """Verify db connection."""
        try:
            self.execute_and_fetch_all("SELECT 1 FROM dual")[0][0]
        except Exception as e:
            raise DbException(e)

    @property
    def set_shapes_query(self):
        # TODO
        self.QUERY_SHAPE_TPL = """
        """

    def insert_geography(self):
        # TODO

"""SqlServer connection."""
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
            self.execute_and_fetch_all("SELECT 1")[0][0]
        except Exception as e:
            raise DbException(e)

    @property
    def set_shapes_query(self):
        self.QUERY_SHAPE_TPL = """
            INSERT INTO Shape (Id, Shape)
            VALUES ('{shape_id}',
            geography::STGeomFromText('LINESTRING({points})', 4326))
        """

    def shape_query_format(self, shape_id, list_pts):
        str_points = ""
        for row in list_pts:
            str_points += str(row[1]) + " " + str(row[0]) + ", "
        str_points = str_points[:-2]
        cdict = {
            "shape_id": shape_id,
            "points": str_points
        }
        return self.QUERY_SHAPE_TPL.format(**cdict)

    def insert_geography(self, df):
        shape = df["Id"].unique()
        list_coord = self.stops_sequence(df)
        for i in range(len(shape)):
            cquery = self.shape_query_format(shape[i], list_coord[i])
            self.execute(cquery, commit=False)

    def fetch_indices(self, tablename, prefix):
        cursor = self.get_cursor()
        query = """
            SELECT i.name AS Index_Name
            FROM sys.indexes i
                INNER JOIN sys.objects o ON i.object_id = o.object_id
            WHERE i.name like '{}_%'
                AND o.type = 'U'
                AND o.name = '{}'
        """
        cquery = query.format(prefix, tablename)
        cursor.execute(cquery)

        return [i[0] for i in cursor.fetchall()]

    def toggle_indices(self, table, prefix, toggle=""):
        indices = self.fetch_indices(table, prefix)
        action = {"ON": "REBUILD", "OFF": "DISABLE"}
        cursor = self.get_cursor()
        for index in indices:
            cursor.execute(
                "ALTER INDEX {} ON {} {};".format(
                    index, table, action[toggle]
                )
            )
        self.commit()

    @staticmethod
    def topify(qry):
        if type(qry) is not str:
            return qry

        if ' ' not in qry:
            return qry

        if qry.split(' ', 1)[0].upper() != 'SELECT':
            return qry

        second_word = qry.split(' ', 2)[1]
        if second_word.upper() == "TOP":
            return qry

        parts = qry.split(' ', 1)
        return ' '.join([parts[0], "TOP 1", parts[1]])
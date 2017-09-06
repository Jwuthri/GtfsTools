"""Tools to create a conneciton with a db, and insert or update data."""
from collections import namedtuple
import datetime
import pymssql
from tqdm import tqdm

import pandas as pd


class DBConnector(object):

    def __init__(self, db_name, db_settings={}):
        self.db_settings = db_settings
        self.db_settings['database'] = db_name
        self.db_conn = None
        self.db_cursor = None
        self.QUERY_TPL = """
            INSERT INTO {table} ({key})
            VALUES ({value});
        """

    @property
    def db_name(self):
        return self.db_settings.get('database')

    def get_cursor(self, opts={}):
        if self.db_conn:
            return self.db_cursor

        db_settings = self.db_settings.copy()
        db_settings.update(opts)
        self.db_conn = pymssql.connect(**db_settings)
        self.db_cursor = self.db_conn.cursor()

        return self.db_cursor

    def execute_and_fetch_all(self, query):
        cur = self.get_cursor()
        cur.execute(query)
        res = cur.fetchall()

        return res

    def get_query_frame(self, query):
        res = self.execute_and_fetch_all(query)
        cols = list(map(lambda x: x[0], self.db_cursor.description))

        return pd.DataFrame(res, columns=cols)

    def execute(self, query, commit=True):
        cursor = self.get_cursor()
        try:
            cursor.execute(query)
        except pymssql.IntegrityError as e:
            raise e

        if commit:
            self.commit()

    def commit(self):
        self.db_conn.commit()

    def rollback(self):
        self.db_conn.rollback()

    def close(self):
        self.db_conn.close()

    def insert_namedtuples(self, namedtuples, tablename):
        queries = self.gen_insert_namedtuples_queries(
            namedtuples,
            tablename
        )
        pbar = tqdm(total=len(queries))
        for idx, query in enumerate(queries):
            self.execute(query, False)
            pbar.update(1)
        pbar.close()

    def construct_closure(self, tuple_cls):

        def fct_construct(obj):
            return tuple_cls(*obj)

        def cls_construct(obj):
            return tuple_cls(obj)

        if tuple_cls.__name__ != 'tuple':

            return fct_construct

        return cls_construct

    def frame2tuples(self, df, tuple_cls=tuple):
        creator = self.construct_closure(tuple_cls)
        tuples = [
            creator(row)
            for row in df.values
        ]

        return tuples

    def quote(self, x):

        return str("'" + x.replace("'", "''") + "'")

    def stringify(self, value):
        if isinstance(value, str) and value == "NULL":
            return value

        if isinstance(value, str):
            return self.quote(value)

        if isinstance(value, datetime.datetime):
            return self.quote(str(value))

        if isinstance(value, datetime.date):
            return self.quote(str(value))

        return str(value)

    def filter_none(self, keys, values):
        d = dict(zip(keys, values))
        dk = []
        for k, v in d.items():
            if v is None:
                dk.append(k)

        for k in dk:
            del d[k]

        return zip(*d.items())

    def compile_insert_query(self, tablename, keys, values):
        if values is None:
            print("WARNING: empty values, no query compiled")
            return ""

        fkeys, fvalues = self.filter_none(keys, values)

        assert None not in fvalues

        join_f = lambda x: ",".join(x)
        join_v = lambda x: ",".join([self.stringify(v) for v in x])

        cdict = {
            "table": tablename,
            "key": join_f(fkeys),
            "value": join_v(fvalues)
        }

        return self.QUERY_TPL.format(**cdict)

    def gen_insert_namedtuples_queries(self, namedtuples, table_name):
        qbuf = []
        for ntuple in namedtuples:
            columns = ntuple._asdict().keys()
            qbuf.append(
                self.compile_insert_query(
                    table_name, columns, list(ntuple)
                )
            )

        return qbuf

    def stops_sequence(self, df):
        list_coord = list()
        shapes = df["Id"].unique()
        for shape in shapes:
            res = df[df["Id"] == shape]
            list_coord.append(zip(res["Latitude"], res["Longitude"]))

        return list_coord

    def insert_dataframe(self, df, table):
        ntuple = namedtuple(table, df.columns)
        ltuples = self.frame2tuples(df, ntuple)
        self.insert_namedtuples(ltuples, table)

    def insert_geography(self, df):
        raise NotImplementedError

    def update_table(self, df, table, set_col, where_col):
        pbar = tqdm(total=len(df))
        for i, row in df.iterrows():
            pbar.update(1)
            query_final = """UPDATE {} SET """.format(table)
            for col in set_col:
                query_final += col + " = '"
                query_final += str(row[col]) + "', "
            query_final = query_final[:-2]
            query_final += " WHERE "

            for col in where_col:
                query_final += col + " = "
                query_final += "'" + str(row[col]) + "' AND "
            query_final = query_final[:-5]
            query_final = query_final.replace("'None'", "NULL")
            self.execute(query_final, commit=False)

        pbar.close()

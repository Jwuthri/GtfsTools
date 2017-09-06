"""Bulk module, to defer writing to SQL Server through csv files
and bulk inserts.

The eponymous class simplifies the creation and flushing of csv files to speed
up the insertion of numerous rows to a SQLServer Database
"""

import os
import shutil
import csv
from time import gmtime, strftime

from utilities.database.sql_server import DB


class Bulk(object):
    """Helper class to handle bulk insertion to a SQLServer database

    Parameters:
        - database: database name to insert data into
        - limiter: to be used when writing csv files
        - quotechar: to be used when writing csv files
    """

    csvfiles = {}
    templocation = "/dev/shm"
    rootlocation = {
        "SQLServer": r"\\MALISSARD\DFS\ADL\db\TransportationData\bulk\{}\{}",
        "Python": "/opt/ADL_db/TransportationData/bulk"
    }

    def __init__(self, database, limiter=',', quotechar='"', escapechar='\\'):
        self.database = database
        self.delimiter = limiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.db = DB(self.database)
        dirpath = os.sep.join([self.rootlocation["Python"], self.database])
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        temppath = os.sep.join([self.templocation, self.database])
        if not os.path.exists(temppath):
            os.makedirs(temppath)

    def write(self, table, dfrows, step='a', thread='1'):
        """
        Parameters:
            - table: the name of the table
            - dfrows: the dataframe
            - step: orders the insertions into related tables
        """
        if thread in self.csvfiles:
            self.csvfiles[thread] += 1
        else:
            self.csvfiles[thread] = 0

        temppath = os.sep.join([self.templocation, self.database])
        fname = '.'.join([step, table, thread, "csv"])
        ftpath = os.sep.join([temppath, fname])

        if list == type(dfrows):
            with open(ftpath, "wb") as csvfile:
                cw = csv.writer(
                    csvfile, delimiter=self.delimiter,
                    quotechar=self.quotechar,
                    escapechar=self.escapechar,
                    quoting=csv.QUOTE_MINIMAL
                )
                for row in dfrows:
                    cw.writerow(row)
        else:
            dfrows.to_csv(
                ftpath, header=False,
                sep=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                quoting=csv.QUOTE_NONE,
                line_terminator="\r\n",
                encoding="utf-8"
            )

        dirpath = os.sep.join([self.rootlocation["Python"], self.database])
        fname = '.'.join(
            [
                step, table, thread, str(self.csvfiles[thread]), "csv"
            ]
        )
        fpath = os.sep.join([dirpath, fname])
        shutil.move(ftpath, fpath)

    def insert(self, table, fpath):
        query = """
            BULK INSERT {}
            FROM '{}'
            WITH (FIELDTERMINATOR='{}')
        """
        cquery = query.format(table, fpath, self.delimiter)
        cursor = self.db.get_cursor()
        cursor.execute(cquery)
        self.db.commit()

    def flush(self):
        dirpath = os.sep.join([self.rootlocation["Python"], self.database])
        if os.path.exists(dirpath):
            for fname in sorted(os.listdir(dirpath)):
                self.insert(
                    fname.split('.')[1],
                    self.rootlocation["SQLServer"].format(self.database, fname)
                )
            os.rename(dirpath, '.'.join(
                [dirpath, strftime("%Y%m%d%H%M", gmtime())])
            )

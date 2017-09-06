"""Insert, mix, update, or normalize a GTFS."""
import logging

import pandas as pd

from mixer.gtfs.mixer.reader import ReaderETL
from mixer.gtfs.mixer.writer import WriterGTFS
from mixer.gtfs.reader.controller import Controller as CR
from mixer.gtfs.normalizer.controller import Controller as CN
from mixer.gtfs.mapper.controller import Controller as CM
from mixer.gtfs.versioner.controller import Controller as CV
from mixer.gtfs.subseter.controller import Controller as SC
from mixer.gtfs.generater.time_distance import TimeDist
from mixer.gtfs.crosser.model import Model as GC
from mixer.gtfs.separater.model import Model as GD
from utilities.decorator import logged
from mixer.glogger import logger


class Controller(object):
    """Main function of mixer."""

    def __init__(self, db_name, gtfs_path):
        """Constructor."""
        self.db_name = db_name
        self.gtfs_path = gtfs_path
        self.reader = ReaderETL(db_name)
        self.writer = WriterGTFS(db_name)

    def insert_first_gtfs(self, dict_df):
        """Insert gtfs if first one."""
        stops = dict_df["Stop"]
        dict_df["TransferTimesNDistances"] = self.gen_time_dist(stops, stops)
        logger.log(logging.INFO, EventLog.log_insert_gtfs)
        self.writer.insert_gtfs(dict_df)

    def merge_gtfs(self, dict_df):
        """Merge and then insert gtfs."""
        dict_gtfs_in_base = self.reader.read_database()
        gtfs_ = list(dict_gtfs_in_base["Gtfs"])
        gtfs_id = dict_df["Gtfs"]["Id"].iloc[0]
        if gtfs_id in gtfs_:
            msg = "{} gtfs is already in the database".format(gtfs_id)
            logger.log(logging.ERROR, msg)
            return 0

        diff = GD(dict_df, dict_gtfs_in_base)
        # Handle the GTFS intersection.
        ct = GC(dict_gtfs_in_base, dict_df, self.db_name)
        ct.gtfs_intersection()

        new_data = diff.whats_new()
        up_data = diff.whats_up(new_data)
        end_data = diff.whats_end()

        new_stops = new_data["Stop"]
        other_stops = dict_gtfs_in_base["Stop"]
        all_stops = pd.concat([other_stops, new_stops])
        new_data["TransferTimesNDistances"] = self.gen_time_dist(
            new_stops, all_stops)

        logger.log(logging.INFO, EventLog.insert_merged_gtfs)
        self.writer.insert_gtfs(new_data)
        logger.log(logging.INFO, EventLog.reup_data)
        self.writer.up_gtfs(up_data)
        logger.log(logging.INFO, EventLog.close_data)
        self.writer.end_gtfs(end_data)

    def gen_time_dist(self, df1, df2):
        """Gen time dist btw 2 df of stops."""
        logger.log(logging.INFO, "Generating the TransferTimesNDistances")
        td = TimeDist(df1, df2)

        return td.main()

    def insertion_strat(self, dict_df):
        """Insert first or merge."""
        first_gtfs = self.reader.is_the_db_clean()
        if first_gtfs:
            logger.log(logging.INFO, "This is the first GTFS")
            self.insert_first_gtfs(dict_df)
        else:
            msg = "There is already a gtfs, need to merge them"
            logger.log(logging.INFO, msg)
            self.merge_gtfs(dict_df)

    @logged(level=logging.INFO, name=logger)
    def insert_new_gtfs(self, insert=True):
        """Insert a gtfs mixed if already one in the base,
        else just insert the gtfs."""
        logger.log(logging.INFO, EventLog.log_read_zip)
        dict_reader = CR(self.gtfs_path).main()
        logger.log(logging.INFO, EventLog.log_normalize_gtfs)
        dict_norm = CN(self.gtfs_path, dict_reader).main()
        logger.log(logging.INFO, EventLog.log_mapping_gtfs)
        dict_map = CM(dict_norm).main()
        logger.log(logging.INFO, EventLog.log_versioning_gtfs)
        dict_vers = CV(dict_map).main()
        logger.log(logging.INFO, EventLog.log_subset_gtfs)
        dict_sub = SC(dict_vers).main()
        logger.log(logging.INFO, EventLog.log_is_this_first_gtfs)
        if insert:
            return self.insertion_strat(dict_sub)
        else:
            return dict_sub


class EventLog(object):
    """Just log message."""

    log_read_zip = """

        ########################
        # READING THE GTFS ZIP #
        ########################
    """

    log_is_this_first_gtfs = """

        ########################
        # IS THIS FIRST GTFS ? #
        ########################
    """

    log_normalize_gtfs = """

        ######################
        # NORMALIZE THE GTFS #
        ######################
    """

    log_mapping_gtfs = """

        ####################
        # MAPPING THE GTFS #
        ####################
    """

    log_versioning_gtfs = """

        #######################
        # VERSIONING THE GTFS #
        #######################
    """

    log_subset_gtfs = """

        ###################
        # SUBSET THE GTFS #
        ###################
    """

    log_insert_gtfs = """

        ###################
        # INSERT THE GTFS #
        ###################
    """

    insert_merged_gtfs = """

        ######################
        # INSERT MERGED GTFS #
        ######################
    """

    reup_data = """

        ##################
        # UP REUSED DATA #
        ##################
    """

    close_data = """

        ###########################
        # CLOSE NO MORE USED DATA #
        ###########################
    """

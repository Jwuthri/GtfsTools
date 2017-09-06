"""Generate a logger based on time."""
import logging
from logging import FileHandler


class Logger(object):

    def __init__(self, file_name, level, to_console=False, logger_name=None):
        if logger_name:
            self.log_name = logger_name
        else:
            self.log_name = file_name
        self.logger = logging.getLogger(self.log_name)
        self.logger.setLevel(level)
        file_handler = FileHandler(file_name)
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s : %(levelname)s : %(message)s')
        )
        self.logger.addHandler(file_handler)
        if to_console:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)
            stream_handler.setFormatter(logging.Formatter(
                '%(name)-12s: %(asctime)s : %(levelname)s : %(message)s')
            )
            self.logger.addHandler(stream_handler)

    def log(self, lvl, message):
        self.logger.log(lvl, message)

    def kill(self):
        for log in self.logger.handlers:
            self.logger.removeHandler(log)

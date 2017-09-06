"""Exception class."""


class MapException(Exception):
    """Class to generate the raise exception."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class DbException(Exception):
    """Class to generate the raise exception."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

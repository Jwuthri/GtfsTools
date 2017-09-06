"""Arguments Parser."""
import argparse


class ArgumentsParser(object):
    """Arguments parser"""

    def __init__(self, params=[]):
        """Constructor"""
        self.params = params
        self.parser = argparse.ArgumentParser()
        for arg, msg, choices in self.params:
            if not choices:
                self.parser.add_argument(arg, help=msg)
            else:
                self.parser.add_argument(arg, help=msg, choices=choices)

    def get_args(self):
        """get the args from the parsed parameters."""

        args = self.parser.parse_args()
        return [
            getattr(args, str(var[0]).replace('--', ''))
            for var in list(self.params)
        ]

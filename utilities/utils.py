"""Usefull tools for the reconstructor."""
from itertools import chain
from zipfile import ZipFile

import pandas as pd


def change_list_type(item, func):
    """Change type of a list.

    :rparams item: list to change type
    :rparams func: new type
    :return: list with new type
    :rtype: list
    """
    if isinstance(item, list):
        return [change_list_type(x, func) for x in item]

    return func(item)


def pairwise(data, last=False):
    """Generator for pair data.

    :rparams data: dataframe or list
    :params last: pair btw last and first ?
    :return: generator of pair
    :rtype: list
    """
    res = None
    if isinstance(data, pd.DataFrame):
        res = [(x[1], y[1])
               for x, y in zip(data.iterrows(), data[1:].iterrows())]
        if last:
            res = res + [
                (x[1], y[1])
                for x, y
                in zip(data[-1:].iterrows(), data[:1].iterrows())
            ]

    elif isinstance(data, list):
        res = [(x, y) for x, y in zip(data, data[1:])]
        if last:
            res = res + [
                (x, y) for x, y in zip(data[-1:], data[:1])
            ]

    return res


def dict_union(*args):
    """Concat n dict into one."""
    return dict(chain.from_iterable(d.items() for d in args))


def fmt_str(bad_string):
    """Remove accents, space useless and transform into unicode."""
    # import string
    try:
        better_string = bad_string.strip()
        return better_string.lower()
        # for char in string.punctuation:
        #     bad_string = bad_string.replace(char, '')
        # return " ".join(bad_string.split()).lower()
    except:
        return bad_string


def list_zip_files(afile):
    """Buff a zip."""
    if not isinstance(afile, ZipFile):
        afile = ZipFile(afile)

    zfiles = [zf.filename for zf in afile.filelist]

    return zfiles

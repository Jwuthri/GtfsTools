"""All tools used to simplify the code."""
import time
import logging
from functools import wraps, partial
import multiprocessing as mp
import warnings

import pandas as pd
from pandas.compat import signature

from utilities.logger import Logger


def logged(func=None, level=logging.DEBUG, name=None, msg=None):
    """
    Decorator to automatically the time of execution of a function
    in a logfile, and write a message

    Parameters
    ----------
    func : the function name
    level : the level of the log
    name : the name of the log
    message : specific message

    Examples
    --------
    >>> @logged(level=logging.INFO)
    ... def toto(x, y):
    ...    print x + y

    >>> toto(3, 4)
    7
    in toto.txt => INFO:toto:0.000001
    """
    if func is None:
        return partial(logged, level=level, name=name, msg=msg)

    logger = name if name else Logger(func.__name__ + ".log", logging.INFO)
    logmsg = msg if msg else func.__name__

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        msg = ":".join([str(func.__name__), str(end - start)])
        logger.log(level, logmsg)
        logger.log(level, msg)

        return result

    return wrapper


def unique(func, cols=None):
    """
    Decorator to remove all the duplicate in adataframe and reindex it

    Parameters
    ----------
    func : the name of the function
    cols : the columns used for the deduplicated

    Examples
    --------
    >>> @unique()
    ... def toto():
    ...    data = {"toto": [0, 1, 2, 0, 3, 0], "tata": [0, 3, 4, 0, 5, 0]}
    ...    df = pd.DataFrame(data)
    ...    print df
    ...    return df
    >>> res = toto()
      tata toto
    0   0   0
    1   3   1
    2   4   2
    3   0   0
    4   5   3
    5   0   0
    >>> print(res)
      tata toto
    0   0   0
    1   3   1
    2   4   2
    3   5   3
    """
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, pd.DataFrame):
            if cols:
                return result.drop_duplicates(
                    subset=cols
                ).reset_index(drop=True)
            else:
                return result.drop_duplicates().reset_index(drop=True)
        else:
            return result

    return wrapper


def mp_func(func):
    """
    Decorator to multiprocess a function

    Parameters
    ----------
    func : function name

    Example:
    --------
    >>> def _toto(value1_value2):
    ...    value1, value2 = value1_value2
    ...    return [value1 * value2]

    >>> @mpmap(_toto)
    ... def toto(value1_value2):
    ...    pass
    """
    def temp(_):
        def apply(args, ignore_result=False):
            res = []
            if args:
                if not mp.current_process().daemon:
                    pol = mp.pool.Pool(mp.cpu_count())
                    lst_res = pol.map(func, args)
                    pol.terminate()
                    pol.join()
                else:
                    lst_res = map(func, args)
                if ignore_result:
                    return None
                for a_res in lst_res:
                    res.extend(a_res)
            return res
        return apply

    return temp


def deprecate_kwarg(old_arg_name, new_arg_name, mapping=None, stacklevel=2):
    """
    Decorator to deprecate a keyword argument of a function

    Parameters
    ----------
    old_arg_name : Name of argument in function to deprecate
    new_arg_name : Name of prefered argument in function
    mapping : If mapping is present, use it to translate old arguments to
        new arguments. A callable must do its own value checking;
        values not found in a dict will be forwarded unchanged.

    Examples
    --------
    >>> @deprecate_kwarg(old_arg_name='cols', new_arg_name='columns')
    ... def f(columns=''):
    ...     print(columns)
    ...
    >>> f(columns='should work ok')
    should work ok
    >>> f(cols='should raise warning')
    FutureWarning: cols is deprecated, use columns instead
    """
    if mapping is not None and not hasattr(mapping, 'get') and \
            not callable(mapping):
        raise TypeError("mapping from old to new argument values "
                        "must be dict or callable!")

    def _deprecate_kwarg(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            old_arg_value = kwargs.pop(old_arg_name, None)
            if old_arg_value is not None:
                if mapping is not None:
                    if hasattr(mapping, 'get'):
                        new_arg_value = mapping.get(old_arg_value,
                                                    old_arg_value)
                    else:
                        new_arg_value = mapping(old_arg_value)
                    msg = "the %s=%r keyword is deprecated, " \
                          "use %s=%r instead" % \
                          (old_arg_name, old_arg_value,
                           new_arg_name, new_arg_value)
                else:
                    new_arg_value = old_arg_value
                    msg = "the '%s' keyword is deprecated, " \
                          "use '%s' instead" % (old_arg_name, new_arg_name)

                warnings.warn(msg, FutureWarning, stacklevel=stacklevel)
                if kwargs.get(new_arg_name, None) is not None:
                    msg = ("Can only specify '%s' or '%s', not both" %
                           (old_arg_name, new_arg_name))
                    raise TypeError(msg)
                else:
                    kwargs[new_arg_name] = new_arg_value
            return func(*args, **kwargs)
        return wrapper
    return _deprecate_kwarg


def make_signature(func):
    """
    Returns a string of the arg list of a func call, with any defaults

    Parameters
    ----------
    func : function name

    Examples
    --------
    >>> def f(a,b,c=2) :
    >>>     return a*b*c
    >>> print(_make_signature(f))
    a,b,c=2
    """
    spec = signature(func)
    if spec.defaults is None:
        n_wo_defaults = len(spec.args)
        defaults = ('',) * n_wo_defaults
    else:
        n_wo_defaults = len(spec.args) - len(spec.defaults)
        defaults = ('',) * n_wo_defaults + spec.defaults
    args = []
    for i, (var, default) in enumerate(zip(spec.args, defaults)):
        args.append(var if default == '' else var + '=' + repr(default))
    if spec.varargs:
        args.append('*' + spec.varargs)
    if spec.keywords:
        args.append('**' + spec.keywords)
    return args, spec.args

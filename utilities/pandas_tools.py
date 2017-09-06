"""Operation on pandas."""
import json
from hashlib import sha1

import pandas as pd
import numpy as np


def sha1_for_named_columns(df, columns):
    """Generate a sha1 based on list of cols."""
    intersection = [col for col in columns if col in df.columns]
    if len(intersection) == len(columns):
        sha1s = pd.Series(data="", index=range(len(df)))
        for i in columns:
            sha1s = sha1s.map(np.str) + df[i].map(np.str)
        sha1s = sha1s.apply(
            lambda s: sha1(str(s).encode("utf8")).hexdigest()
        )
    else:
        sha1s = pd.Series(data=np.nan, index=range(len(df)))

    return sha1s


def hash_frame(df):
    """Transform a data frame into a sha1."""
    return sha1(df.to_string().encode("utf8")).hexdigest()


def cbind(df1, df2):
    """Concat two dataframes thanks to the columns."""
    return pd.concat([df1, df2], axis=1, ignore_index=True)


def rbind(df1, df2):
    """Concat two dataframes thanks to the rows."""
    return pd.concat([df1, df2], axis=0, ignore_index=True)


def remove_rows_contains_null(df, col):
    """Remove all rows which contain a None in the dataframe column."""
    return df[df[col].notnull()]


def keep_rows_contains_null(df, col):
    """Remove all rows which don't contain a None in the dataframe column."""
    return df[~df[col].notnull()]


def compare_data_frame(df1, df2, column):
    """Get difference of two data frames based on a column."""
    return df1[~df1[column].isin(df2[column])]


def count_frequency(df, col):
    """Count the number of occurence value in a column."""
    df['Freq'] = df.groupby(col)[col].transform('count')

    return df


def change_nan_value(df, new_value):
    """Change the nan into new value."""
    return df.where((pd.notnull(df)), new_value)


def extract_json(serie, k):
    """Extract a value by a key."""
    return serie.map(lambda row: json.loads(row)[k])

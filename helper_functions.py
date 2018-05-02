import unidecode
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------------------------------------------------------
# The purpose of this script is to store helper functions.


def df_colnames_to_utf(df_in):
    """
    This function decodes the names of data frame's columns.
    """
    df_out = df_in.copy()
    colnames = list(df_out.columns)
    for iter_key, iter_val in enumerate(colnames):
        colnames[iter_key] = unidecode.unidecode(iter_val)
    df_out.columns = pd.Index(colnames)
    return df_out


def df_cols_to_utf(df_in, list_of_cols):
    """
    This function converts indicated columns in the data frame by decoding the text.
    """
    df_out = df_in.copy()
    full_list_of_cols = list(df_in.columns)

    def decode_series(series_in):
        list_out = list(series_in)
        for iter_num, iter_val in enumerate(list_out):
            if iter_val is np.nan:
                pass
            else:
                list_out[iter_num] = unidecode.unidecode(iter_val)
        return pd.Series(list_out)

    for iter_col in full_list_of_cols:
        if iter_col in list_of_cols:
            df_in.loc[:, iter_col] = decode_series(series_in=df_in.loc[:, iter_col])
    return df_out
# ----------------------------------------------------------------------------------------------------------------------


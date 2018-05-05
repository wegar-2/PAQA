import logging
import unidecode
import pandas as pd
import numpy as np
import constants
import errors


# ----------------------------------------------------------------------------------------------------------------------
# prepare a logger
main_logger = logging.getLogger(name="main_logger")
# ----------------------------------------------------------------------------------------------------------------------


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
    full_list_of_cols = list(df_out.columns)

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
            df_out.loc[:, iter_col] = decode_series(series_in=df_out.loc[:, iter_col])
    return df_out


def update_station_code(codes_old_new_mapping, code_in):
    """
    This function updates the station code name 'code_in'
    passed to it using the dictionary 'codes_old_new_mapping'
    :param codes_old_new_mapping:
    :param code_in: code which is to be updated to its new value
    :return: new code; if new code is passed, then it is not changed
    """
    if code_in in codes_old_new_mapping.values():
        return code_in
    elif code_in in codes_old_new_mapping.keys():
        return codes_old_new_mapping[code_in]
    else:
        raise errors.UnknownStationCodeError("ERROR occurred in function update_station_code: "
                                             "the old code has not been found in the dictionary: " +
                                             str(code_in))


def remove_redundant_top_rows_from_sheet(df_in, data_year):
    """
    This function check the top rows of the input data frame df_in to verify how many of them
    need to be removed and performs the removal.
    :param df_in: data frame to be pre-processed
    :param data_year: int, the year that is covered by the data in the data frame df_in
    :return: processed data frame
    """
    df_out = df_in.copy()
    # processing of non-2016 data
    if data_year != 2016:
        df_out.drop([0, 1], axis=0, inplace=True)
        df_out.reset_index(inplace=True, drop=True)
    # separate approach to 2016 data
    elif data_year == 2016:
        df_out.reset_index(inplace=True, drop=False)
        colnames = list(df_out.iloc[0, 1:df_out.shape[1]])
        colnames.insert(0, "date")
        df_out.drop([0, 1, 2, 3, 4], axis=0, inplace=True)
        df_out.reset_index(inplace=True, drop=True)
        df_out.columns = pd.Index(colnames)
        df_out.reset_index(inplace=True, drop=True)
    else:
        raise errors.InvalidYearError("Error! Invalid year has been passed to the "
                                      "remove_redundant_top_rows_from_sheet function. ")


def process_the_datafile(df_in, codes_old_new_mapping, data_year):
    """
    This function processes a data frame containing data loaded from an xlsx file with data.
    What it makes is transforming the data file into a form in which it can be loaded into the database.
    :param df_in:
    :param codes_old_new_mapping:
    :return:
    """
    main_logger.info(msg="Inside function: process_the_datafile. ")
    df_out = df_in.copy()
    # dropping useless rows
    df_out = remove_redundant_top_rows_from_sheet(df_in=df_out, data_year=data_year)
    colnames = list()
    # renaming columns - ensuring that all names are new stations' codes
    for iter_num, iter_val in enumerate(list(df_out.columns[1:])):
        try:
            colnames[iter_num] = update_station_code(codes_old_new_mapping=codes_old_new_mapping,
                                                     code_in=iter_val)
        except errors.UnknownStationCodeError:
            main_logger.error("Error occurred during the call to update_station_code function; an unknown station code "
                              "encountered! ")
        except Exception as exc:
            main_logger.error(msg="Error occurred during a call to update_station_code function; the error is not "
                                  "related to the fact that the code passed to the called function is absent. ")
            main_logger.error(msg=exc)
    colnames.insert(0, "date")
    df_out.columns = pd.Index(colnames)
    df_out.reset_index(inplace=True, drop=False)
    # melting the DataFrame
    df_out = pd.melt(df_out, id_vars=["index", "date"],
                     value_vars=colnames[1:],
                     var_name="new_station_code", value_name="pollution_level")
    return df_out


# ----------------------------------------------------------------------------------------------------------------------

my_dir = "/home/herhor/github_repos/PAQA/data/2015_SO2_24g.xlsx"
data1 = pd.read_excel(my_dir)
data1.head()
# data1.reset_index(drop=False, inplace=True)

my_dir2 = "/home/herhor/github_repos/PAQA/data/2016_SO2_24g.xlsx"
data2 = pd.read_excel(my_dir2)
data2.head()
data2.reset_index(inplace=True)


import pandas as pd
import os
import logging
import numpy as np
import sqlalchemy as sa
import unidecode
import constants
import pickle

# 0. get the main_logger
main_logger = logging.getLogger(name="main_logger")


# -------------------------------------
# 1. CITIES and STATIONS tables -------
# -------------------------------------
data_file = "/home/herhor/github_repos/PAQA/data/metadane_stacje_i_stanowiska.xlsx"


def upload_cities_and_stations_data(data_file=data_file):
    """
    This function performs upload of the data on cities and stations into the database.
    :param data_file:
    :return:
    """
    # ------------------------------------------------------------------------------------------------------------------
    # 1. CITIES
    dir_data = "/home/herhor/github_repos/PAQA/data/metadane_stacje_i_stanowiska.xlsx"
    data1 = pd.read_excel(dir_data, sheet_name="Stacje")
    distinct_cities = list(set(list(data1.loc[:, "Miejscowość"])))
    if np.nan in distinct_cities:
        distinct_cities.remove(np.nan)
    if "OTHER" not in distinct_cities:
        distinct_cities.append("OTHER")
    # remove polish characters from the names of the cities
    for iter_num, iter_val in enumerate(distinct_cities):
        distinct_cities[iter_num] = unidecode.unidecode(iter_val)
    df_cities = pd.DataFrame(data={"city_name": list(distinct_cities)})

    # ------------------------------------------------------------------------------------------------------------------
    # 2. STATIONS
    # 2.1. save dictionary of mappings of stations' names OLD -> NEW
    station_codes_mappings = dict(zip(
        list(data1.loc[:, 'Stary Kod stacji']),
        list(data1.loc[:, 'Kod stacji'])
    ))
    with open(os.path.dirname(data_file), mode='wb') as target_file:
        pickle.dump(station_codes_mappings, file=target_file)
    # 2.2. prepare the data frame with data on stations
    df_stations = data1.loc[:, ['Nazwa stacji', 'Stary Kod stacji', 'Kod stacji', 'Miejscowość']]
    new_colnames



    # ------------------------------------------------------------------------------------------------------------------
    # 3. upload the data frames df_cities and df_stations into the dedicated tables in the database
    engine1 = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/")
    connection1 = engine1.connect()
    df_cities.to_sql(name='CITIES', schema='PAQA_DB', if_exists='append', index=False, con=connection1)



def update_station_code(codes_old_new_mapping, code_in):
    """
    This function updates the station code name 'code_in'
    passed to it using the dictionary 'codes_old_new_mapping'
    :param codes_old_new_mapping:
    :param code_in:
    :return:
    """
    print("code_in: ", code_in)
    if code_in in codes_old_new_mapping.values():
        return code_in
    elif code_in in codes_old_new_mapping.keys():
        return codes_old_new_mapping[code_in]
    else:
        raise Exception("ERROR occurred in function update_station_code: "
                        "the old code has not been found in the dictionary")

# -----------------------------------
# ----- 2. DATA_POLLUTION table -----
# -----------------------------------


test_df = pd.read_excel("/home/herhor/github_repos/PAQA/data/2012_PM10_24g.xlsx")
test_df.iloc[0:5, 0:5]
test_df_2 = test_df.iloc[2:, :]
test_df_2.iloc[0:5, 0:5]
test_df_2.reset_index(inplace=True, drop=True)
test_df_2 = test_df_2.rename(columns={"Kod stacji": "date"})
test_df_2.iloc[0:5, 0:5]

new_colnames = [update_station_code(codes_old_new_mapping=station_codes_mappings,
                                    code_in=el) for el in list(test_df_2.columns)[1:]]
list(test_df_2.columns)[1]
list(station_codes_mappings.keys())[0:10]
list(station_codes_mappings.values())[0:10]


def process_the_datafile(df_in):
    """
    This function processes a data frame containing data loaded from an xlsx file with data.
    What it makes is transforming the data file into a form in which it can be loaded into the database.
    :param df_in:
    :return:
    """



dir_data = os.path.join(os.getcwd(), "data")
files_in_dir_data = os.listdir(dir_data)
data_for_years = [str(el) for el in constants.my_yearly_datasets_dict.keys()]

# iterations - over pollutants' dictionary
for iter_key, iter_val in constants.pollutants_dict.items():
    main_logger.info(msg="\n\n\n")
    main_logger.info(msg="--------------------------------------------------")
    main_logger.info(msg="Current pollutant name: " + iter_key)
    main_logger.info(msg="Current pollutant code: " + iter_val)
    # check whether data exists for the current pollutant for various years
    for iter_year in data_for_years:
        iter_file_name = "_".join([iter_year, iter_val, constants.data_frequency]) + ".xlsx"
        main_logger.info("Checking for file: " + iter_file_name)
        if iter_file_name in files_in_dir_data:
            main_logger.info(msg="\t\tFile found. Processing and uploading. ")
            try:
                iter_file_path = os.path.join(dir_data, iter_file_name)
                iter_data = pd.read_excel(iter_file_name)
                iter_data = process_the_datafile(df_in=iter_data)
            except Exception as exc:
                main_logger.error(msg="Error occurred when loading file: " + iter_file_path)
                main_logger.error(msg=exc)
        else:
            main_logger.info(msg="\t\tFile not found. Moving on to next file. ")



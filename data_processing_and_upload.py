import pandas as pd
import os
import logging
import numpy as np
import sqlalchemy as sa
import unidecode
import constants
import pickle
import helper_functions as hf

# 0. get the main_logger
main_logger = logging.getLogger(name="main_logger")


# -------------------------------------
# 1. CITIES and STATIONS tables -------
# -------------------------------------

def upload_cities_and_stations_data():
    """
    This function performs upload of the data on cities and stations into the database.
    """
    # ------------------------------------------------------------------------------------------------------------------
    # 1. CITIES
    data1 = pd.read_excel(constants.dir_data_stations,
                          sheet_name=constants.sheetname_data_stations)
    data1 = hf.df_cols_to_utf(df_in=hf.df_colnames_to_utf(df_in=data1),
                              list_of_cols=constants.stations_cities_cols)
    distinct_cities = list(set(list(data1.loc[:, "Miejscowosc"])))
    if np.nan in distinct_cities:
        distinct_cities.remove(np.nan)
    if "OTHER" not in distinct_cities:
        distinct_cities.append("OTHER")
    # remove polish characters from the names of the cities
    for iter_num, iter_val in enumerate(distinct_cities):
        distinct_cities[iter_num] = unidecode.unidecode(iter_val)
    df_cities = pd.DataFrame(data={"city_name": list(distinct_cities)})
    df_cities.sort_values(by="city_name", inplace=True)
    df_cities.reset_index(drop=True, inplace=True)
    df_cities.reset_index(inplace=True, drop=False)
    df_cities.rename(columns={"index": "id_city"}, inplace=True)
    df_cities.loc[:, "id_city"] = df_cities.loc[:, "id_city"] + 1

    # ------------------------------------------------------------------------------------------------------------------
    # 2. STATIONS
    # 2.1. save dictionary of mappings of stations' names OLD -> NEW
    station_codes_mappings = dict(zip(
        list(data1.loc[:, 'Stary Kod stacji']),
        list(data1.loc[:, 'Kod stacji'])
    ))
    with open(os.path.join(constants.data_dir, "stations_code_mappings.p"), mode='wb') as target_file:
        pickle.dump(station_codes_mappings, file=target_file)
    # 2.2. prepare the data frame with data on stations
    df_stations = data1.loc[:, constants.stations_cities_cols]
    df_stations = pd.merge(left=df_stations, right=df_cities, left_on="Miejscowosc", right_on="city_name")
    df_stations.drop(["city_name", "Miejscowosc"], axis=1, inplace=True)
    df_stations.reset_index(inplace=True, drop=False)
    df_stations.rename(columns=constants.dict_of_stations_colnames, inplace=True)
    df_stations.loc[:, "id_station"] = df_stations.loc[:, "id_station"] + 1

    # ------------------------------------------------------------------------------------------------------------------
    # 3. upload the data frames df_cities and df_stations into the dedicated tables in the database
    engine1 = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/PAQA_DB")
    connection1 = engine1.connect()

    # 3.1. upload the CITIES table
    main_logger.info(msg="\n\n\nUploading table: CITIES")
    main_logger.info(msg="The CITIES dataframe: ")
    main_logger.info(msg=df_cities)
    df_cities.to_sql(name="CITIES", con=connection1,
                     if_exists="append", index=False)

    # 3.2. upload the STATIONS table
    main_logger.info(msg="\n\n\nUploading table: STATIONS")
    main_logger.info(msg="The STATIONS dataframe: ")
    main_logger.info(msg=df_stations)
    df_stations.to_sql(name="STATIONS", con=connection1,
                       if_exists="append", index=False)

    connection1.close()
    del connection1, engine1


# -----------------------------------
# ----- 2. DATA_POLLUTION table -----
# -----------------------------------


dir_data = os.path.expanduser("~/github_repos/PAQA/data")
files_in_dir_data = os.listdir(os.path.expanduser("~/github_repos/PAQA/data"))
files_in_dir_data = sorted([el for el in files_in_dir_data if el[0] == "2"])

list_of_dfs_pollution_data = []

# iterations - over pollutants' dictionary
iter_key = "particles_10_micrometers"
iter_val = constants.pollutants_dict[iter_key]
for iter_key, iter_val in constants.pollutants_dict.items():
    main_logger.info(msg="\n\n\n")
    main_logger.info(msg="--------------------------------------------------")
    main_logger.info(msg="Current pollutant name: " + iter_key)
    main_logger.info(msg="Current pollutant code: " + iter_val)
    # check whether data exists for the current pollutant for various years
    for iter_year in list(constants.my_yearly_datasets_dict.keys()):
        iter_file_name = "_".join([str(iter_year), iter_val, constants.data_frequency]) + ".xlsx"
        main_logger.info("Checking for file: " + iter_file_name)
        if iter_file_name in files_in_dir_data:
            main_logger.info(msg="\t\tFile found. Processing and uploading. ")
            try:
                dir_data = os.path.join(dir_data, iter_file_name)
                main_logger.info(msg="Reading-in the file: " + dir_data)
                iter_data = pd.read_excel(dir_data)
                main_logger.info(msg="Data loaded from file: " + dir_data + " - before processing. ")
                main_logger.info(msg=iter_data.head())
                iter_data = hf.process_the_datafile(df_in=iter_data)
                main_logger.info(msg="Data loaded from file: " + dir_data + " - after processing. ")
                main_logger.info(msg=iter_data.head())
                # append the data frame to the list of all dataframes
                list_of_dfs_pollution_data.append(iter_data)
            except Exception as exc:
                main_logger.error(msg="Error occurred when loading file: " + dir_data)
                main_logger.error(msg=exc)
        else:
            main_logger.info(msg="\t\tFile not found. Moving on to next file. ")


def upload_pollution_data():
    """
    This function uploads the data on
    """
    files_in_dir_data = os.listdir(os.path.expanduser("~/github_repos/PAQA/data"))
    files_in_dir_data = sorted([el for el in files_in_dir_data if el[0] == "2"])

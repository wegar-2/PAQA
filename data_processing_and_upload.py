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
    df_stations = pd.merge(left=df_stations, right=df_cities,
                           left_on="Miejscowosc", right_on="city_name")
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
    df_stations.to_sql(name="STATIONS", con=connection1, if_exists="append", index=False)
    connection1.close()
    del connection1, engine1


# -----------------------------------
# ----- 2. POLLUTION_DATA table -----
# -----------------------------------

def upload_pollution_data():
    """
    This function uploads the data on values of various air pollutants.
    """
    # ------------------------------------------------------------------------------------------------------------------
    # 1. setup of variables
    main_logger.info(msg="Inside upload_pollution_data function. Starting to upload pollution_data. ")
    # 1.1. variables initialization
    files_in_dir_data = os.listdir(path=constants.data_dir)
    files_in_dir_data = sorted([el for el in files_in_dir_data if el[0] == "2"])
    if os.path.exists(os.path.join(constants.data_dir, "stations_code_mappings.p")):
        codes_old_new_mapping = open(file=os.path.join(constants.data_dir, "stations_code_mappings.p"),
                                     mode="rb")
        codes_old_new_mapping = pickle.load(file=codes_old_new_mapping)
    else:
        raise Exception("ERROR! The file with mapping of old stations' codes to new stations' codes does not exist. "
                        "The order of execution of scripts might have been incorrect. ")
    list_of_source_files = list()
    list_of_dfs_pollution_data = list()
    db_engine = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/PAQA_DB")
    # 1.2. printing info on the range of data being covered
    main_logger.info(msg="\n\nYears covered:")
    for iter_num, iter_val in enumerate(list(constants.my_yearly_datasets_dict.keys())):
        main_logger.info(msg=str(iter_num + 1) + ". "  + str(iter_val))
    main_logger.info(msg="\n\n")
    for iter_key, iter_val in constants.pollutants_dict.items():
        main_logger.info(msg=iter_key + ": " + iter_val)
    # 1.3. printing other info
    main_logger.info(msg="\n\nList of files to load: ")
    for iter_num, iter_val in enumerate(list_of_source_files):
        main_logger.info(msg="\t" + str(iter_num) + ". " + iter_val)
    main_logger.info(msg="\n\nPrinting full dictionary of codes below. ")
    for iter_key, iter_val in codes_old_new_mapping.items():
        main_logger.info(msg=str(iter_key) + ": " + iter_val)

    # ------------------------------------------------------------------------------------------------------------------
    # 2. iterations over pollutants' dictionary
    main_logger.info(msg="\n\n\n")
    main_logger.info(msg="--------------------------------------------------------")
    main_logger.info(msg="--------------------------------------------------------")
    main_logger.info(msg="-------  Starting iterations over pollutants  ----------")
    main_logger.info(msg="--------------------------------------------------------")
    main_logger.info(msg="--------------------------------------------------------")
    for iter_key, iter_val in constants.pollutants_dict.items():
        main_logger.info(msg="\n\n")
        main_logger.info(msg="--------------------------------------------------")
        main_logger.info(msg="Current pollutant name: " + iter_key)
        main_logger.info(msg="Current pollutant code: " + iter_val)
        # check whether data exists for the current pollutant for various years
        main_logger.info(msg="\n\n--------------------------------------------")
        main_logger.info(msg="Starting iterations over years for pollutant: " + iter_val)
        for iter_year in list(constants.my_yearly_datasets_dict.keys()):
            # 2.1. gathering data for a given pollutant
            iter_file_name = "_".join([str(iter_year), iter_val, constants.data_frequency]) + ".xlsx"
            list_of_source_files.append(iter_file_name)
            main_logger.info("Checking for file: " + iter_file_name)
            if iter_file_name in files_in_dir_data:
                main_logger.info(msg="\t\tFile found. Processing and uploading. ")
                try:
                    dir_data = os.path.join(constants.data_dir, iter_file_name)
                    main_logger.info(msg="Reading-in the file: " + dir_data)
                    iter_data = pd.read_excel(dir_data)
                    main_logger.info(msg="Data loaded from file: " + dir_data + " - before processing. ")
                    main_logger.info(msg=iter_data.head())
                    iter_data = hf.process_the_datafile(df_in=iter_data,
                                                        codes_old_new_mapping=codes_old_new_mapping)
                    main_logger.info(msg="Data loaded from file: " + dir_data + " - after processing. ")
                    main_logger.info(msg=iter_data.head())
                    # append the data frame to the list of all dataframes
                    list_of_dfs_pollution_data.append(iter_data)
                except Exception as exc:
                    main_logger.error(msg="Error occurred when loading file: " + constants.data_dir)
                    main_logger.error(msg=exc)
            else:
                main_logger.info(msg="\t\tFile not found. Moving on to next file. ")
            main_logger.info(msg="Finished gathering data for pollutant: " + iter_val)
            # 2.2. uploading data for a given pollutant
            main_logger.info(msg="Uploading data. ")
            # df_to_upload = pd.concat(objs=list_of_dfs_pollution_data, axis=0)
            db_connection = db_engine.connect()
            # df_to_upload.to_sql(name="POLLUTION_DATA", con=db_connection, if_exists="append", index=False)
            # clear the list of DataFrames before moving on to the next pollutant
            main_logger.info("Current length of the list_of_dfs: " + str(len(list_of_dfs_pollution_data)))
            list_of_dfs_pollution_data.clear()
    # ------------------------------------------------------------------------------------------------------------------
    # 3. Printing summary on the loaded data
    main_logger.info(msg="\n\n\n")
    main_logger.info(msg="Printing list of files from which the data on pollutions has been loaded: ")
    for iter_num, iter_val in enumerate(list_of_source_files):
        main_logger.info(msg="\t" + str(iter_num + 1) + ". " + iter_val)
    main_logger.info(msg="Quitting the upload_pollution_data function. ")



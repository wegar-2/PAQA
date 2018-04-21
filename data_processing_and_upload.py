import pandas as pd
import os
import logging
import numpy as np
import sqlalchemy as sa
import unidecode
import constants

# 0. get the main_logger
main_logger = logging.getLogger(name="main_logger")


# -------------------------------------
# 1. CITIES and STATIONS tables -------
# -------------------------------------

# 1.1. CITIES
dir_data = "/home/herhor/github_repos/PAQA/data/kody_stacji_pomiarowych.xlsx"
data1 = pd.read_excel(dir_data, sheet_name="KODY_STACJI")

data1.head(2)
distinct_cities = list(set(list(data1.loc[:, "MIEJSCOWOSC"])))
if np.nan in distinct_cities:
    distinct_cities.remove(np.nan)
if "OTHER" not in distinct_cities:
    distinct_cities.append("OTHER")

for iter_num, iter_val in enumerate(distinct_cities):
    distinct_cities[iter_num] = unidecode.unidecode(iter_val)

df_cities = pd.DataFrame(data={"city_name": list(distinct_cities)})

engine1 = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/")
connection1 = engine1.connect()

df_cities.to_sql(name='CITIES', schema='PAQA_DB',
                 if_exists='append', index=False, con=connection1)

# 1.2. STATIONS
data1.head(2)
df_stations = data1.loc[:, ['KOD STARY', 'KOD NOWY', 'NAZWA STACJI', 'MIEJSCOWOSC']]
df_stations.head(3)

station_codes_mappings = dict(zip(
    list(df_stations.loc[:, 'KOD STARY']),
    list(df_stations.loc[:, 'KOD NOWY'])
))

station_codes_mappings.values()


def update_station_code(codes_old_new_mapping, code_in):
    """
    This function updates the station code name 'code_in' passed to it using the dictionary 'codes_old_new_mapping'
    :param codes_old_new_mapping:
    :param code_in:
    :return:
    """
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
            except Exception as exc:
                main_logger.error(msg="Error occurred when loading file: " + iter_file_path)
                main_logger.error(msg=exc)
        else:
            main_logger.info(msg="\t\tFile not found. Moving on to next file. ")



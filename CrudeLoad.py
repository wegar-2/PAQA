import pandas as pd
import numpy as np
import sqlalchemy as sa
import os
import unidecode
import constants
import pickle
import helper_functions as hf
import tqdm


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# 1. PREPARE TABLES ON THE SERVER
# 1.1. try to connect to the db
user_string = ""
user_pass = ""
con_string = "mysql://" + user_string + ":" + user_pass + "@localhost/gios"
try:
    mysqldb_engine = sa.create_engine(con_string)
except Exception as e:
    print(e)
metadata1 = sa.MetaData(bind=mysqldb_engine)
# 1.2. create table: CITIES

try:
    sa.Table('CITIES', metadata1,
             sa.Column('id_city', sa.INT, primary_key=True,
                       autoincrement=True, nullable=False),
             sa.Column('city_name', sa.VARCHAR(100), nullable=False))
    metadata1.create_all()
except Exception as exc:
    print(exc)
# 1.3. create table: STATIONS
try:
    sa.Table('STATIONS', metadata1,
             sa.Column('id_station', sa.INT, primary_key=True, autoincrement=True, nullable=False),
             sa.Column('station_name', sa.VARCHAR(100), nullable=False),
             sa.Column('old_station_code', sa.VARCHAR(100), nullable=False),
             sa.Column('new_station_code', sa.VARCHAR(100), nullable=False),
             sa.Column('id_city', sa.INT, sa.ForeignKey('CITIES.id_city'), nullable=False))
    metadata1.create_all()
except Exception as exc:
    print(exc)
# 1.4. create table: POLLUTION_DATA
try:
    sa.Table('POLLUTION_DATA', metadata1,
             sa.Column('id_pollution_data', sa.INT, primary_key=True, autoincrement=True, nullable=False),
             sa.Column('id_station', sa.INT, sa.ForeignKey('STATIONS.id_station'), nullable=False),
             sa.Column('measurement_date', sa.DATE, nullable=False),
             sa.Column('measurement_value', sa.FLOAT(18, 4), nullable=True),
             sa.Column('pollutant', sa.VARCHAR(5), nullable=False))
    metadata1.create_all()
except Exception as exc:
    print(exc)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# 2. LOAD DATA ON CITIES AND STATIONS INTO THE SERVER
# ----------- 2.1. prepare data on cities -----------
print(os.getcwd())
dir_data_stations = os.path.expanduser("~/.../metadane_stacje_i_stanowiska.xlsx")
sheetname_data_stations = "Stacje"
data1 = pd.read_excel(dir_data_stations, sheet_name=sheetname_data_stations)
data1 = hf.df_cols_to_utf(df_in=hf.df_colnames_to_utf(df_in=data1), list_of_cols=constants.stations_cities_cols)
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

# ----------- 2.2. prepare data on stations -----------
# save dictionary of mappings of stations' names OLD -> NEW [to be used later]
data_dir = '.../data_xlsx_files'
station_codes_mappings = dict(zip(list(data1.loc[:, 'Stary Kod stacji']), list(data1.loc[:, 'Kod stacji'])))
with open(os.path.join(data_dir, "stations_code_mappings.p"), mode='wb') as target_file:
    pickle.dump(station_codes_mappings, file=target_file)
# proper work on data on stations
station_codes_mappings = dict(zip(list(data1.loc[:, 'Stary Kod stacji']), list(data1.loc[:, 'Kod stacji'])))
stations_cities_cols = ['Kod stacji', 'Stary Kod stacji', 'Miejscowosc', 'Nazwa stacji']
dict_of_stations_colnames = {'Stary Kod stacji': 'old_station_code', 'Kod stacji': 'new_station_code',
                             'Nazwa stacji': 'station_name', 'index': 'id_station'}
df_stations = data1.loc[:, stations_cities_cols]
df_stations.loc[df_stations.loc[:, "Miejscowosc"].isna(), "Miejscowosc"] = "OTHER"
df_stations = pd.merge(left=df_stations, right=df_cities, left_on="Miejscowosc", right_on="city_name", how="left")
df_stations.drop(["city_name", "Miejscowosc"], axis=1, inplace=True)
df_stations.reset_index(inplace=True, drop=False)
df_stations.rename(columns=dict_of_stations_colnames, inplace=True)
df_stations.loc[:, "id_station"] = df_stations.loc[:, "id_station"] + 1

# --------- 2.3. upload the data frames df_cities and df_stations into the dedicated tables in the database ---------
mysqldb_engine = sa.create_engine(con_string)
connection1 = mysqldb_engine.connect()
# 3.1. upload the CITIES table
df_cities.to_sql(name="CITIES", con=connection1, if_exists="append", index=False)
# 3.2. upload the STATIONS table
df_stations.to_sql(name="STATIONS", con=connection1, if_exists="append", index=False)
connection1.close()
del connection1, mysqldb_engine
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# 3. LOAD DATA ON POLLUTIONS
# 3.1. setup of variables
files_in_dir_data = os.listdir(path=data_dir)
files_in_dir_data = sorted([el for el in files_in_dir_data if el[0] == "2"])
if os.path.exists(os.path.join(constants.data_dir, "stations_code_mappings.p")):
    codes_old_new_mapping = open(file=os.path.join(constants.data_dir, "stations_code_mappings.p"), mode="rb")
    codes_old_new_mapping = pickle.load(file=codes_old_new_mapping)
else:
    raise Exception("ERROR! The file with mapping of old stations' codes to new stations' codes does not exist. "
                    "The order of execution of scripts might have been incorrect. ")
list_of_source_files = list()
list_of_dfs_pollution_data = list()
dict_of_dataframes = dict()
db_engine = sa.create_engine(con_string)
pollutants_dict = {"particles_2.5_micrometers": "PM2.5",
                   "sulphur_dioxide": "SO2"}
my_yearly_datasets_dict = {2016: "242"}
data_frequency = "24g"

# 3.2. iterations over pollutants' dictionary
for iter_key, iter_val in pollutants_dict.items():
    # check whether data exists for the current pollutant for various years
    for iter_year in list(my_yearly_datasets_dict.keys()):
        # gathering data for a given pollutant
        iter_file_name = "_".join([str(iter_year), iter_val, data_frequency]) + ".xlsx"
        list_of_source_files.append(iter_file_name)
        if iter_file_name in files_in_dir_data:
            try:
                dir_data = os.path.join(data_dir, iter_file_name)
                iter_data = pd.read_excel(dir_data)
                iter_data = hf.process_the_datafile(df_in=iter_data, codes_old_new_mapping=codes_old_new_mapping,
                                                    data_year=iter_year)
                # append the data frame to the list of all DataFrames
                list_of_dfs_pollution_data.append(iter_data)
            except Exception as exc:
                print(exc)
    # adding the data to data dictionary
    if len(list_of_dfs_pollution_data) == 0:
        dict_of_dataframes[iter_val] = None
    else:
        dict_of_dataframes[iter_val] = pd.concat(objs=list_of_dfs_pollution_data, axis=0)
    # clear the list of DataFrames before moving on to the next pollutant
    list_of_dfs_pollution_data.clear()

# 3.3. data upload into the POLLUTION_DATA table
# 3.3.1. prepare full list of dataframes that need to be concatenated before upload into the DB
list_of_dfs_to_concat = []
for iter_key, iter_val in dict_of_dataframes.items():
    if iter_val is not None:
        iter_val["pollutant"] = iter_key
        list_of_dfs_to_concat.append(iter_val)
# 3.3.2. concatenate the data frames and process the data to make it ready for the upload
df_to_upload = pd.concat(objs=list_of_dfs_to_concat, axis=0)
df_to_upload.drop(["index"], axis=1, inplace=True)
df_to_upload.reset_index(drop=False, inplace=True)
df_to_upload.loc[:, "index"] = df_to_upload.loc[:, "index"] + 1
df_to_upload.rename(columns={"index": "id_pollution_data",
                             "date": "measurement_date",
                             "pollution_level": "measurement_value"}, inplace=True)
# 3.3.3. add column with the station ID
db_connection = db_engine.connect()
df_stations = pd.read_sql(sql="SELECT * FROM STATIONS;", con=db_connection)
df_to_upload_final = pd.merge(left=df_to_upload, right=df_stations, left_on="new_station_code",
                              right_on="new_station_code", how="left")
df_null_stations = df_to_upload_final.loc[df_to_upload_final.loc[:, "id_station"].isna(), :]
df_to_upload_final = df_to_upload_final.loc[:, ["id_pollution_data", "measurement_date", "measurement_value",
                                                "pollutant", "id_station"]]
df_to_upload_final.reset_index(drop=False, inplace=True)
df_to_upload_final.drop(["id_pollution_data"], axis=1, inplace=True)
df_to_upload_final.rename(columns={"index": "id_pollution_data"}, inplace=True)
df_to_upload_final.loc[:, "id_pollution_data"] = df_to_upload_final.loc[:, "id_pollution_data"] + 1
# 3.3.4. upload the data - insert row by row
db_metadata = sa.MetaData(bind=db_engine)
connection1 = db_engine.connect()
df_to_upload_final.to_sql(name="POLLUTION_DATA", con=connection1, if_exists="append", index=False)
# data_table = sa.Table("POLLUTION_DATA", db_metadata, autoload=True)

for iter_num, iter_row in tqdm.tqdm(df_to_upload_final.iterrows()):
    if iter_row["measurement_value"] is np.nan:
        measurement_value = None
    else:
        measurement_value = iter_row["measurement_value"]
    ins = data_table.insert().values(id_pollution_data=iter_row["id_pollution_data"],
                                     id_station=iter_row["id_station"],
                                     measurement_date=iter_row["measurement_date"],
                                     measurement_value=measurement_value,
                                     pollutant=iter_row["pollutant"])
    db_connection.execute(ins)
db_connection.close()
# ----------------------------------------------------------------------------------------------------------------------

